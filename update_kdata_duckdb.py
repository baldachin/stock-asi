#!/usr/bin/env python3
"""
K线数据增量更新 - DuckDB + Baostock 版
数据源: Baostock (返回 amount 成交额，支持前复权)
策略:
  1. 从 DuckDB 读取本地最新日期
  2. 从 Baostock 获取全量 A股列表
  3. 按批次抓取 K线，直接写入 DuckDB（ON CONFLICT 更新）
  4. 每日 cron: 工作日 16:00 执行
"""

import pandas as pd
import duckdb
import baostock as bs
import time
import os
import fcntl
import sys
from datetime import datetime, date, timedelta

# ---------- 配置 ----------
DB_PATH = '~/stock_data/stock.db'
BATCH_SIZE  = 50           # 每批股票数（单连接连续请求）
MAX_RETRIES = 3
DAYS_BACK   = 15           # 每次多抓几天防止遗漏
# ----------------------------

LOCK_FILE = '/tmp/update_kdata_duckdb.lock'

def acquire_lock():
    """单例锁：防止多个 update 进程同时跑（hermes-gateway 偶尔会触发）"""
    try:
        lock_fd = open(LOCK_FILE, 'w')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        return lock_fd
    except BlockingIOError:
        return None

def get_last_date():
    """从 DuckDB 读取本地最新日期"""
    conn = duckdb.connect(DB_PATH, read_only=True)
    row = conn.execute("SELECT MAX(date) FROM kdata").fetchone()
    conn.close()
    return row[0] if row and row[0] else date(1990, 1, 1)

def get_all_codes():
    """从 Baostock 获取全量 A股代码"""
    lg = bs.login()
    rs = bs.query_stock_basic()
    data = rs.get_data()
    bs.logout()
    a = data[data['type'] == '1'].copy()
    a['code_raw'] = a['code'].str.replace('sh.', '').str.replace('sz.', '')
    codes = a[a['code_raw'].str.match(r'^[036]\d{5}$', na=False)]['code_raw'].tolist()
    print(f"  A股总数: {len(codes)}")
    return codes

def fetch_one(bs_conn, code, start_date, end_date):
    """拉取单只股票的日K线，返回 DataFrame
    bs_conn is kept for signature compatibility; query_* is module-level in baostock.
    """
    bs_code = f"sh.{code}" if code.startswith(('6', '9')) else f"sz.{code}"
    for attempt in range(MAX_RETRIES):
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                'date,open,high,low,close,volume,amount',
                start_date, end_date, 'd',
                adjustflag='2'  # 前复权
            )
            if rs.error_code != '0':
                return pd.DataFrame()
            df = rs.get_data()
            if df.empty:
                return pd.DataFrame()
            df['symbol'] = code
            df['date'] = pd.to_datetime(df['date']).dt.date
            for col in ['open','high','low','close','volume','amount']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df['volume'] = df['volume'].astype('int64')
            return df[['date','symbol','open','high','low','close','volume','amount']]
        except Exception:
            if attempt < MAX_RETRIES - 1:
                time.sleep(0.5)
    return pd.DataFrame()

def write_to_duckdb(df, conn=None):
    """写入 DuckDB，ON CONFLICT 时更新
    如果传入 conn 则复用（避免每批开/关连接导致与其他进程争锁）。
    带重试以容忍 streamlit dashboard 短期持锁。
    """
    if df.empty:
        return 0
    own_conn = conn is None
    if own_conn:
        conn = duckdb.connect(DB_PATH)
    last_err = None
    for attempt in range(10):
        try:
            conn.execute("""
                INSERT INTO kdata (symbol, date, open, high, low, close, volume, amount)
                SELECT symbol, date, open, high, low, close, volume, amount
                FROM df
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    amount = excluded.amount
            """)
            if own_conn:
                conn.close()
            return len(df)
        except Exception as e:
            last_err = e
            err_str = str(e)
            if 'lock' in err_str.lower() or 'IO Error' in err_str:
                # 重试前先尝试回滚/清理连接状态（长连接上锁失败后可能处于坏状态）
                try: conn.rollback()
                except Exception: pass
                time.sleep(1.5 * (attempt + 1))  # 1.5s, 3s, 4.5s, ...
                continue
            raise
    # 所有重试耗尽
    if own_conn:
        try: conn.close()
        except: pass
    raise RuntimeError(f"write_to_duckdb failed after retries: {last_err}")

def process_batch(codes_batch, start_date, end_date, conn=None):
    """一个连接抓完一批，写入 DuckDB
    复用传入的 conn（避免每批/每行开闭连接与 streamlit 争锁）。
    """
    rows = 0
    try:
        lg = bs.login()
        for code in codes_batch:
            df = fetch_one(lg, code, start_date, end_date)
            if not df.empty:
                write_to_duckdb(df, conn=conn)
                rows += len(df)
            time.sleep(0.05)  # 避免请求过快
        bs.logout()
    except Exception as e:
        print(f"\n  批次异常: {e}")
    return rows

def main():
    t0 = datetime.now()

    # 单例锁: 防止 hermes-gateway 多次触发导致多进程同时跑
    lock_fd = acquire_lock()
    if lock_fd is None:
        # 检查现有 lock 是哪个进程
        try:
            with open(LOCK_FILE) as f:
                pid = f.read().strip()
            print(f"[{t0.strftime('%H:%M:%S')}] 另一个 update_kdata_duckdb 进程 (PID={pid}) 正在运行, 退出")
        except Exception:
            print(f"[{t0.strftime('%H:%M:%S')}] 锁文件存在但无法读取, 退出")
        return

    try:
        _main_locked(t0)
    finally:
        lock_fd.close()
        try: os.remove(LOCK_FILE)
        except: pass

def _main_locked(t0):
    today = date.today()
    today_str = today.strftime('%Y-%m-%d')
    print(f"\n{'='*50}")
    print(f"[{t0.strftime('%H:%M:%S')}] K线增量更新 (Baostock + DuckDB)")
    print(f"{'='*50}")

    # Step 1: 本地最新日期
    print(f"\n[Step1] 读取本地快照...")
    last_date = get_last_date()
    last_str = last_date.strftime('%Y-%m-%d') if last_date else 'N/A'
    print(f"  本地最新: {last_str}")

    if last_date and last_date >= today:
        print("  数据已最新，退出")
        return

    # Step 2: 股票列表
    print(f"\n[Step2] 获取股票列表...")
    codes = get_all_codes()
    if not codes:
        print("  获取股票列表失败")
        return

    # Step 3: 抓取范围
    fetch_start = (last_date - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%d')
    print(f"\n[Step3] 抓取范围: {fetch_start} → {today_str}")
    print(f"  股票总数: {len(codes)}, 批次大小: {BATCH_SIZE}")

    # Step 4: 分批抓取
    batches = [codes[i:i+BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
    total_batches = len(batches)
    done = 0
    total_rows = 0

    # 长生命周期 DuckDB 写连接 — 避免每只股票开/关连接触发 streamlit 争锁
    # 打开本身也可能因 streamlit 持锁失败，加重试
    write_conn = None
    for open_attempt in range(20):
        try:
            write_conn = duckdb.connect(DB_PATH)
            break
        except Exception as e:
            if 'lock' in str(e).lower() or 'IO Error' in str(e):
                print(f"\n  [retry] 打开 DuckDB 写连接失败 (第{open_attempt+1}/20次): {e}", flush=True)
                time.sleep(3.0)
                continue
            raise
    if write_conn is None:
        raise RuntimeError("无法打开 DuckDB 写连接 — streamlit 长时间持锁？")
    try:
        for batch_idx, batch in enumerate(batches):
            rows = process_batch(batch, fetch_start, today_str, conn=write_conn)
            total_rows += rows
            done += 1
            elapsed = (datetime.now() - t0).total_seconds()
            print(f"\r  [{done:3d}/{total_batches}] {elapsed:.0f}s | {total_rows:,} 行", end='', flush=True)
    finally:
        try: write_conn.close()
        except: pass

    print()

    # Step 5: 验证
    conn = duckdb.connect(DB_PATH, read_only=True)
    new_last = conn.execute("SELECT MAX(date) FROM kdata").fetchone()[0]
    new_total = conn.execute("SELECT COUNT(*) FROM kdata").fetchone()[0]
    conn.close()

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n完成! 耗时 {elapsed:.0f}s, 新增 {total_rows:,} 行")
    print(f"最新日期: {new_last}, 总行数: {new_total:,}")

if __name__ == '__main__':
    main()
