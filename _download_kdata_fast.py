#!/usr/bin/env python3
"""
并发下载 K线数据 (Baostock → Parquet)
- 用 ThreadPoolExecutor 并发 10 只股票/批, 速度提升 5-10x
- 每只独立 Baostock session (bs.login/logout 必须在主线程)
- 输出 F:/Develops/stock_data/kdata.parquet
- 进度写入 _download_progress.txt
"""
import os
import sys
import time
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import baostock as bs
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

OUT_PATH = 'F:/Develops/stock_data/kdata.parquet'
PROGRESS_FILE = 'F:/Develops/stock-asi/_download_progress.txt'
LOG_FILE = 'F:/Develops/stock-asi/_download.log'
START_DATE = '2024-01-01'  # 缩短到 2.5 年 (Baostock 顺序下载, 5533 只需 ~4-5h)
END_DATE = date.today().strftime('%Y-%m-%d')
MAX_WORKERS = 10   # 并发线程数

# 进度锁 (主线程写进度文件)
_progress_lock = threading.Lock()
_progress = {'done': 0, 'rows': 0, 'failed': 0, 'total': 0}

def log(msg):
    ts = time.strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')

def write_progress():
    with _progress_lock:
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            f.write(f"{_progress['done']}/{_progress['total']}\n")
            f.write(f"{_progress['rows']}\n")
            f.write(f"{_progress['failed']}\n")

def fetch_one(code, start_date, end_date):
    """拉单只股票 (在独立线程中运行)"""
    bs_code = f"sh.{code}" if code.startswith(('6', '9')) else f"sz.{code}"
    # 每只股票独立 login (Baostock 限制: 一个连接同时只能处理一个请求)
    bs.login()
    try:
        for attempt in range(3):
            try:
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    'date,open,high,low,close,volume,amount',
                    start_date, end_date, 'd', adjustflag='2'
                )
                if rs.error_code != '0':
                    return ('failed', code, f"err_code={rs.error_code}")
                df = rs.get_data()
                if df is None or df.empty:
                    return ('empty', code, '')
                df['symbol'] = code
                df['date'] = pd.to_datetime(df['date']).dt.date
                for col in ['open', 'high', 'low', 'close']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('int64')
                df['amount'] = pd.to_numeric(df['amount'], errors='coerce').astype('float64')
                return ('ok', code, df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount']])
            except Exception as e:
                if attempt < 2:
                    time.sleep(0.5)
                else:
                    return ('failed', code, str(e)[:80])
    finally:
        bs.logout()
    return ('failed', code, 'unknown')

def get_existing_max_date():
    if not os.path.exists(OUT_PATH):
        return START_DATE
    con = duckdb.connect(':memory:')
    try:
        r = con.execute(f"SELECT MAX(date) FROM read_parquet('{OUT_PATH}')").fetchone()
        if r and r[0]:
            return str(r[0])
    except Exception as e:
        log(f"读 max_date 失败:{e},用 START_DATE")
    finally:
        con.close()
    return START_DATE

def main():
    t0 = time.time()
    log(f"=== K线数据下载 (FAST + 并发={MAX_WORKERS}) ===")
    log(f"目标: {OUT_PATH}")
    log(f"时间范围: {START_DATE} ~ {END_DATE}")

    last_date = get_existing_max_date()
    if last_date >= END_DATE:
        log(f"已有数据已是最新 ({last_date}),退出")
        return
    fetch_start = (pd.to_datetime(last_date) - timedelta(days=15)).strftime('%Y-%m-%d')
    if fetch_start < START_DATE:
        fetch_start = START_DATE
    log(f"抓取起点: {fetch_start} (last_date={last_date})")

    # 股票列表
    bs.login()
    rs = bs.query_stock_basic()
    data = rs.get_data()
    bs.logout()
    a = data[data['type'] == '1'].copy()
    a['code_raw'] = a['code'].str.replace('sh.', '').str.replace('sz.', '')
    codes = a[a['code_raw'].str.match(r'^[036]\d{5}$', na=False)]['code_raw'].tolist()
    log(f"A股代码数: {len(codes)}")
    _progress['total'] = len(codes)
    write_progress()

    # 并发抓取
    all_dfs = []
    failed = []
    empty = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_one, code, fetch_start, END_DATE): code for code in codes}
        for future in as_completed(futures):
            status, code, result = future.result()
            if status == 'ok':
                all_dfs.append(result)
                _progress['rows'] += len(result)
            elif status == 'empty':
                empty += 1
            else:
                failed.append(code)
                _progress['failed'] += 1
            _progress['done'] += 1

            elapsed = time.time() - t0
            rate = _progress['done'] / elapsed if elapsed > 0 else 0
            eta = (len(codes) - _progress['done']) / rate if rate > 0 else 0
            if _progress['done'] % 50 == 0 or _progress['done'] == len(codes):
                log(f"  [{_progress['done']:>4}/{len(codes)}] {elapsed:.0f}s | "
                    f"{_progress['rows']:,} 行 | 失败 {len(failed)} 空 {empty} | ETA {eta:.0f}s")
            # 每 100 只写一次进度
            if _progress['done'] % 100 == 0:
                write_progress()

    write_progress()

    if not all_dfs:
        log("无数据,退出")
        return

    # 合并
    log(f"合并 {len(all_dfs)} 个 DataFrame...")
    df = pd.concat(all_dfs, ignore_index=True)
    del all_dfs
    import gc; gc.collect()
    log(f"合并后: {len(df):,} 行")

    # 与现有数据合并去重
    if os.path.exists(OUT_PATH):
        log("读取已有数据合并...")
        df_old = pq.read_table(OUT_PATH).to_pandas()
        log(f"  已有: {len(df_old):,} 行")
        df = pd.concat([df_old, df], ignore_index=True)
        df = df.drop_duplicates(subset=['symbol', 'date'], keep='last')
        df = df.sort_values(['symbol', 'date']).reset_index(drop=True)
        log(f"  合并后: {len(df):,} 行")

    # 写 Parquet
    log(f"写入 {OUT_PATH} (snappy, 1M/row group)...")
    tmp = OUT_PATH + '.tmp'
    # 推断 schema
    sample = df.head(100)
    schema = pa.Table.from_pandas(sample, preserve_index=False).schema
    RG = 1_000_000
    writer = pq.ParquetWriter(tmp, schema, compression='snappy', use_dictionary=True)
    for start in range(0, len(df), RG):
        end = min(start + RG, len(df))
        chunk = df.iloc[start:end]
        table = pa.Table.from_pandas(chunk, preserve_index=False, schema=schema)
        writer.write_table(table)
        log(f"  RG {start//RG}: {end:,} 行")
    writer.close()
    os.replace(tmp, OUT_PATH)
    sz = os.path.getsize(OUT_PATH)
    log(f"完成! {len(df):,} 行, {sz/1e6:.1f} MB, 耗时 {(time.time()-t0)/60:.1f} min")
    log(f"失败: {len(failed)}, 空: {empty}")
    if failed[:10]:
        log(f"  前 10 失败: {failed[:10]}")

if __name__ == '__main__':
    main()