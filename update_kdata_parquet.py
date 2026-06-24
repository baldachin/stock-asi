#!/usr/bin/env python3
"""
K线数据增量更新 - Parquet 版 (替代 update_kdata_duckdb.py)

数据源: Baostock (返回 amount 成交额，支持前复权)
存储: ~/stock_data/kdata.parquet (snappy 压缩, 1M 行/row group)
策略:
  1. 从 kdata.parquet 读 max_date (用 row group 过滤避免加载全量)
  2. 从 Baostock 获取全量 A股列表
  3. 按批次抓取 K线，累积到内存
  4. 读 kdata.parquet (裁掉最后 N 天避免重复), 与新数据合并
  5. 写 kdata.parquet.tmp → fsync → os.replace 原子替换
  6. 单例锁 (fcntl) 防 hermes-gateway 重复触发

相比 DuckDB 版优势:
  - 零锁, dashboard 可以同时跑 (并发读无影响)
  - 写失败 → 旧文件 100% 完好, 重试即可
  - 不需要 stop-and-respawn streamlit
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import baostock as bs
import time
import os
import sys
from datetime import datetime, date, timedelta

# 跨平台单例锁: POSIX 用 fcntl.flock, Windows 用 msvcrt.locking
try:
    import fcntl  # POSIX
    _HAS_FCNTL = True
except ImportError:
    fcntl = None
    _HAS_FCNTL = False
    try:
        import msvcrt  # Windows
        _HAS_MSVCRT = True
    except ImportError:
        msvcrt = None
        _HAS_MSVCRT = False

sys.path.insert(0, os.path.expanduser('~/stock'))
from parquet_atomic import write_atomic

# ---------- 配置 ----------
PARQUET_PATH = os.path.expanduser('~/stock_data/kdata.parquet')
BATCH_SIZE  = 50           # 每批股票数
MAX_RETRIES = 3
DAYS_BACK   = 30           # 每次多抓几天防止遗漏 (必须 >= CROP_DAYS, 否则会丢数据)
CROP_DAYS   = 30           # 合并时裁掉最后 30 天, 避免与已有 recent 数据重复
# ----------------------------

LOCK_FILE = os.path.expanduser('~/stock_data/update_kdata_parquet.lock')

def acquire_lock():
    """单例锁: 防止多个 update 进程同时跑 (POSIX fcntl / Windows msvcrt)"""
    lock_fd = open(LOCK_FILE, 'w')
    try:
        if _HAS_FCNTL:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif _HAS_MSVCRT:
            # msvcrt.locking 需要先 seek 到 0,锁定文件至少 1 字节
            lock_fd.seek(0)
            try:
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                lock_fd.close()
                return None
        else:
            # 无锁可用, 退化为单 PID 检测 (不严格,但单用户场景够用)
            lock_fd.write(str(os.getpid()))
            lock_fd.flush()
            return lock_fd
    except (BlockingIOError, OSError):
        lock_fd.close()
        return None
    lock_fd.write(str(os.getpid()))
    lock_fd.flush()
    return lock_fd

def get_last_date_from_parquet():
    """从 kdata.parquet 读 max_date, 用 row group 过滤避免加载全量"""
    if not os.path.exists(PARQUET_PATH):
        # 首次运行: 默认从 2017-01-01 开始 (10 年历史, 约 1200 万行)
        # 如果想要全量 (1990-至今),改成 date(1990, 1, 1)
        return date(2017, 1, 1)

    pf = pq.ParquetFile(PARQUET_PATH)
    # 1. 读最后一行的 date 列 (每个 row group 1M 行, 我们读 1 个 row group)
    # 实际上 min/max 统计已经在 footer 里, 用 pq 读 metadata
    # 但 pyarrow 没暴露, 我们读最后 1 个 row group 找 max date
    last_rg = pf.read_row_group(pf.num_row_groups - 1, columns=['date'])
    max_date = last_rg.column('date').to_pylist()
    return max(max_date)

def get_max_id_from_parquet():
    """从 kdata.parquet 读 max(id) — 用来给新行分配 id (兼容 DuckDB 时代的 PK)"""
    if not os.path.exists(PARQUET_PATH):
        return 0
    pf = pq.ParquetFile(PARQUET_PATH)
    if 'id' not in pf.schema_arrow.names:
        return 0
    # 读所有 row group 的 id 列, 取 max
    max_id = 0
    for i in range(pf.num_row_groups):
        col = pf.read_row_group(i, columns=['id']).column('id').to_pylist()
        if col:
            max_id = max(max_id, max(col))
    return max_id

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

def fetch_one(code, start_date, end_date):
    """拉取单只股票的日K线, 返回 DataFrame"""
    bs_code = f"sh.{code}" if code.startswith(('6', '9')) else f"sz.{code}"
    for attempt in range(MAX_RETRIES):
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                'date,open,high,low,close,volume,amount',
                start_date, end_date, 'd',
                adjustflag='2'
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

def fetch_all_incremental(codes, start_date, end_date, t0):
    """抓取所有股票的增量 K线, 累积到内存 DataFrame"""
    print(f"\n[Step3] 抓取范围: {start_date} → {end_date}")
    print(f"  股票总数: {len(codes)}, 批次大小: {BATCH_SIZE}")

    batches = [codes[i:i+BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
    total_batches = len(batches)
    done = 0
    total_rows = 0
    all_dfs = []

    for batch_idx, batch in enumerate(batches):
        bs.login()
        try:
            for code in batch:
                df = fetch_one(code, start_date, end_date)
                if not df.empty:
                    all_dfs.append(df)
                    total_rows += len(df)
                time.sleep(0.05)
        finally:
            bs.logout()
        done += 1
        elapsed = (datetime.now() - t0).total_seconds()
        print(f"\r  [{done:3d}/{total_batches}] {elapsed:.0f}s | {total_rows:,} 行", end='', flush=True)

    print()
    if not all_dfs:
        return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)

def merge_and_write(df_new):
    """合并新数据到 kdata.parquet, 原子写入

    步骤:
      1. 打开 kdata.parquet.new (流式 writer)
      2. 读 kdata.parquet 逐 row group, 过滤掉最近 CROP_DAYS 天, 写入新文件
      3. 把 df_new 写到最后
      4. 关闭 writer, 原子 rename

    流式: 一次只在内存放 1 个 row group (~50MB) + df_new (< 1MB)
    """
    if not os.path.exists(PARQUET_PATH):
        # 首次: 直接写
        print(f"\n[Step4] 首次写入 {PARQUET_PATH}")
        table = pa.Table.from_pandas(df_new, preserve_index=False)
        write_atomic(table, PARQUET_PATH, row_group_size=1_000_000)
        return

    # 读老数据, 裁掉最后 CROP_DAYS 天 (这些天会被 df_new 覆盖)
    last_date = get_last_date_from_parquet()
    crop_date = last_date - timedelta(days=CROP_DAYS)

    # 2026-06-23: 数据安全校验 — 如果 crop_date 比 df_new 最小日期还大,
    # 说明 df_new 没完全覆盖裁切窗口, 裁切会永久丢失中间数据 → abort
    if not df_new.empty:
        df_new_min_date = df_new['date'].min()
        if isinstance(df_new_min_date, str):
            df_new_min_date = pd.to_datetime(df_new_min_date).date()
        if crop_date > df_new_min_date:
            msg = (f'[FATAL] crop_date={crop_date} > df_new.min_date={df_new_min_date}, '
                   f'裁切窗口未被新数据完全覆盖, 拒绝写入以防数据丢失. '
                   f'增加 DAYS_BACK 或手动修复.')
            print(msg, flush=True)
            raise RuntimeError(msg)
    print(f"\n[Step4] 流式重写 kdata.parquet (裁掉 {crop_date} 之后)...")

    # 流式: 用 ParquetWriter 逐 row group 写
    new_path = PARQUET_PATH + ".new"
    # 先获取原 schema
    orig_schema = pq.read_schema(PARQUET_PATH)
    writer = pq.ParquetWriter(new_path, orig_schema, compression='snappy')

    pf = pq.ParquetFile(PARQUET_PATH)
    kept_rows = 0
    for rg_idx in range(pf.num_row_groups):
        rg = pf.read_row_group(rg_idx)
        mask = pa.compute.less(rg.column('date'), crop_date)
        rg_filtered = rg.filter(mask)
        if len(rg_filtered) > 0:
            writer.write_table(rg_filtered)
            kept_rows += len(rg_filtered)
        if (rg_idx + 1) % 5 == 0:
            print(f"    处理 {rg_idx + 1}/{pf.num_row_groups} row groups (保留 {kept_rows:,} 行)")
    print(f"  老数据保留: {kept_rows:,} 行 ({pf.num_row_groups} row groups)")

    # 写新数据
    if not df_new.empty:
        # 2026-06-23: 去重 (按 symbol+date 保留最后一条)
        # 原因: DAYS_BACK=CROP_DAYS=30 时, 多次 cron 跑可能导致同一 (symbol, date) 多次出现
        #       writer 用追加写入不去重, 不 dedup 会让行数虚高且 ASI 计算错误
        before = len(df_new)
        df_new = df_new.sort_values(['symbol', 'date']).drop_duplicates(['symbol', 'date'], keep='last')
        after = len(df_new)
        if before != after:
            print(f"  去重: {before:,} → {after:,} 行 (移除 {before-after:,} 重复)")

        # 1) 对齐列: 原文件有 'id' 列 (DuckDB 时代的 PK), df_new 没有
        if 'id' in orig_schema.names:
            start_id = get_max_id_from_parquet()
            ids = list(range(start_id + 1, start_id + 1 + len(df_new)))
            df_new = df_new.copy()
            df_new.insert(0, 'id', ids)
        # 2) 对齐列顺序 (orig_schema 决定)
        cols_in_order = [c for c in orig_schema.names if c in df_new.columns]
        df_new = df_new[cols_in_order]
        table_new = pa.Table.from_pandas(df_new, preserve_index=False, safe=False)
        # 3) 类型对齐 (date32 vs object, etc.)
        if table_new.schema != orig_schema:
            table_new = table_new.cast(orig_schema, safe=False)
        writer.write_table(table_new)
        print(f"  新数据写入: {len(df_new):,} 行")

    writer.close()
    print(f"  ✓ 新文件已关闭, 原子 rename")

    # fsync + rename (write_atomic 不接受已有文件, 自己实现)
    fd = os.open(new_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(new_path, PARQUET_PATH)
    print(f"  ✓ 原子 rename 完成")

_t0 = None  # 全局占位, 兼容老调用 (实际不用)

def main():
    global _t0
    _t0 = datetime.now()
    t0 = _t0

    # 单例锁
    lock_fd = acquire_lock()
    if lock_fd is None:
        try:
            with open(LOCK_FILE) as f:
                pid = f.read().strip()
            print(f"[{t0.strftime('%H:%M:%S')}] 另一个 update_kdata_parquet 进程 (PID={pid}) 正在运行, 退出")
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
    print(f"[{t0.strftime('%H:%M:%S')}] K线增量更新 (Baostock + Parquet)")
    print(f"{'='*50}")

    # Step 1: 本地最新日期
    print(f"\n[Step1] 读取本地快照...")
    last_date = get_last_date_from_parquet()
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

    # Step 4: 抓取
    df_new = fetch_all_incremental(codes, fetch_start, today_str, t0)
    if df_new.empty:
        print("  无新数据")
        return

    # Step 5: 合并写
    merge_and_write(df_new)

    # Step 6: 验证
    new_last = get_last_date_from_parquet()
    pf = pq.ParquetFile(PARQUET_PATH)
    new_total = pf.metadata.num_rows
    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n完成! 耗时 {elapsed:.0f}s, 新增 {len(df_new):,} 行")
    print(f"最新日期: {new_last}, 总行数: {new_total:,}")

if __name__ == '__main__':
    main()
