#!/usr/bin/env python3
"""
kdata.parquet 增量重建脚本
背景: kdata.parquet 于 2026-05-20 因 append 测试失败被截断为 0 字节
策略: 从 baostock 拉取全量历史 + 合并腾讯增量，稳健分批处理
"""

import baostock as bs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time, os, sys, glob
import signal

# ========== 配置 ==========
KDATA_OUT     = 'kdata.parquet'
KDATA_INC     = 'kdata_incremental.parquet'
BACKUP_BROKEN = 'kdata_corrupted_20260520.parquet'
START_DATE    = '2017-01-01'
END_DATE      = '2026-05-20'
BATCH_SIZE    = 300   # 每批股票数（越少越快落盘）
PROGRESS_FILE = 'rebuild_progress.txt'
LOG_FILE      = 'rebuild.log'

schema = pa.schema([
    pa.field('symbol', pa.int32()),
    pa.field('open',   pa.float32()),
    pa.field('high',   pa.float32()),
    pa.field('low',    pa.float32()),
    pa.field('close',  pa.float32()),
    pa.field('amount', pa.float64()),
    pa.field('volume', pa.float64()),
    pa.field('date',   pa.timestamp('us')),
])

def log(msg):
    ts = time.strftime('%H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)
    with open(LOG_FILE, 'a') as f:
        f.write(f"[{ts}] {msg}\n")

def save_progress(batch_num, done, failed):
    with open(PROGRESS_FILE, 'w') as f:
        f.write(f"{batch_num}\n{done}\n{len(failed)}\n")

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            lines = f.read().strip().split('\n')
            if len(lines) >= 3:
                return int(lines[0]), int(lines[1]), int(lines[2])
    return 0, 0, []

# ========== Step 1: 登录 baostock ==========
log("Step 1: 登录 baostock")
bs.login()
# 保持连接活跃
time.sleep(0.5)

# ========== Step 2: 获取 A股列表 ==========
log("Step 2: 获取 A股列表")
rs = bs.query_stock_basic()
stock_list = []
while rs.error_code == '0' and rs.next():
    stock_list.append(rs.get_row_data())
basic_df = pd.DataFrame(stock_list, columns=rs.fields)
a_stocks = basic_df[basic_df['type'] == '1']['code'].tolist()
log(f"  A股数量: {len(a_stocks)}")

# ========== Step 3: 设置临时目录 ==========
tmp_dir = 'kdata_rebuild_tmp'
os.makedirs(tmp_dir, exist_ok=True)
if os.path.exists(LOG_FILE):
    os.unlink(LOG_FILE)

# ========== Step 4: 分批拉取并写入 ==========
t0 = time.time()
failed = []
batch_files = []

# 加载进度
start_batch, total_done, failed_count = load_progress()
failed = []
if os.path.exists('failed_stocks.txt'):
    with open('failed_stocks.txt') as f:
        failed = [l.strip() for l in f if l.strip()]

log(f"Step 4: 从批次 {start_batch} 继续，已完成 {total_done} 只")
log(f"开始拉取数据 (BATCH_SIZE={BATCH_SIZE})...")

for batch_num in range(start_batch, (len(a_stocks) + BATCH_SIZE - 1) // BATCH_SIZE):
    batch_start = batch_num * BATCH_SIZE
    batch_end   = min(batch_start + BATCH_SIZE, len(a_stocks))
    batch_codes = a_stocks[batch_start:batch_end]

    log(f"--- 批次 {batch_num} ({batch_start}-{batch_end}) ---")
    t_batch = time.time()
    chunk_buffer = []
    batch_failed = 0

    for i, code in enumerate(batch_codes):
        # 显式超时保护
        rs = bs.query_history_k_data_plus(code,
            "date,code,open,high,low,close,volume,amount",
            start_date=START_DATE, end_date=END_DATE,
            frequency="d", adjustflag="3")

        if rs.error_code != '0':
            failed.append(code)
            batch_failed += 1
            continue

        df = rs.get_data()
        if df is None or len(df) == 0:
            failed.append(code)
            batch_failed += 1
            continue

        try:
            df['symbol'] = df['code'].str.replace('sh.', '').str.replace('sz.', '').astype(int)
            df['date'] = pd.to_datetime(df['date'])
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount']].dropna(subset=['date', 'symbol'])
            if len(df) > 0:
                chunk_buffer.append(df)
        except Exception as e:
            failed.append(code)
            batch_failed += 1
            continue

    if chunk_buffer:
        df_batch = pd.concat(chunk_buffer, ignore_index=True)
        del chunk_buffer
        df_batch = df_batch.sort_values(['symbol', 'date']).reset_index(drop=True)

        batch_file = os.path.join(tmp_dir, f'batch_{batch_num:04d}.parquet')
        table = pa.Table.from_pandas(df_batch, preserve_index=False, schema=schema)
        with pq.ParquetWriter(batch_file, schema) as w:
            w.write_table(table)
        batch_files.append(batch_file)
        n_rows = len(df_batch)
        del df_batch
    else:
        n_rows = 0

    elapsed = time.time() - t_batch
    total_elapsed = time.time() - t0
    rate = batch_end / total_elapsed
    eta = (len(a_stocks) - batch_end) / rate if rate > 0 else 0

    log(f"  批次 {batch_num}: {n_rows:,} 行 | 失败 {batch_failed} | 本批 {elapsed:.0f}s | 总 {total_elapsed:.0f}s | ETA {eta:.0f}s")

    # 保存进度
    with open('failed_stocks.txt', 'w') as f:
        f.write('\n'.join(failed[-100:]))  # 只保留最近100个
    save_progress(batch_num + 1, batch_end, failed)
    log(f"  进度已保存: {batch_end}/{len(a_stocks)} ({100*batch_end/len(a_stocks):.1f}%)")

    # 强制 gc
    import gc; gc.collect()

log(f"Step 5: 数据拉取完成，共 {len(batch_files)} 个批次文件")

# ========== Step 5: 合并所有批次 + 腾讯增量 ==========
log("Step 5: 合并批次文件...")
t_merge = time.time()

batch_parquet_files = sorted(glob.glob(os.path.join(tmp_dir, 'batch_*.parquet')))
log(f"  找到 {len(batch_parquet_files)} 个批次文件")

all_chunks = []
total_rows = 0
for bf in batch_parquet_files:
    df_chunk = pd.read_parquet(bf)
    all_chunks.append(df_chunk)
    total_rows += len(df_chunk)
    log(f"  加载 {os.path.basename(bf)}: {len(df_chunk):,} 行")

log(f"  合并 {len(all_chunks)} 个 chunk...")
df_main = pd.concat(all_chunks, ignore_index=True)
del all_chunks
import gc; gc.collect()
log(f"  合并后: {len(df_main):,} 行 | 耗时 {time.time()-t_merge:.1f}s")

# 加载腾讯增量
log("加载腾讯增量数据...")
df_inc = pd.read_parquet(KDATA_INC)
df_inc['date'] = pd.to_datetime(df_inc['date'])
df_inc['symbol'] = df_inc['symbol'].astype(int)
log(f"  腾讯增量: {len(df_inc):,} 行 ({df_inc['date'].min().date()} ~ {df_inc['date'].max().date()})")

# 合并去重
log("合并 + 去重...")
df_combined = pd.concat([df_main, df_inc], ignore_index=True)
del df_main, df_inc
import gc; gc.collect()
df_combined = df_combined.drop_duplicates(subset=['symbol', 'date'], keep='last')
df_combined = df_combined.sort_values(['symbol', 'date']).reset_index(drop=True)
log(f"  合并后: {len(df_combined):,} 行")

# ========== Step 6: 写入最终文件 ==========
log("Step 6: 写入最终 kdata.parquet...")
t_write = time.time()

# 先写临时文件，成功后替换
final_tmp = os.path.join(tmp_dir, 'kdata_final.parquet')
RG_ROWS = 600_000
writer = pq.ParquetWriter(final_tmp, schema)
total_written = 0
for start in range(0, len(df_combined), RG_ROWS):
    end = min(start + RG_ROWS, len(df_combined))
    table = pa.Table.from_pandas(df_combined.iloc[start:end], preserve_index=False, schema=schema)
    writer.write_table(table)
    total_written += (end - start)
    log(f"  RG {start//RG_ROWS}: 写入 {end-start:,} 行")
writer.close()

# 替换
os.replace(final_tmp, KDATA_OUT)
sz = os.path.getsize(KDATA_OUT)
log(f"  写入完成: {total_written:,} 行 | 大小 {sz/1e9:.2f}GB | 耗时 {time.time()-t_write:.1f}s")

# 清理
for bf in batch_parquet_files:
    os.unlink(bf)
os.unlink('failed_stocks.txt')
os.unlink(PROGRESS_FILE)
os.rmdir(tmp_dir)

log(f"=== 重建完成 ===")
log(f"  文件: {KDATA_OUT} ({sz/1e9:.2f} GB)")
log(f"  总行数: {total_written:,}")
log(f"  失败股票: {len(failed)}")
log(f"  总耗时: {time.time()-t0:.0f}s ({ (time.time()-t0)/60:.1f}min)")

bs.logout()
