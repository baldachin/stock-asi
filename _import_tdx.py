#!/usr/bin/env python3
"""
导入通达信 (TDX) 导出数据到 kdata.parquet (v2 - 优化版)

TDX 文件位置: F:/Develops/Stock/data/tdx_export/day/
文件命名: SH#600000.txt / SZ#000001.txt (5208 只)
文件格式 (GB18030 编码, \\t 分隔):
  - 第 1 行: "{code} {name} 日线 前复权"
  - 第 2 行: 表头 (日期, 开盘, 最高, 最低, 收盘, 成交量, 成交额)
  - 数据行: 1999/11/10\\t-2.07\\t-2.03\\t-2.36\\t-2.27\\t174085000\\t4859102208.00
  - 末尾可能有: #数据来源:通达信 (注释行,需跳过)

策略:
  1. 批量读所有 TDX 文件 (每 500 个一组,共享一个 DataFrame)
  2. 用 DuckDB SQL 把 TDX 与现有 kdata 做 LEFT JOIN,优先用 TDX 补 amount/volume
  3. 写回 kdata.parquet (原子替换,1M 行/row group)
"""
import os
import re
import sys
import time
import gc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

TDX_DIR = 'F:/Develops/Stock/data/tdx_export/day'
OUT_PATH = 'F:/Develops/stock_data/kdata.parquet'
LOG_FILE = 'F:/Develops/stock-asi/_import_tdx.log'
TMP_DIR = 'F:/Develops/stock-asi/_tdx_tmp'

def log(msg):
    ts = time.strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')

def parse_tdx_batch(files_batch):
    """解析一批 TDX 文件,返回 DataFrame (rows 列表再 df)"""
    rows = []
    for fn in files_batch:
        path = os.path.join(TDX_DIR, fn)
        m = re.match(r'([A-Z]+)#(\d+)\.txt', fn)
        if not m:
            continue
        symbol = m.group(2)
        try:
            with open(path, 'r', encoding='gb18030', errors='replace') as f:
                content = f.read()
            lines = content.split('\n')
            for line in lines[2:]:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split('\t')
                if len(parts) != 7:
                    continue
                if not re.match(r'\d{4}/\d{2}/\d{2}', parts[0]):
                    continue
                rows.append([symbol] + parts)
        except Exception as e:
            log(f"  读 {fn} 失败: {e}")
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=['symbol', 'date', 'open', 'high', 'low',
                                     'close', 'volume', 'amount'])
    # 类型转换
    df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d').dt.date
    df['open']  = pd.to_numeric(df['open'], errors='coerce').astype('float64')
    df['high']  = pd.to_numeric(df['high'], errors='coerce').astype('float64')
    df['low']   = pd.to_numeric(df['low'], errors='coerce').astype('float64')
    df['close'] = pd.to_numeric(df['close'], errors='coerce').astype('float64')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('int64')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').astype('float64')
    return df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']]

def main():
    t0 = time.time()
    log(f"=== TDX 数据导入 (v2) ===")
    log(f"源目录: {TDX_DIR}")
    log(f"目标: {OUT_PATH}")

    # Step 1: 解析所有 TDX 文件 (流式写 parquet 临时文件,避免内存爆炸)
    log("Step 1: 解析 TDX 文件...")
    os.makedirs(TMP_DIR, exist_ok=True)
    tmp_tdx = os.path.join(TMP_DIR, 'tdx_all.parquet')

    files = sorted([f for f in os.listdir(TDX_DIR) if f.endswith('.txt')])
    log(f"找到 {len(files)} 个文件")

    BATCH = 500
    schema = pa.schema([
        ('symbol', pa.string()),
        ('date',   pa.date32()),
        ('open',   pa.float64()),
        ('high',   pa.float64()),
        ('low',    pa.float64()),
        ('close',  pa.float64()),
        ('volume', pa.int64()),
        ('amount', pa.float64()),
    ])
    if os.path.exists(tmp_tdx):
        os.remove(tmp_tdx)
    writer = pq.ParquetWriter(tmp_tdx, schema, compression='snappy', use_dictionary=True)

    total_rows = 0
    for batch_start in range(0, len(files), BATCH):
        batch = files[batch_start:batch_start+BATCH]
        df = parse_tdx_batch(batch)
        if not df.empty:
            # symbol 转 string (parquet schema 是 string)
            df['symbol'] = df['symbol'].astype(str)
            table = pa.Table.from_pandas(df, preserve_index=False, schema=schema)
            writer.write_table(table)
            total_rows += len(df)
        elapsed = time.time() - t0
        rate = (batch_start + BATCH) / elapsed if elapsed > 0 else 0
        eta = (len(files) - batch_start - BATCH) / rate if rate > 0 else 0
        if (batch_start + BATCH) % 1000 == 0 or (batch_start + BATCH) >= len(files):
            log(f"  [{min(batch_start+BATCH, len(files)):>4}/{len(files)}] {elapsed:.0f}s | "
                f"{total_rows:,} 行 | ETA {eta:.0f}s")
        del df; gc.collect()

    writer.close()
    log(f"Step 1 完成: {total_rows:,} 行 ({time.time()-t0:.0f}s)")
    log(f"  临时文件: {tmp_tdx} ({os.path.getsize(tmp_tdx)/1e6:.1f} MB)")

    # Step 2: 用 DuckDB 合并 (高效)
    log("Step 2: 用 DuckDB 合并 TDX + kdata...")
    con = duckdb.connect(':memory:')
    # 现有数据 amount 缺失情况
    stats = con.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amt_null,
            SUM(CASE WHEN amount = 0 OR amount IS NULL THEN 1 ELSE 0 END) AS amt_zero_or_null
        FROM read_parquet('{OUT_PATH}')
    """).fetchone()
    log(f"  现有 amount 状态: 总 {stats[0]:,}, NULL {stats[1]:,}, 0或NULL {stats[2]:,}")

    # 合并: kdata LEFT JOIN tdx, TDX 优先
    # 用 COALESCE(原值, TDX值) 保留已有值,TDX 仅补缺失
    log("  合并...")
    df_combined = con.execute(f"""
        SELECT
            k.symbol,
            k.date,
            COALESCE(k.open,   t.open)   AS open,
            COALESCE(k.high,   t.high)   AS high,
            COALESCE(k.low,    t.low)    AS low,
            COALESCE(k.close,  t.close)  AS close,
            COALESCE(k.volume, t.volume) AS volume,
            COALESCE(k.amount, t.amount) AS amount
        FROM read_parquet('{OUT_PATH}') k
        LEFT JOIN read_parquet('{tmp_tdx}') t
            ON k.symbol = t.symbol AND k.date = t.date
    """).df()
    log(f"  合并后: {len(df_combined):,} 行 ({time.time()-t0:.0f}s)")

    # 添加 TDX 中独有 (现有数据没有) 的股票
    log("  添加 TDX 独有股票...")
    df_new_only = con.execute(f"""
        SELECT
            t.symbol,
            t.date,
            t.open, t.high, t.low, t.close,
            t.volume, t.amount
        FROM read_parquet('{tmp_tdx}') t
        LEFT JOIN read_parquet('{OUT_PATH}') k
            ON k.symbol = t.symbol AND k.date = t.date
        WHERE k.symbol IS NULL
    """).df()
    log(f"  TDX 独有: {len(df_new_only):,} 行")
    con.close()

    if len(df_new_only) > 0:
        df_combined = pd.concat([df_combined, df_new_only], ignore_index=True)
        df_combined = df_combined.sort_values(['symbol', 'date']).reset_index(drop=True)
        log(f"  含 TDX 独有后: {len(df_combined):,} 行")

    # Step 3: 写回 Parquet
    log(f"Step 3: 写入 {OUT_PATH}...")
    tmp = OUT_PATH + '.tmp'
    sample = df_combined.head(100)
    schema2 = pa.Table.from_pandas(sample, preserve_index=False).schema
    RG = 1_000_000
    writer = pq.ParquetWriter(tmp, schema2, compression='snappy', use_dictionary=True)
    for start in range(0, len(df_combined), RG):
        end = min(start + RG, len(df_combined))
        chunk = df_combined.iloc[start:end]
        table = pa.Table.from_pandas(chunk, preserve_index=False, schema=schema2)
        writer.write_table(table)
        log(f"  RG {start//RG}: {end:,} 行")
    writer.close()
    os.replace(tmp, OUT_PATH)
    sz = os.path.getsize(OUT_PATH)
    log(f"完成! {len(df_combined):,} 行, {sz/1e6:.1f} MB, 耗时 {(time.time()-t0)/60:.1f} min")

    # 验证改善
    log("验证 amount 改善:")
    con = duckdb.connect(':memory:')
    new_stats = con.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amt_null,
            SUM(CASE WHEN amount = 0 OR amount IS NULL THEN 1 ELSE 0 END) AS amt_zero_or_null
        FROM read_parquet('{OUT_PATH}')
    """).fetchone()
    log(f"  总 {new_stats[0]:,}, NULL {new_stats[1]:,}, 0或NULL {new_stats[2]:,}")
    log(f"  改善: NULL 减少 {stats[1] - new_stats[1]:,}, "
        f"0或NULL 减少 {stats[2] - new_stats[2]:,}")
    con.close()

    # 清理临时
    try:
        os.remove(tmp_tdx)
        os.rmdir(TMP_DIR)
    except: pass
    log("=== 全部完成 ===")

if __name__ == '__main__':
    main()