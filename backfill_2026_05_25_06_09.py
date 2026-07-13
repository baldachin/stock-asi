#!/usr/bin/env python3
"""
回填 2026-05-25 ~ 2026-06-09 共 12 个交易日 (从 baostock 抓)

背景 (2026-07-13 诊断):
- 5/26-5/29 writer 连续 4 天只写 2324 只 (正常 5133+)
- 6/1-6/5 连续 5 天只写 4466 只
- 6/8 写 4774 只, 6/9 写 2425 只
- 6/10 起恢复正常 (5131+)
- 7/9 和 7/10 cron writer 都跳过了这段缺失日期, 一直没补

策略 (复用 backfill_may_9_to_20_2026.py 模式):
- 复用 update_kdata_parquet.fetch_one + get_all_codes (单线程顺序)
- 用 pyarrow 流式合并 (内存峰值 < 100MB):
  1. 写 current < 5/25 (老数据)
  2. 写 baostock 抓取的 5/25~6/9 数据 (id 续接)
  3. 写 current > 6/9 (新数据)
- 原子 os.replace

用法:
    ~/stock/.venv/bin/python ~/stock/backfill_2026_05_25_06_09.py
"""
import sys
import os
import time
import duckdb
import baostock as bs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datetime import date as _date

# 复用 writer 里的抓取逻辑
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from update_kdata_parquet import fetch_one, get_all_codes

CURRENT_PATH = '/home/hanshuang8902/stock_data/kdata.parquet'
TMP_PATH = '/home/hanshuang8902/stock_data/kdata.backfill_5_25_6_9_tmp.parquet'

FROM_DATE = '2026-05-25'
TO_DATE   = '2026-06-09'
BATCH_SIZE = 50


def fetch_range(codes, from_date, to_date):
    """抓取所有股票在 from_date~to_date 区间的 K 线, 返回 DataFrame (无 id 列)"""
    print(f'[backfill] 股票数: {len(codes)}, 抓取 {from_date} ~ {to_date}', flush=True)

    batches = [codes[i:i+BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
    total_batches = len(batches)
    all_dfs = []
    total_rows = 0
    t0 = time.time()

    for batch_idx, batch in enumerate(batches):
        bs.login()
        try:
            for code in batch:
                df = fetch_one(code, from_date, to_date)
                if not df.empty:
                    all_dfs.append(df)
                    total_rows += len(df)
        finally:
            bs.logout()
        done = batch_idx + 1
        elapsed = time.time() - t0
        print(f'\r  [{done:3d}/{total_batches}] {elapsed:.0f}s | {total_rows:,} 行', end='', flush=True)

    print()
    if not all_dfs:
        raise RuntimeError('抓取结果为空, 无数据可回填')

    df_new = pd.concat(all_dfs, ignore_index=True)
    print(f'[backfill] 抓取完成: {len(df_new):,} 行', flush=True)

    # dedup by symbol+date (保留最后一条)
    before = len(df_new)
    df_new = df_new.sort_values(['symbol', 'date']).drop_duplicates(['symbol', 'date'], keep='last')
    after = len(df_new)
    if before != after:
        print(f'[backfill] 去重: {before:,} → {after:,}', flush=True)
    return df_new


def main():
    # 取股票列表
    codes = get_all_codes()
    print(f'[backfill] A股代码总数: {len(codes)}', flush=True)

    # 抓取数据
    df_new = fetch_range(codes, FROM_DATE, TO_DATE)

    # 取 current schema + max_id
    target_schema = pq.read_schema(CURRENT_PATH)
    con = duckdb.connect(':memory:')
    max_id = con.execute(f"SELECT COALESCE(MAX(id), 0) FROM read_parquet('{CURRENT_PATH}')").fetchone()[0]
    print(f'[backfill] current max_id: {max_id:,}', flush=True)

    # 补 id 列 + 列对齐到 target schema (id, symbol, date, open, high, low, close, volume, amount)
    df_with_id = df_new.copy()
    id_arr = pa.array(range(max_id + 1, max_id + 1 + len(df_with_id)), type=pa.int64())
    df_with_id['id'] = id_arr.to_pylist()
    df_with_id = df_with_id[['id', 'symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']]

    # 流式合并: current < 5/25 → backfill (5/25~6/9) → current > 6/9
    current_pf = pq.ParquetFile(CURRENT_PATH)
    writer = pq.ParquetWriter(TMP_PATH, target_schema, compression='snappy')

    # Step 1: 写 current < 5/25
    print(f'\n[backfill] Step 1: 写 current < {FROM_DATE}', flush=True)
    t0 = time.time()
    from_ts = _date.fromisoformat(FROM_DATE)
    kept1 = 0
    for rg_idx in range(current_pf.num_row_groups):
        rg = current_pf.read_row_group(rg_idx)
        mask = pc.less(rg.column('date'), from_ts)
        rg_filtered = rg.filter(mask)
        if len(rg_filtered) > 0:
            writer.write_table(rg_filtered)
            kept1 += len(rg_filtered)
        if (rg_idx + 1) % 5 == 0:
            print(f'    row_group {rg_idx+1}/{current_pf.num_row_groups} (kept {kept1:,})', flush=True)
    print(f'  保留 {kept1:,} 行 ({time.time()-t0:.1f}s)', flush=True)

    # Step 2: 写 backfill 数据
    print(f'\n[backfill] Step 2: 写 backfill {FROM_DATE}~{TO_DATE}', flush=True)
    t0 = time.time()
    table_new = pa.Table.from_pandas(df_with_id, preserve_index=False, safe=False)
    if table_new.schema != target_schema:
        table_new = table_new.cast(target_schema, safe=False)
    writer.write_table(table_new)
    print(f'  写入 {len(df_with_id):,} 行 (id {max_id+1}~{max_id+len(df_with_id)}) ({time.time()-t0:.1f}s)', flush=True)

    # Step 3: 写 current > 6/9
    print(f'\n[backfill] Step 3: 写 current > {TO_DATE}', flush=True)
    t0 = time.time()
    after_ts = _date.fromisoformat(TO_DATE)
    kept3 = 0
    for rg_idx in range(current_pf.num_row_groups):
        rg = current_pf.read_row_group(rg_idx)
        mask = pc.greater(rg.column('date'), after_ts)
        rg_filtered = rg.filter(mask)
        if len(rg_filtered) > 0:
            writer.write_table(rg_filtered)
            kept3 += len(rg_filtered)
        if (rg_idx + 1) % 5 == 0:
            print(f'    row_group {rg_idx+1}/{current_pf.num_row_groups} (kept {kept3:,})', flush=True)
    print(f'  保留 {kept3:,} 行 ({time.time()-t0:.1f}s)', flush=True)

    writer.close()

    # 原子替换
    sz = os.path.getsize(TMP_PATH) / 1024 / 1024
    print(f'\n[backfill] 原子替换 {CURRENT_PATH} ({sz:.1f} MB)', flush=True)
    fd = os.open(TMP_PATH, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(TMP_PATH, CURRENT_PATH)
    print(f'  ✓ 替换完成', flush=True)

    # 验证
    print(f'\n[backfill] 验证 5/20~6/15 范围:', flush=True)
    df_check = con.execute(f"""
        SELECT date, COUNT(*) AS rows, COUNT(DISTINCT symbol) AS stocks,
               ROUND(SUM(volume)/1e8, 1) AS shares_yi
        FROM read_parquet('{CURRENT_PATH}')
        WHERE date BETWEEN '2026-05-20' AND '2026-06-15'
          AND volume > 0
        GROUP BY date ORDER BY date
    """).fetchdf()
    print(df_check.to_string(index=False), flush=True)

    df_total = con.execute(f"""
        SELECT MIN(date) AS min_d, MAX(date) AS max_d,
               COUNT(*) AS rows, COUNT(DISTINCT date) AS days,
               COUNT(DISTINCT symbol) AS stocks
        FROM read_parquet('{CURRENT_PATH}')
    """).fetchdf()
    print(f'\n[backfill] 全量统计:', flush=True)
    print(df_total.to_string(index=False), flush=True)

    print(f'\n[backfill] DONE.', flush=True)


if __name__ == '__main__':
    main()