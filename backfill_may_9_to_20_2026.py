#!/usr/bin/env python3
"""
回填 2026-05-09 ~ 2026-05-20 共 10 个交易日 (从 baostock 抓)

背景:
- 5/9~5/20 数据原本缺失 (pre_may_backfill 没有, stock.db 已被清理)
- 直接调 update_kdata_parquet.py 的抓取函数 + 合并逻辑

用法:
    ~/stock/.venv/bin/python ~/stock/backfill_may_9_to_20_2026.py
"""
import sys
import os
import time
import duckdb
import baostock as bs
import pandas as pd

# 复用 writer 里的抓取逻辑
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from update_kdata_parquet import fetch_one, get_all_codes

CURRENT_PATH = '/home/hanshuang8902/stock_data/kdata.parquet'
TMP_PATH = '/home/hanshuang8902/stock_data/kdata.backfill_5_9_5_20_tmp.parquet'

FROM_DATE = '2026-05-06'
TO_DATE   = '2026-05-20'
BATCH_SIZE = 50


def main():
    codes = get_all_codes()
    if not codes:
        raise RuntimeError('获取股票列表失败')
    print(f'[backfill] 股票数: {len(codes)}, 抓取 {FROM_DATE} ~ {TO_DATE}', flush=True)

    # 抓取 (单线程顺序, 跟 writer 一样)
    batches = [codes[i:i+BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
    total_batches = len(batches)
    all_dfs = []
    total_rows = 0
    t0 = time.time()

    for batch_idx, batch in enumerate(batches):
        bs.login()
        try:
            for code in batch:
                df = fetch_one(code, FROM_DATE, TO_DATE)
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

    # dedup by symbol+date
    before = len(df_new)
    df_new = df_new.sort_values(['symbol', 'date']).drop_duplicates(['symbol', 'date'], keep='last')
    after = len(df_new)
    if before != after:
        print(f'[backfill] 去重: {before:,} → {after:,}', flush=True)

    # 取 schema + max_id (用 pyarrow.parquet 拿 schema)
    import pyarrow.parquet as pq
    target_schema = pq.read_schema(CURRENT_PATH)
    con = duckdb.connect(':memory:')
    max_id = con.execute(f"SELECT COALESCE(MAX(id), 0) FROM read_parquet('{CURRENT_PATH}')").fetchone()[0]
    print(f'[backfill] max_id: {max_id:,}', flush=True)

    # 补 id 列
    import pyarrow as pa
    id_arr = pa.array(range(max_id + 1, max_id + 1 + len(df_new)), type=pa.int64())
    # df_new columns: date, symbol, open, high, low, close, volume, amount
    # schema 顺序: id, symbol, date, open, high, low, close, volume, amount
    df_with_id = df_new.copy()
    df_with_id['id'] = id_arr.to_pylist()
    df_with_id = df_with_id[['id', 'symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']]

    # 流式合并: 先写当前 kdata (< 5/9), 再 append df_with_id (5/9~5/20), 再写当前 kdata (> 5/20)
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    from datetime import date as _date

    current_pf = pq.ParquetFile(CURRENT_PATH)
    target_schema = pq.read_schema(CURRENT_PATH)
    writer = pq.ParquetWriter(TMP_PATH, target_schema, compression='snappy')

    # Step 1: 写 current < 5/9
    print(f'[backfill] Step 1: 写 current < {FROM_DATE}', flush=True)
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
    print(f'  保留 {kept1:,} 行 ({time.time()-t0:.1f}s)', flush=True)

    # Step 2: 写 backfill 数据
    print(f'[backfill] Step 2: 写 backfill {FROM_DATE}~{TO_DATE}', flush=True)
    t0 = time.time()
    # 转 pyarrow.Table 并确保 schema 一致
    table_new = pa.Table.from_pandas(df_with_id, preserve_index=False, safe=False)
    if table_new.schema != target_schema:
        table_new = table_new.cast(target_schema, safe=False)
    writer.write_table(table_new)
    print(f'  写入 {len(df_with_id):,} 行 ({time.time()-t0:.1f}s)', flush=True)

    # Step 3: 写 current > 5/20
    print(f'[backfill] Step 3: 写 current > {TO_DATE}', flush=True)
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
    print(f'\n[backfill] 验证 5/6~5/22 范围:', flush=True)
    df_check = con.execute(f"""
        SELECT date, COUNT(*) AS rows, COUNT(DISTINCT symbol) AS stocks
        FROM read_parquet('{CURRENT_PATH}')
        WHERE date BETWEEN '2026-05-06' AND '2026-05-22'
        GROUP BY date ORDER BY date
    """).df()
    print(df_check.to_string(index=False))

    print(f'\n[backfill] DONE.', flush=True)


if __name__ == '__main__':
    main()