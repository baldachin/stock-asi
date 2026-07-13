#!/usr/bin/env python3
"""
回填 7/9 和 7/10 两天数据 (不裁切老数据, 严格只补缺口)

逻辑:
  1. 备份当前 kdata.parquet → kdata.parquet.pre_jul_backfill
  2. 复用 update_kdata_parquet 的 fetch_one/fetch_all_incremental 抓 7/9 + 7/10
  3. 过滤停牌 (open IS NULL)
  4. 分配新 id (max_id + 1 起)
  5. pyarrow 流式重写:
     - 复制所有老 row group 不动
     - 追加 7/9 + 7/10 新行到末尾
  6. 验证: 重读新文件, 检查 7/9 rows=4778 左右, 7/10 同
"""

import sys
import os
import time
from datetime import date, datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

sys.path.insert(0, os.path.expanduser('~/stock'))

PARQUET_PATH = '/home/hanshuang8902/stock_data/kdata.parquet'
BACKUP_PATH  = PARQUET_PATH + '.pre_jul_backfill'
# 7/9 缺 4372 行 (只剩 406 行 301048-302132 区间)
# 7/10 是今天盘中, baostock query_history_k_data_plus 拿不到当日 K 线 (需要等收盘), 跳过
MISSING_DATES = [date(2026, 7, 9)]
TARGET_END = MISSING_DATES[-1].strftime('%Y-%m-%d')
TARGET_START = MISSING_DATES[0].strftime('%Y-%m-%d')


def get_max_id(path):
    pf = pq.ParquetFile(path)
    if 'id' not in pf.schema_arrow.names:
        return 0
    max_id = 0
    for i in range(pf.num_row_groups):
        col = pf.read_row_group(i, columns=['id']).column('id').to_pylist()
        if col:
            max_id = max(max_id, max(col))
    return max_id


def backup():
    import shutil
    if os.path.exists(BACKUP_PATH):
        print(f"  备份已存在 ({BACKUP_PATH}), 跳过")
        return
    shutil.copy2(PARQUET_PATH, BACKUP_PATH)
    size_mb = os.path.getsize(BACKUP_PATH) / 1024 / 1024
    print(f"  ✓ 备份: {BACKUP_PATH} ({size_mb:.1f}MB)")


def fetch_missing():
    """复用 writer 的 fetch 函数抓 7/9 + 7/10"""
    from update_kdata_parquet import get_all_codes, fetch_all_incremental
    print("\n[Step 1] 获取股票列表...")
    codes = get_all_codes()
    print(f"\n[Step 2] 抓取 {TARGET_START} ~ {TARGET_END} 缺失日期...")
    t0 = datetime.now()
    df = fetch_all_incremental(codes, TARGET_START, TARGET_END, t0)
    return df


def validate(df):
    """过滤停牌 + 检查每天行数"""
    if df.empty:
        print("  [WARN] 抓取结果为空")
        return df
    before = len(df)
    # 过滤停牌 (open IS NULL)
    df = df[df['open'].notna()].copy()
    print(f"  过滤停牌: {before:,} → {len(df):,} 行")

    # 按日期统计
    print("\n  按日期统计:")
    for d, grp in df.groupby('date'):
        print(f"    {d}: {len(grp):,} 行, distinct symbols={grp['symbol'].nunique()}")
    return df


def merge_no_crop(df_new):
    """流式重写 kdata.parquet, 严格保留所有老数据, 仅追加 df_new"""
    orig_schema = pq.read_schema(PARQUET_PATH)
    print(f"\n[Step 3] 流式重写 {PARQUET_PATH}")
    print(f"  schema: {[f.name for f in orig_schema]}")
    new_path = PARQUET_PATH + ".new"
    writer = pq.ParquetWriter(new_path, orig_schema, compression='snappy')

    # Step A: 复制所有老 row group (不动)
    pf = pq.ParquetFile(PARQUET_PATH)
    print(f"  Step A: 复制 {pf.num_row_groups} 老 row groups...")
    kept = 0
    for i in range(pf.num_row_groups):
        rg = pf.read_row_group(i)
        writer.write_table(rg)
        kept += rg.num_rows
        if (i + 1) % 20 == 0:
            print(f"    [{i+1}/{pf.num_row_groups}] {kept:,} 行")
    print(f"  ✓ 老数据: {kept:,} 行")

    # Step B: 去重 + 补 id + 列对齐
    if not df_new.empty:
        before = len(df_new)
        df_new = df_new.sort_values(['symbol', 'date']).drop_duplicates(['symbol', 'date'], keep='last')
        after = len(df_new)
        if before != after:
            print(f"  去重: {before:,} → {after:,}")

        if 'id' in orig_schema.names:
            start_id = get_max_id(PARQUET_PATH)
            ids = list(range(start_id + 1, start_id + 1 + len(df_new)))
            df_new = df_new.copy()
            df_new.insert(0, 'id', ids)

        cols_in_order = [c for c in orig_schema.names if c in df_new.columns]
        df_new = df_new[cols_in_order]
        table_new = pa.Table.from_pandas(df_new, preserve_index=False, safe=False)
        if table_new.schema != orig_schema:
            table_new = table_new.cast(orig_schema, safe=False)
        writer.write_table(table_new)
        print(f"  ✓ 新数据: {len(df_new):,} 行")

    writer.close()

    # fsync + atomic rename
    fd = os.open(new_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(new_path, PARQUET_PATH)
    print(f"  ✓ 原子 rename 完成")


def verify():
    """重读 kdata.parquet 检查缺失日期已补"""
    import duckdb
    con = duckdb.connect(':memory:')
    print("\n[Verify] 重读新文件...")
    df = con.execute(f"""
        SELECT date, COUNT(*) AS rows, COUNT(DISTINCT symbol) AS stocks
        FROM read_parquet('{PARQUET_PATH}')
        WHERE date IN ('2026-07-09', '2026-07-10')
        GROUP BY date ORDER BY date
    """).df()
    print(df.to_string(index=False))

    pf = pq.ParquetFile(PARQUET_PATH)
    print(f"\n  总 row groups: {pf.num_row_groups}")
    print(f"  总行数: {pf.metadata.num_rows:,}")

    # 简单 sanity check
    for d in MISSING_DATES:
        d_str = d.strftime('%Y-%m-%d')
        row = df[df['date'].astype(str) == d_str]
        if row.empty:
            print(f"  [FAIL] {d_str} 仍未写入!")
            return False
        r = row.iloc[0]
        if r['rows'] < 4000:
            print(f"  [WARN] {d_str} 只有 {r['rows']} 行 (正常 ~4778)")
        else:
            print(f"  [OK] {d_str}: {r['rows']} 行")
    return True


def main():
    t0 = datetime.now()
    print(f"\n{'='*60}")
    print(f"回填缺失日期: {TARGET_START} ~ {TARGET_END}")
    print(f"{'='*60}")

    print("\n[Step 0] 备份...")
    backup()

    df = fetch_missing()
    df = validate(df)

    if df.empty:
        print("\n[ABORT] 抓取结果为空, 不写入")
        return 1

    merge_no_crop(df)
    ok = verify()

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n完成! 耗时 {elapsed:.0f}s")
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())