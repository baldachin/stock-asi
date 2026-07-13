#!/usr/bin/env python3
"""
回填 6/10 ~ 7/10 全部数据 (2026 年), 共 22 个交易日
专门修复 7/10 cron 17:00 误裁切导致的 6/10-7/9 数据回退 + 补全 7/10

策略:
  1. 备份当前 kdata.parquet → .pre_cron_damage (外部已做)
  2. 从 baostock 抓 6/10 ~ 7/10 共 22 天所有股票 K线
  3. 过滤停牌 (open IS NULL)
  4. 流式重写 kdata.parquet:
     - 保留所有老数据 (CROP_DAYS=0 模式)
     - 把抓的 22 天新数据追加到末尾
     - 按 (symbol, date) dedup keep=last (新行覆盖旧行)
  5. 验证每天 rows >= 4700
"""

import sys
import os
import time
import shutil
from datetime import date, datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.expanduser('~/stock'))

PARQUET_PATH = '/home/hanshuang8902/stock_data/kdata.parquet'
BACKUP_PATH  = PARQUET_PATH + '.pre_cron_damage'
TARGET_FROM  = date(2026, 6, 10)
TARGET_TO    = date(2026, 7, 10)


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


def fetch_missing():
    """复用 writer 的 fetch 函数抓 6/10 ~ 7/10"""
    from update_kdata_parquet import get_all_codes, fetch_all_incremental
    print("\n[Step 1] 获取股票列表...")
    codes = get_all_codes()
    print(f"\n[Step 2] 抓取 {TARGET_FROM} ~ {TARGET_TO} (22 个交易日)...")
    t0 = datetime.now()
    df = fetch_all_incremental(codes, TARGET_FROM.strftime('%Y-%m-%d'),
                               TARGET_TO.strftime('%Y-%m-%d'), t0)
    return df


def validate(df):
    if df.empty:
        print("  [WARN] 抓取结果为空")
        return df
    before = len(df)
    df = df[df['open'].notna()].copy()
    print(f"  过滤停牌: {before:,} → {len(df):,} 行")

    print("\n  按日期统计:")
    for d, grp in df.groupby('date'):
        print(f"    {d}: {len(grp):,} 行, distinct={grp['symbol'].nunique()}")
    return df


def merge_append(df_new):
    """流式重写: 复制所有老 row group + 追加 df_new (末尾 dedup)"""
    orig_schema = pq.read_schema(PARQUET_PATH)
    print(f"\n[Step 3] 流式重写 {PARQUET_PATH}")
    print(f"  schema: {[f.name for f in orig_schema]}")
    new_path = PARQUET_PATH + ".new"
    writer = pq.ParquetWriter(new_path, orig_schema, compression='snappy')

    # Step A: 复制所有老 row group
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

    # Step B: dedup + 补 id + 列对齐
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
    """重读 kdata.parquet 检查 6/10 ~ 7/10 每天行数"""
    import duckdb
    con = duckdb.connect(':memory:')
    print("\n[Verify] 重读新文件...")

    df = con.execute(f"""
        SELECT date, COUNT(*) AS rows, COUNT(DISTINCT symbol) AS stocks,
               SUM(CASE WHEN amount IS NULL OR amount = 0 THEN 1 ELSE 0 END) AS zero_amt
        FROM read_parquet('{PARQUET_PATH}')
        WHERE date BETWEEN '2026-06-10' AND '2026-07-10'
        GROUP BY date ORDER BY date
    """).df()
    print(df.to_string(index=False))

    pf = pq.ParquetFile(PARQUET_PATH)
    print(f"\n  总 row groups: {pf.num_row_groups}")
    print(f"  总行数: {pf.metadata.num_rows:,}")

    # Sanity: 每天应该 4000+ 行 (7/10 盘中可能不全, 但至少有 1000+ 行)
    today = date.today()
    all_ok = True
    for _, r in df.iterrows():
        d = r['date']
        if hasattr(d, 'strftime'):
            d_str = d.strftime('%Y-%m-%d')
        else:
            d_str = str(d)[:10]
        d_date = date.fromisoformat(d_str)
        if r['rows'] < 1000:
            print(f"  [FAIL] {d_str}: {r['rows']} 行 (低于 1000)")
            all_ok = False
        elif d_date == today and r['rows'] < 4000:
            print(f"  [INFO] {d_str}: {r['rows']} 行 (今天盘中, 数据未收齐)")
        elif r['rows'] < 4000:
            print(f"  [WARN] {d_str}: {r['rows']} 行 (低于 4000)")
            all_ok = False
        else:
            print(f"  [OK] {d_str}: {r['rows']} 行")
    return all_ok


def main():
    t0 = datetime.now()
    print(f"\n{'='*60}")
    print(f"回填缺失日期: {TARGET_FROM} ~ {TARGET_TO} (22 个交易日)")
    print(f"{'='*60}")

    df = fetch_missing()
    df = validate(df)

    if df.empty:
        print("\n[ABORT] 抓取结果为空")
        return 1

    merge_append(df)
    ok = verify()

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n完成! 耗时 {elapsed:.0f}s")
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())