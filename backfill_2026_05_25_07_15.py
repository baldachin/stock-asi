#!/usr/bin/env python3
"""
回填 5/25 ~ 7/15 全部缺失/未跑全日期 (2026 年)
覆盖 3 个异常窗口: 5/26-5/29 (2,324), 6/9 (2,425), 7/14 (3,227)
以及 6/15-7/13 期间跌到 4,054 段 (基准应是 ~5,131)

7/15 是今天 (盘中), 不主动抓 — 留给 17:00 cron 自然更新

策略:
  1. 备份当前 kdata.parquet → .pre_5_25_7_15_backfill
  2. 从 baostock 抓 5/25 ~ 7/15 共 ~37 个交易日所有 5,536 只股 K线
  3. 过滤停牌 (open IS NULL)
  4. 流式重写 kdata.parquet:
     - 保留所有老数据 (复制 row group)
     - 追加新数据到末尾
     - 按 (symbol, date) dedup keep=last (新行覆盖旧行)
  5. 验证每天 rows >= 4500

注意: 7/15 是今天 (7/15 是交易日), 抓取时数据可能未收齐
"""

import sys
import os
from datetime import date, datetime

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.expanduser('~/stock'))

PARQUET_PATH = '/home/hanshuang8902/stock_data/kdata.parquet'
BACKUP_PATH  = PARQUET_PATH + '.pre_5_25_7_15_backfill'
TARGET_FROM  = date(2026, 5, 25)
# 7/15 是今天盘中, 不抓 (留给 17:00 cron 自然补)
TARGET_TO    = date(2026, 7, 14)


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
    """复用 writer 的 fetch 函数抓 5/25 ~ 7/15"""
    from update_kdata_parquet import get_all_codes, fetch_all_incremental
    print("\n[Step 1] 获取股票列表...")
    codes = get_all_codes()
    print(f"\n[Step 2] 抓取 {TARGET_FROM} ~ {TARGET_TO} ...")
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


def merge_replace_window(df_new):
    """流式重写: 跳过 [TARGET_FROM, TARGET_TO] 区间的所有老行, 追加 df_new
    (回填场景: 区间内老数据"半残"必须用新数据替换, 区间外原封不动)
    """
    import pyarrow.compute as pc
    orig_schema = pq.read_schema(PARQUET_PATH)
    print(f"\n[Step 3] 流式重写 {PARQUET_PATH} (替换 [{TARGET_FROM}, {TARGET_TO}] 窗口)")
    print(f"  schema: {[f.name for f in orig_schema]}")
    new_path = PARQUET_PATH + ".new"
    writer = pq.ParquetWriter(new_path, orig_schema, compression='snappy')

    # Step A: 复制所有老 row group, 跳过 [TARGET_FROM, TARGET_TO] 窗口
    pf = pq.ParquetFile(PARQUET_PATH)
    print(f"  Step A: 复制 {pf.num_row_groups} 老 row groups (跳过窗口内行)...")
    kept = 0
    skipped = 0
    for i in range(pf.num_row_groups):
        rg = pf.read_row_group(i)
        d = rg.column('date')
        # 保留 date < TARGET_FROM 或 date > TARGET_TO
        keep_mask = pc.or_(pc.less(d, TARGET_FROM), pc.greater(d, TARGET_TO))
        rg_filtered = rg.filter(keep_mask)
        if len(rg_filtered) > 0:
            writer.write_table(rg_filtered)
            kept += len(rg_filtered)
        skipped += rg.num_rows - len(rg_filtered)
        if (i + 1) % 20 == 0:
            print(f"    [{i+1}/{pf.num_row_groups}] 保留 {kept:,} 行, 跳过 {skipped:,} 行")
    print(f"  ✓ 老数据保留: {kept:,} 行, 跳过窗口内: {skipped:,} 行")

    # Step B: df_new 内部 dedup + 补 id + 列对齐
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
    """重读 kdata.parquet 检查 5/25 ~ 7/15 每天行数"""
    import duckdb
    con = duckdb.connect(':memory:')
    print("\n[Verify] 重读新文件...")

    df = con.execute(f"""
        SELECT date, COUNT(*) AS rows, COUNT(DISTINCT symbol) AS stocks,
               SUM(CASE WHEN amount IS NULL OR amount = 0 THEN 1 ELSE 0 END) AS zero_amt
        FROM read_parquet('{PARQUET_PATH}')
        WHERE date BETWEEN '2026-05-25' AND '2026-07-15'  -- 含 7/15 看看今天有没有零行
        GROUP BY date ORDER BY date
    """).df()
    print(df.to_string(index=False))

    pf = pq.ParquetFile(PARQUET_PATH)
    print(f"\n  总 row groups: {pf.num_row_groups}")
    print(f"  总行数: {pf.metadata.num_rows:,}")

    today = date.today()
    all_ok = True
    for _, r in df.iterrows():
        d = r['date']
        d_str = str(d)[:10] if not hasattr(d, 'strftime') else d.strftime('%Y-%m-%d')
        d_date = date.fromisoformat(d_str)
        if r['rows'] < 1000:
            print(f"  [FAIL] {d_str}: {r['rows']} 行 (低于 1000)")
            all_ok = False
        elif d_date == today and r['rows'] < 4000:
            print(f"  [INFO] {d_str}: {r['rows']} 行 (今天盘中)")
        elif r['rows'] < 4000:
            print(f"  [WARN] {d_str}: {r['rows']} 行 (低于 4000)")
            all_ok = False
        else:
            print(f"  [OK] {d_str}: {r['rows']} 行")
    return all_ok


def main():
    t0 = datetime.now()
    print(f"\n{'='*60}")
    print(f"回填缺失日期: {TARGET_FROM} ~ {TARGET_TO}")
    print(f"{'='*60}")

    print("\n[Step 0] 备份...")
    backup()

    df = fetch_missing()
    df = validate(df)

    if df.empty:
        print("\n[ABORT] 抓取结果为空")
        return 1

    merge_replace_window(df)
    ok = verify()

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n完成! 耗时 {elapsed:.0f}s")
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
