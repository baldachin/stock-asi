#!/usr/bin/env python3
"""
5月数据回填 (2026-05-06 ~ 2026-05-20，共 11 个交易日)

背景:
  - 旧 stock.db (后悔窗口) 里这 11 天数据完整, amount 字段齐全
  - kdata.parquet 里这段整段缺失 (4/30 → 5/21 跳跃), 原因: writer CROP_DAYS=30
    裁剪后, 后续增量没拉到这段
  - 本脚本: 从旧 db 导出 5/6-5/20 → merge (不是 update) 到 kdata.parquet → 原子替换

修复完后, 旧 db 该按 6/12 cron 计划清理

用法:
  cd ~/stock && .venv/bin/python backfill_may_2026.py
"""

import os
import sys
import shutil
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime

PARQUET_PATH = '/home/hanshuang8902/stock_data/kdata.parquet'
OLD_DB_PATH  = '/home/hanshuang8902/stock_data/stock.db'
BACKUP_PATH  = '/home/hanshuang8902/stock_data/kdata.parquet.pre_may_backfill'

DATE_START = date(2026, 5, 6)
DATE_END   = date(2026, 5, 20)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def fetch_missing_window():
    """从旧 db 读 5/6-5/20 数据"""
    log(f"打开旧 db: {OLD_DB_PATH} (read_only)")
    con = duckdb.connect(OLD_DB_PATH, read_only=True)
    try:
        # 校验表里字段顺序
        cols = [r[0] for r in con.execute("DESCRIBE kdata").fetchall()]
        log(f"  旧 db kdata schema: {cols}")

        df = con.execute(f"""
            SELECT date, symbol, open, high, low, close, volume, amount
            FROM kdata
            WHERE date >= '{DATE_START}' AND date <= '{DATE_END}'
            ORDER BY date, symbol
        """).fetch_df()
    finally:
        con.close()

    df['date'] = pd.to_datetime(df['date']).dt.date
    for c in ['open','high','low','close','volume','amount']:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    log(f"  拉出 {len(df):,} 行, 覆盖 {df['date'].min()} ~ {df['date'].max()}")
    return df

def validate(df):
    """校验拉出的数据没有空 amount / 异常

    注意: parquet 历史风格是停牌日不写行 (5/21+ 全无 NULL), 所以把 NULL 行过滤掉
    """
    n_zero = (df['amount'] == 0).sum()
    n_null_any = df.isnull().any(axis=1).sum()
    n_dup  = df.duplicated(subset=['date','symbol']).sum()
    log(f"  原始: amount=0 行数: {n_zero}, 任意字段空: {n_null_any}, (date,symbol) 重复: {n_dup}")
    if n_zero > 0 or n_dup > 0:
        raise RuntimeError(f"数据校验未通过, 拒绝写入 (zero={n_zero}, dup={n_dup})")

    # 过滤掉停牌 (open IS NULL) 的行 — 与 parquet 现有风格一致
    n_halted = df['open'].isnull().sum()
    if n_halted > 0:
        log(f"  过滤停牌 (open IS NULL) 行: {n_halted} 行 (与 parquet 5/21+ 风格一致)")
        df = df[df['open'].notnull()].copy().reset_index(drop=True)
    return df

def check_already_present(df_new):
    """保险检查: kdata.parquet 里目标日期段应完全没有数据"""
    con = duckdb.connect(':memory:')
    con.execute(f"CREATE VIEW k AS SELECT * FROM read_parquet('{PARQUET_PATH}')")
    n = con.execute(f"""
        SELECT COUNT(*) FROM k
        WHERE date >= '{DATE_START}' AND date <= '{DATE_END}'
    """).fetchone()[0]
    if n > 0:
        raise RuntimeError(
            f"kdata.parquet 里 5月窗口已有 {n} 行, 跟旧 db 重叠, 拒绝写入"
        )
    return True

def merge_atomic(df_new):
    """流式重写 kdata.parquet: 老数据 + 5月新数据 (按 date 排序) → 原子替换"""
    orig_schema = pq.read_schema(PARQUET_PATH)
    log(f"  目标 schema: {orig_schema.names}")

    # 备份
    log(f"备份原文件: {BACKUP_PATH}")
    shutil.copy2(PARQUET_PATH, BACKUP_PATH)

    # 分配 id (兼容 DuckDB 时代 PK)
    if 'id' in orig_schema.names:
        pf = pq.ParquetFile(PARQUET_PATH)
        max_id = 0
        for i in range(pf.num_row_groups):
            col = pf.read_row_group(i, columns=['id']).column('id').to_pylist()
            if col:
                max_id = max(max_id, max(col))
        log(f"  原 max(id) = {max_id:,}")
        df_new = df_new.copy()
        df_new.insert(0, 'id', range(max_id + 1, max_id + 1 + len(df_new)))

    # 对齐列顺序
    cols_in_order = [c for c in orig_schema.names if c in df_new.columns]
    df_new = df_new[cols_in_order]

    new_path = PARQUET_PATH + ".new"
    log(f"开始流式重写到 {new_path}")
    writer = pq.ParquetWriter(new_path, orig_schema, compression='snappy')

    # 1) 复制老数据, 注意: 老数据里没有 5/6-5/20 (已验证), 直接流过
    pf = pq.ParquetFile(PARQUET_PATH)
    kept = 0
    for i in range(pf.num_row_groups):
        rg = pf.read_row_group(i)
        writer.write_table(rg)
        kept += len(rg)
    log(f"  写入老数据: {kept:,} 行 ({pf.num_row_groups} row groups)")

    # 2) 写新数据, 按 date 排序 (老数据按 date 升序, 5/6-5/20 接在 4/30 之后)
    df_new = df_new.sort_values(['date','symbol']).reset_index(drop=True)
    table_new = pa.Table.from_pandas(df_new, preserve_index=False, safe=False)
    if table_new.schema != orig_schema:
        table_new = table_new.cast(orig_schema, safe=False)
    writer.write_table(table_new)
    log(f"  写入新数据: {len(df_new):,} 行 (5月窗口)")

    writer.close()

    # fsync + atomic rename
    fd = os.open(new_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(new_path, PARQUET_PATH)
    log(f"  ✓ 原子替换完成")

def verify():
    """重读新文件, 验证 5/6-5/20 完整填入"""
    con = duckdb.connect(':memory:')
    con.execute(f"CREATE VIEW k AS SELECT * FROM read_parquet('{PARQUET_PATH}')")
    r = con.execute(f"""
        SELECT date, COUNT(*) AS n, COUNT(DISTINCT symbol) AS s,
               SUM(CASE WHEN amount=0 OR amount IS NULL THEN 1 ELSE 0 END) AS z
        FROM k
        WHERE date >= '{DATE_START}' AND date <= '{DATE_END}'
        GROUP BY date ORDER BY date
    """).fetchall()
    log("修复后 5月窗口逐日行数:")
    ok = True
    for d, n, s, z in r:
        flag = "" if z == 0 and n >= 5100 else "  <-- ANOMALY"
        log(f"  {d}  rows={n}  symbols={s}  zero_amt={z}{flag}")
        if z > 0 or n < 5100:
            ok = False
    return ok

def main():
    t0 = datetime.now()
    log("=" * 50)
    log("5月数据回填 (2026-05-06 ~ 2026-05-20)")
    log("=" * 50)

    log("[1/5] 从旧 db 导出 5月数据")
    df = fetch_missing_window()
    log("[2/5] 数据校验 (过滤停牌行)")
    df = validate(df)
    log("[3/5] 确认 kdata.parquet 目标窗口无重叠")
    check_already_present(df)
    log("[4/5] merge 原子写")
    merge_atomic(df)
    log("[5/5] 验证")
    ok = verify()

    elapsed = (datetime.now() - t0).total_seconds()
    log(f"\n完成 ({elapsed:.0f}s)  备份: {BACKUP_PATH}")
    log(f"结果: {'✓ 成功' if ok else '✗ 异常, 请检查'}")
    return 0 if ok else 1

if __name__ == '__main__':
    sys.exit(main())
