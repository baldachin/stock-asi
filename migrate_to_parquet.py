#!/usr/bin/env python3
"""
DuckDB → Parquet 一次性迁移脚本 (流式版本, 内存友好)

输出:
  stock.db:kdata            → ~/stock_data/kdata.parquet (流式逐 batch)
  stock.db:asi_yearly       → ~/stock_data/asi_yearly.parquet
  stock.db:asi_yearly_up    → ~/stock_data/asi_yearly_up.parquet
  stock.db:stock_basic      → ~/stock_data/stock_basic.parquet

策略:
  - kdata 不 sort, 不在内存里
  - DuckDB 分批读 (100K 行/batch), pyarrow ParquetWriter 流式写
  - 小表直接 fetch_arrow_table (无 OOM 风险)
  - 写时 row_group_size=1M, 边写边落盘
"""

import sys
import os
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

sys.path.insert(0, 'F:/Develops/stock-asi')
from parquet_atomic import write_atomic

DUCKDB_PATH = 'F:/Develops/stock_data/stock.db'
PARQUET_DIR = 'F:/Develops/stock_data'

def check_no_holders():
    import subprocess
    r = subprocess.run(['fuser', DUCKDB_PATH], capture_output=True, text=True)
    if r.stdout.strip():
        print(f"❌ DuckDB 仍被持有: {r.stdout.strip()}")
        sys.exit(1)
    print("✓ DuckDB 无锁持有者")

def check_no_writer():
    import subprocess
    r = subprocess.run(['pgrep', '-f', 'update_kdata_duckdb'], capture_output=True, text=True)
    if r.stdout.strip():
        print(f"❌ update_kdata_duckdb 仍在跑: {r.stdout.strip()}")
        sys.exit(1)
    print("✓ 无 writer 进程")

def stream_export_kdata(con, target_path, batch_size=200_000):
    """流式导出 kdata (1.2GB, 16M 行) — 内存峰值 < 50MB"""
    print(f"\n[{datetime.now()}] 流式导出 kdata → {target_path}")

    # 1. 拿 schema
    schema = con.execute("SELECT * FROM kdata LIMIT 0").fetch_arrow_table().schema
    print(f"  Schema: {schema.names}")
    print(f"  列: {schema.names}")

    # 2. 拿总行数 (进度条用)
    total = con.execute("SELECT COUNT(*) FROM kdata").fetchone()[0]
    print(f"  总行数: {total:,}")

    # 3. 流式 writer
    new_path = target_path + ".new"
    writer = pq.ParquetWriter(new_path, schema, compression='snappy')
    written = 0
    t0 = datetime.now()

    # DuckDB 不直接支持流式 Arrow, 但可以 LIMIT/OFFSET 循环
    # 或者用 arrow 格式批量读 (但内存大)
    # 折中: 一次读 batch_size 行, 写, 释放
    offset = 0
    while offset < total:
        sql = f"SELECT * FROM kdata LIMIT {batch_size} OFFSET {offset}"
        table = con.execute(sql).fetch_arrow_table()
        if len(table) == 0:
            break
        writer.write_table(table)
        written += len(table)
        offset += batch_size
        elapsed = (datetime.now() - t0).total_seconds()
        rate = written / elapsed if elapsed > 0 else 0
        print(f"  [{written:>10,}/{total:,}] {elapsed:5.0f}s | {rate:>6.0f} 行/s", end='\r', flush=True)
        del table
    print()
    writer.close()

    # 4. fsync + atomic rename
    fd = os.open(new_path, os.O_RDONLY)
    os.fsync(fd); os.close(fd)
    os.replace(new_path, target_path)
    print(f"  ✓ 写入 {target_path}, 大小: {os.path.getsize(target_path) / 1e6:.1f} MB, 耗时: {(datetime.now() - t0).total_seconds():.0f}s")

def export_small_table(con, table_name, target_path):
    """导小表 (< 100MB) — 一次性 fetch_arrow_table"""
    print(f"\n[{datetime.now()}] 导出 {table_name} → {target_path}")
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"  行数: {count:,}")

    table = con.execute(f"SELECT * FROM {table_name}").fetch_arrow_table()
    print(f"  内存: {table.nbytes / 1e6:.2f} MB")

    write_atomic(table, target_path, compression='snappy', row_group_size=100_000)
    size_mb = os.path.getsize(target_path) / 1e6
    print(f"  写入: {size_mb:.2f} MB")

    # 验证
    t_back = pq.read_table(target_path)
    assert len(t_back) == count, f"行数不对: {len(t_back)} vs {count}"
    print(f"  ✓ 验证: {len(t_back):,} 行")
    del t_back, table

def main():
    print("===== DuckDB → Parquet 流式迁移 =====\n")
    check_no_holders()
    check_no_writer()

    print(f"\nDuckDB: {DUCKDB_PATH}")
    print(f"Parquet 目录: {PARQUET_DIR}\n")

    # 备份已有 Parquet
    for fname in ['asi_yearly.parquet', 'stock_basic.parquet']:
        src = f'{PARQUET_DIR}/{fname}'
        if os.path.exists(src) and not os.path.exists(src + '.pre_migration.bak'):
            print(f"[{datetime.now()}] 备份 {fname}")
            import shutil
            shutil.copy2(src, src + '.pre_migration.bak')

    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        # kdata 最大, 流式导出
        stream_export_kdata(con, f'{PARQUET_DIR}/kdata.parquet')

        # 小表
        export_small_table(con, 'asi_yearly',    f'{PARQUET_DIR}/asi_yearly.parquet')
        export_small_table(con, 'asi_yearly_up', f'{PARQUET_DIR}/asi_yearly_up.parquet')
        export_small_table(con, 'stock_basic',   f'{PARQUET_DIR}/stock_basic.parquet')
    finally:
        con.close()

    print(f"\n[{datetime.now()}] ✓ 全部导出完成")

if __name__ == '__main__':
    main()
