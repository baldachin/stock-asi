#!/usr/bin/env python3
"""
小表预迁移: asi_yearly + asi_yearly_up + stock_basic
这些表 < 5MB, writer 跑时也能做 (writer 只锁 kdata)

输出:
  stock.db:asi_yearly    → ~/stock_data/asi_yearly.parquet
  stock.db:asi_yearly_up → ~/stock_data/asi_yearly_up.parquet
  stock.db:stock_basic   → ~/stock_data/stock_basic.parquet

(原 kdata 1.2GB 留给主迁移脚本)
"""

import sys
import os
import duckdb
import pyarrow as pa
from datetime import datetime

sys.path.insert(0, '/home/hanshuang8902/stock')
from parquet_atomic import write_atomic

DUCKDB_PATH = '/home/hanshuang8902/stock_data/stock.db'
PARQUET_DIR = '/home/hanshuang8902/stock_data'

TABLES = [
    ('asi_yearly',    f'{PARQUET_DIR}/asi_yearly.parquet'),
    ('asi_yearly_up', f'{PARQUET_DIR}/asi_yearly_up.parquet'),
    ('stock_basic',   f'{PARQUET_DIR}/stock_basic.parquet'),
]

def main():
    print("===== 小表预迁移 (asi_yearly, asi_yearly_up, stock_basic) =====\n")

    # 检查 writer
    import subprocess
    r = subprocess.run(['pgrep', '-f', 'update_kdata_duckdb'], capture_output=True, text=True)
    if r.stdout.strip():
        print(f"⚠️ update_kdata_duckdb 仍在跑: {r.stdout.strip()}")
        print("   这些小表的 export 也会读 stock.db, 必须有 read lock")
        print("   等 writer 跑完再做, 或先停掉 writer")
        # 不退出, 尝试一下 (DuckDB read_only 不会阻塞 read_only)
    else:
        print("✓ 无 writer 进程")

    # 备份已有 Parquet
    for _, path in TABLES:
        if os.path.exists(path):
            bak = path + '.pre_migration.bak'
            if not os.path.exists(bak):
                print(f"[{datetime.now()}] 备份 {os.path.basename(path)} → {bak}")
                import shutil
                shutil.copy2(path, bak)

    # DuckDB read_only 打开
    # 注意: 如果 writer 正在写, duckdb.connect(read_only=True) 可能会冲突
    # DuckDB 1.5.x 允许 read_only + read_only 并发, 但 read_only + write 互斥
    print(f"\n打开 {DUCKDB_PATH} (read_only)...")
    try:
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
    except Exception as e:
        if 'lock' in str(e).lower():
            print(f"❌ DuckDB 仍被 writer 持锁, 退出: {e}")
            sys.exit(1)
        raise

    try:
        for table_name, target_path in TABLES:
            print(f"\n[{datetime.now()}] 导出 {table_name} → {target_path}")
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  行数: {count:,}")

            table = con.execute(f"SELECT * FROM {table_name}").fetch_arrow_table()
            print(f"  内存: {table.nbytes / 1e6:.2f} MB")

            write_atomic(table, target_path, compression='snappy', row_group_size=1_000_000)
            size_mb = os.path.getsize(target_path) / 1e6
            print(f"  写入: {size_mb:.2f} MB")

            # 验证
            t_back = pa.parquet.read_table(target_path)
            assert len(t_back) == count
            print(f"  ✓ 验证: {len(t_back):,} 行")
            del t_back, table
    finally:
        con.close()

    print(f"\n[{datetime.now()}] ✓ 小表迁移完成")
    print("\n下一步: 等 kdata writer 跑完后跑 migrate_to_parquet.py 主脚本")

if __name__ == '__main__':
    main()
