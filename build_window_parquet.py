#!/usr/bin/env python3
"""
从全量 kdata.parquet 构建最近 N 年窗口版 kdata_{N}y.parquet
供 dashboard.py 优先读取, 显著降低内存峰值

输入: ~/stock_data/kdata.parquet (全量, 16M 行, ~3.7GB 物化内存)
输出: ~/stock_data/kdata_{N}y.parquet (窗口, N=5 默认 → ~1.3GB 物化)

用法:
    ~/stock/.venv/bin/python ~/stock/build_window_parquet.py             # 默认 5 年
    ~/stock/.venv/bin/python ~/stock/build_window_parquet.py --years 2   # 自定义

原理:
- 用 duckdb in-memory 读全量 parquet, WHERE date >= MAX(date) - INTERVAL 'N years'
- pyarrow.Table 写出 (snappy 压缩), write_atomic 原子替换

调用方:
- 手动: 首次启动前跑一次 (dashboard.py 启动 ~5s → ~1.5s)
- 定时: ~/stock/run_update_cron.sh 末尾追加 (writer 完成后跑一次)
- 降级: dashboard.py 检测不到 window parquet 时, 自动用全量 + WHERE 过滤
"""
import sys
import os
import time
import argparse
import duckdb
import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parquet_atomic import write_atomic

# ========== 配置 ==========
KDATA_FULL = os.path.expanduser('~/stock_data/kdata.parquet')


def build_window(years: int = 5) -> str:
    """构建最近 N 年窗口版 parquet. 返回输出路径."""
    out_path = os.path.expanduser(f'~/stock_data/kdata_{years}y.parquet')

    if not os.path.exists(KDATA_FULL):
        raise FileNotFoundError(f'kdata.parquet not found: {KDATA_FULL}')

    con = duckdb.connect(':memory:')
    con.execute('SET threads TO 2')

    print(f'[build_window] {years}y from {KDATA_FULL}', flush=True)
    t0 = time.time()

# 用 duckdb 直接读 parquet + filter, 拿到 pyarrow.Table
    # 注意: SELECT * 保持原 schema (symbol, date, open, high, low, close, volume, amount)
    table = con.execute(f"""
        SELECT symbol, date, open, high, low, close, volume, amount
        FROM read_parquet('{KDATA_FULL}')
        WHERE date >= (SELECT MAX(date) FROM read_parquet('{KDATA_FULL}')) - INTERVAL '{years} years'
    """).fetch_arrow_table()

    rows = table.num_rows
    print(f'[build_window] filtered to {rows:,} rows in {time.time()-t0:.1f}s', flush=True)

    # 看一下日期范围
    date_col = table.column('date').to_pylist()
    print(f'[build_window] date range: {min(date_col)} ~ {max(date_col)}', flush=True)

    # 原子写入
    t0 = time.time()
    write_atomic(table, out_path)
    sz = os.path.getsize(out_path) / 1024 / 1024
    print(f'[build_window] wrote {out_path} ({sz:.1f} MB) in {time.time()-t0:.1f}s', flush=True)
    print(f'[build_window] DONE.', flush=True)
    return out_path


def main():
    p = argparse.ArgumentParser(description='构建窗口版 kdata_{N}y.parquet')
    p.add_argument('--years', type=int, default=5, help='窗口年数 (默认 5)')
    args = p.parse_args()
    build_window(args.years)


if __name__ == '__main__':
    main()