#!/usr/bin/env python3
"""
ASI (成交额强度指标) 计算 - Parquet 版 (替代 asi_calculator_duckdb.py)

公式 (v2 - 价格加权):
  base = ln(max_rank + 1 - rank) / ln(max_rank + 1) × 100        (0~100, 排名强度)
  weight = 1 + tanh(ret_pct / K)                                (0~2, 价格方向)
  score = base × weight                                          (-100~+200)

数据源: ~/stock_data/kdata.parquet (DuckDB 读 Parquet)
输出:  ~/stock_data/asi_yearly.parquet 或 asi_yearly_up.parquet

策略: 用 in-memory DuckDB 计算 SQL, 写 Parquet (原子替换)
"""

import sys
import duckdb
import time
from datetime import datetime

sys.path.insert(0, '/home/hanshuang8902/stock')
from parquet_atomic import write_atomic
import pyarrow as pa

KDATA_PATH = '/home/hanshuang8902/stock_data/kdata.parquet'
ASI_PATH   = '/home/hanshuang8902/stock_data/asi_yearly.parquet'
ASI_UP_PATH = '/home/hanshuang8902/stock_data/asi_yearly_up.parquet'
K = 3.0

def calculate_asi(year: int = None, up_only: bool = False, weighted: bool = True):
    """计算 ASI 年度得分, 写入对应 Parquet 文件"""
    con = duckdb.connect(':memory:')
    con.execute(f"CREATE VIEW kdata AS SELECT * FROM read_parquet('{KDATA_PATH}')")

    if year is None:
        years = con.execute("SELECT DISTINCT year(date)::INT FROM kdata ORDER BY 1").fetchall()
        years = [r[0] for r in years]
        print(f"发现数据年份: {years}")
        for y in years:
            calculate_asi(y, up_only=up_only, weighted=weighted)
        con.close()
        return

    # 模式
    if up_only and weighted:
        mode = "v1+混合 (不建议)"
    elif up_only:
        mode = "v1 仅上涨日"
    elif weighted:
        mode = f"v2 价格加权 (K={K})"
    else:
        mode = "原版 无加权"
    print(f"\n[{datetime.now()}] 计算 {year} 年 ASI (口径: {mode})...")

    if up_only:
        up_filter = "AND close > open"
        ret_expr = "1.0"
    else:
        up_filter = ""
        if weighted:
            ret_expr = f"(1 + TANH(CASE WHEN open > 0 THEN (close - open) / open * 100 ELSE 0 END / {K}))"
        else:
            ret_expr = "1.0"

    # 计算所有年份, 写到内存 Table
    df = con.execute(f"""
        WITH daily_scored AS (
            SELECT
                symbol,
                date,
                amount,
                close,
                open,
                CAST(RANK() OVER (PARTITION BY date ORDER BY amount DESC) AS DOUBLE) AS amount_rank,
                CAST(COUNT(*)   OVER (PARTITION BY date) AS DOUBLE)         AS max_rank
            FROM kdata
            WHERE year(date) = {year}
              AND amount > 0
              {up_filter}
        ),
        daily_asi AS (
            SELECT
                symbol,
                date,
                amount_rank,
                max_rank,
                CASE WHEN amount_rank <= max_rank
                     THEN LN(max_rank + 1 - amount_rank) / LN(max_rank + 1) * 100 * {ret_expr}
                     ELSE 0 END AS asi_score,
                (amount_rank <= 50)  AS in_top50,
                (amount_rank <= 100) AS in_top100
            FROM daily_scored
        ),
        yearly AS (
            SELECT
                symbol,
                COUNT(*)                                       AS asi_trading_days,
                SUM(asi_score)                                AS asi_sum,
                AVG(asi_score)                                AS asi_mean,
                STDDEV(asi_score)                             AS asi_std,
                MIN(amount_rank)                              AS asi_best_rank,
                AVG(amount_rank)                              AS asi_avg_rank,
                SUM(CASE WHEN in_top50  THEN 1 ELSE 0 END)   AS top50_days,
                SUM(CASE WHEN in_top100 THEN 1 ELSE 0 END)   AS top100_days,
                SUM(asi_score) / (COUNT(*) * 100.0)          AS asi_score_ratio
            FROM daily_asi
            GROUP BY symbol
        )
        SELECT
            symbol,
            {year}                                           AS year,
            asi_sum,
            asi_mean,
            asi_std,
            asi_trading_days,
            asi_best_rank,
            asi_avg_rank,
            top50_days,
            top100_days,
            asi_score_ratio,
            RANK() OVER (ORDER BY asi_sum DESC)             AS asi_yearly_rank
        FROM yearly
    """).df()

    # 写 Parquet (原子)
    target = ASI_UP_PATH if up_only else ASI_PATH
    print(f"  写入 {target} ...")
    table = pa.Table.from_pandas(df, preserve_index=False)
    write_atomic(table, target, compression='snappy', row_group_size=1_000_000)

    # 验证
    stat = df
    print(f"  {year}年: {len(stat):,} 只, asi_sum均值: {stat['asi_sum'].mean():.2f}")
    print(f"[{datetime.now()}] {year} 年完成")
    con.close()

if __name__ == '__main__':
    import sys
    year = int(sys.argv[1]) if len(sys.argv) > 1 else None
    up_only = '--up' in sys.argv
    weighted = '--no-weighted' not in sys.argv
    calculate_asi(year, up_only=up_only, weighted=weighted)
