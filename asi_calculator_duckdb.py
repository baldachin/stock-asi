#!/usr/bin/env python3
"""
ASI (成交额强度指标) 计算 - DuckDB SQL 版

公式 (v2 - 价格加权):
  base = ln(max_rank + 1 - rank) / ln(max_rank + 1) × 100        (0~100, 排名强度)
  weight = 1 + tanh(ret_pct / K)                                (0~2, 价格方向)
  score = base × weight                                          (-100~+200)
  - K: 灵敏度参数, 默认 3 (ret_pct=±3% 时 weight 偏离中性 0.46)
  - 上涨: weight>1 加成; 下跌: weight<1 扣分; close=open: weight=1

  rank: 当日成交额排名（1=最高）
  max_rank: 当日有成交的股票总数

口径:
  v1 (旧) up_only=True:  只对上涨日排名, 下跌日不参与
  v2 (新) weighted=True: 每日都排名, 但按 ret_pct 加权 (推荐)

年度聚合: asi_sum, asi_mean, asi_trading_days, top50_days, top100_days
"""

import duckdb
import time
from datetime import datetime

DB = 'F:/Develops/stock_data/stock.db'
K = 3.0  # 灵敏度参数, 越大越不敏感

def _connect_with_retry(max_attempts=20, base_sleep=3.0):
    """开 DuckDB 写连接时如遇 streamlit 持锁, 重试 (backoff 3s/4.5s/6s/...)

    与 update_kdata_duckdb.py 保持一致, 配合 run_update_cron.sh 的 stop-streamlit 流程
    """
    for attempt in range(max_attempts):
        try:
            return duckdb.connect(DB)
        except Exception as e:
            err_str = str(e).lower()
            if 'lock' in err_str or 'io error' in err_str:
                print(f"  [retry] DuckDB 写连接被锁 (第{attempt+1}/{max_attempts}次): {e}", flush=True)
                time.sleep(base_sleep * (attempt + 1) / 2)
                continue
            raise
    raise RuntimeError(f"无法打开 DuckDB 写连接 (重试 {max_attempts} 次后) — streamlit 长时间持锁？")

def calculate_asi(year: int = None, up_only: bool = False, weighted: bool = True):
    """计算 ASI 年度得分, 写入 asi_yearly 表
    up_only:   v1 旧版 - 仅上涨日参与排名
    weighted:  v2 新版 - 全交易日排名, 按涨跌幅加权 (推荐)
    """
    conn = _connect_with_retry()

    if year is None:
        years = conn.execute("SELECT DISTINCT year(date)::INT FROM kdata ORDER BY 1").fetchall()
        years = [r[0] for r in years]
        print(f"发现数据年份: {years}")
        for y in years:
            calculate_asi(y, up_only=up_only, weighted=weighted)
        conn.close()
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
        # 旧版: 仅上涨日参与
        up_filter = "AND close > open"
        ret_expr = "1.0"  # 无价格加权
    else:
        up_filter = ""
        if weighted:
            # v2: 每日都参与, score = base * (1 + tanh(ret/K))
            ret_expr = f"(1 + TANH(CASE WHEN open > 0 THEN (close - open) / open * 100 ELSE 0 END / {K}))"
        else:
            ret_expr = "1.0"

    conn.execute(f"""
        INSERT INTO asi_yearly (symbol, year, asi_sum, asi_mean, asi_std,
            asi_trading_days, asi_best_rank, asi_avg_rank,
            top50_days, top100_days, asi_score_ratio, asi_yearly_rank)
        WITH        daily_scored AS (
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
                -- 价格加权: base * weight
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
        ON CONFLICT (symbol, year) DO UPDATE SET
            asi_sum = excluded.asi_sum,
            asi_mean = excluded.asi_mean,
            asi_std = excluded.asi_std,
            asi_trading_days = excluded.asi_trading_days,
            asi_best_rank = excluded.asi_best_rank,
            asi_avg_rank = excluded.asi_avg_rank,
            top50_days = excluded.top50_days,
            top100_days = excluded.top100_days,
            asi_score_ratio = excluded.asi_score_ratio,
            asi_yearly_rank = excluded.asi_yearly_rank
    """)

    stat = conn.execute(f"""
        SELECT COUNT(*), SUM(CASE WHEN top50_days>0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN top100_days>0 THEN 1 ELSE 0 END), AVG(asi_sum)
        FROM asi_yearly WHERE year = {year}
    """).fetchone()
    print(f"  {year}年: {stat[0]:,} 只, top50>0: {stat[1]:,}, top100>0: {stat[2]:,}, asi_sum均值: {stat[3]:.2f}")

    conn.close()
    print(f"[{datetime.now()}] {year} 年完成")

if __name__ == '__main__':
    import sys
    year = int(sys.argv[1]) if len(sys.argv) > 1 else None
    up_only = '--up' in sys.argv
    weighted = '--no-weighted' not in sys.argv  # 默认开启加权
    calculate_asi(year, up_only=up_only, weighted=weighted)
