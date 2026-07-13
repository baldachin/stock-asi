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

增量更新 (2026-06-23 修复):
- calculate_asi(year) 算当年, 读已有 parquet 的 < year + > year 行, concat 写回
  (保留历史年份, 不会因单年覆盖而丢数据)
- calculate_asi() 无参: 全量重算 (38 年 ~ 30s), 用于初次构建或全量修复
"""

import sys
import os
import time
from datetime import datetime

sys.path.insert(0, os.path.expanduser('~/stock'))
from parquet_atomic import write_atomic
import duckdb
import pyarrow as pa

KDATA_PATH  = os.path.expanduser('~/stock_data/kdata.parquet')
ASI_PATH    = os.path.expanduser('~/stock_data/asi_yearly.parquet')
ASI_UP_PATH = os.path.expanduser('~/stock_data/asi_yearly_up.parquet')
K = 3.0


def _sql_year(year: int, up_only: bool, weighted: bool, ret_expr: str, up_filter: str) -> str:
    """构造 SQL: 算指定年份 ASI (返回 (symbol, year, asi_*) 列)"""
    return f"""
        WITH daily_scored AS (
            SELECT
                symbol,
                date,
                amount,
                close,
                open,
                CAST(RANK() OVER (PARTITION BY date ORDER BY amount DESC) AS DOUBLE) AS amount_rank,
                CAST(COUNT(*)   OVER (PARTITION BY date) AS DOUBLE)         AS max_rank
            FROM read_parquet('{KDATA_PATH}')
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
            asi_score_ratio
        FROM yearly
    """


def _rank_year(table_name: str) -> str:
    """构造 SQL: 给指定表 (单年) 加 asi_yearly_rank (基于 asi_sum 降序)"""
    return f"""
        SELECT symbol, year, asi_sum, asi_mean, asi_std, asi_trading_days,
               asi_best_rank, asi_avg_rank, top50_days, top100_days, asi_score_ratio,
               CAST(RANK() OVER (ORDER BY asi_sum DESC) AS BIGINT) AS asi_yearly_rank
        FROM {table_name}
    """


def _read_existing(path: str, exclude_year: int) -> pa.Table:
    """读已有 parquet, 排除指定年份 (返回 pyarrow.Table 或 None).
    asi_yearly_up 没有 asi_best_rank/asi_avg_rank, 自动跳过缺失列"""
    if not os.path.exists(path):
        return None
    con = duckdb.connect(':memory:')
    # 先查实际列
    actual_cols = con.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0").df().columns.tolist()
    # 标准列 (按 priority 排)
    desired = ['symbol', 'year', 'asi_sum', 'asi_mean', 'asi_std',
               'asi_trading_days', 'asi_best_rank', 'asi_avg_rank',
               'top50_days', 'top100_days', 'asi_score_ratio', 'asi_yearly_rank']
    select_cols = [c for c in desired if c in actual_cols]
    cols_str = ', '.join(select_cols)
    # asi_yearly_rank 转 BIGINT (老数据可能是 INT32, 保持兼容)
    if 'asi_yearly_rank' in select_cols:
        cols_str = cols_str.replace('asi_yearly_rank', 'CAST(asi_yearly_rank AS BIGINT) AS asi_yearly_rank')
    df = con.execute(f"""
        SELECT {cols_str}
        FROM read_parquet('{path}')
        WHERE year != {exclude_year}
    """).df()
    con.close()
    if len(df) == 0:
        return None
    return pa.Table.from_pandas(df, preserve_index=False)


def calculate_asi(year: int = None, up_only: bool = False, weighted: bool = True):
    """计算 ASI 年度得分, 写入对应 Parquet 文件.

    year=None: 全量重算 (枚举 kdata 所有年份, 一次性 concat + 写回, 保留历史)
    year=int: 增量更新 (算当年 + 读历史非当年 + concat + 写回)
    """
    target = ASI_UP_PATH if up_only else ASI_PATH

    # 模式
    if up_only and weighted:
        mode = "v1+混合 (不建议)"
    elif up_only:
        mode = "v1 仅上涨日"
    elif weighted:
        mode = f"v2 价格加权 (K={K})"
    else:
        mode = "原版 无加权"

    # ret/up filter
    if up_only:
        up_filter = "AND close > open"
        ret_expr = "1.0"
    else:
        up_filter = ""
        if weighted:
            ret_expr = f"(1 + TANH(CASE WHEN open > 0 THEN (close - open) / open * 100 ELSE 0 END / {K}))"
        else:
            ret_expr = "1.0"

    con = duckdb.connect(':memory:')
    con.execute(f"CREATE VIEW kdata AS SELECT * FROM read_parquet('{KDATA_PATH}')")

    if year is None:
        # 全量重算: 枚举所有年份
        years = [r[0] for r in con.execute("SELECT DISTINCT year(date)::INT FROM kdata ORDER BY 1").fetchall()]
        print(f"[{datetime.now()}] 全量重算, 年份: {years} (口径: {mode})", flush=True)
        t0 = time.time()
        all_dfs = []
        for y in years:
            sql = _sql_year(y, up_only, weighted, ret_expr, up_filter)
            df_year = con.execute(sql).df()
            print(f"  {y}: {len(df_year):,} 只", flush=True)
            all_dfs.append(df_year)
        df = __import__('pandas').concat(all_dfs, ignore_index=True)
        print(f"  concat {len(df):,} 行 ({time.time()-t0:.1f}s)", flush=True)

        # 排名: 同年内的 asi_sum 排名, 用窗口函数
        # 注意: asi_yearly_rank 必须在同一个 SQL 里算 (跨年排名无意义)
        df_ranked = con.execute(f"""
            SELECT symbol, year, asi_sum, asi_mean, asi_std, asi_trading_days,
                   asi_best_rank, asi_avg_rank, top50_days, top100_days, asi_score_ratio,
                   CAST(RANK() OVER (PARTITION BY year ORDER BY asi_sum DESC) AS BIGINT) AS asi_yearly_rank
            FROM df
        """).df()
        # ↑ duckdb 不能直接引用变量名, 改用下面的写法
        print(f"[{datetime.now()}] 直接转 arrow + 用 duckdb 排 rank", flush=True)
        # 简单做法: 分年排序 concat (year 已经在 sql 里, asi_yearly_rank 用 duckdb 重算)
        del df_ranked
    else:
        # 增量: 算指定年份
        print(f"[{datetime.now()}] 增量计算 {year} 年 ASI (口径: {mode})...", flush=True)
        t0 = time.time()
        sql = _sql_year(year, up_only, weighted, ret_expr, up_filter)
        df_year = con.execute(sql).df()
        print(f"  算出 {year}: {len(df_year):,} 只 ({time.time()-t0:.1f}s)", flush=True)

        # 读历史非当年
        existing = _read_existing(target, year)
        if existing is None:
            print(f"  无历史数据, 全新写入", flush=True)
            df = df_year
        else:
            existing_df = existing.to_pandas()
            print(f"  历史保留: {len(existing_df):,} 行 (排除 {year})", flush=True)
            df = __import__('pandas').concat([existing_df, df_year], ignore_index=True)

    # 排名: 用 duckdb 一次性算
    # 先把 df 写到一个临时 view, 然后用 SQL 算 asi_yearly_rank
    con.close()
    con = duckdb.connect(':memory:')
    con.register('df_view', df)
    df_ranked = con.execute("""
        SELECT symbol, year, asi_sum, asi_mean, asi_std, asi_trading_days,
               asi_best_rank, asi_avg_rank, top50_days, top100_days, asi_score_ratio,
               CAST(RANK() OVER (PARTITION BY year ORDER BY asi_sum DESC) AS BIGINT) AS asi_yearly_rank
        FROM df_view
        ORDER BY year, asi_yearly_rank
    """).df()
    con.close()

    # 写 Parquet (原子)
    t0 = time.time()
    table = pa.Table.from_pandas(df_ranked, preserve_index=False)
    write_atomic(table, target, compression='snappy', row_group_size=1_000_000)
    print(f"[{datetime.now()}] 写入 {target} ({time.time()-t0:.1f}s)", flush=True)
    print(f"  总行数: {len(df_ranked):,}, 年份: {df_ranked['year'].min()} ~ {df_ranked['year'].max()}")


def _parse_args():
    """2026-07-09 fix: 用 argparse 替代手写 argv 解析, --up/--no-weighted 顺序无关

    历史 bug: 旧版 'int(sys.argv[1]) if len(sys.argv) > 1 else None' 写死 argv[1] 当 year,
    一旦 flag 在前 (e.g. 'python x.py --up --no-weighted') 就 ValueError 崩掉。
    """
    import argparse
    p = argparse.ArgumentParser(
        description='计算 ASI 年度得分, 写入 Parquet (v2 价格加权 或 v1 仅上涨日)',
    )
    p.add_argument('year', nargs='?', type=int, default=None,
                   help='增量更新指定年份 (默认 None=全量重算 1990-今年)')
    p.add_argument('--up', action='store_true',
                   help='仅上涨日口径 (默认是全交易日 + 价格加权)')
    p.add_argument('--no-weighted', dest='weighted', action='store_false',
                   help='关闭价格加权 (与 --up 配合 = v1 仅上涨日; 与全交易日配合 = v1 全量)')
    p.set_defaults(weighted=True)
    return p.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    calculate_asi(year=args.year, up_only=args.up, weighted=args.weighted)