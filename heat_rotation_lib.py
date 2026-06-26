#!/usr/bin/env python3
"""
热度轮动共享算法库 (供 dashboard.py 和 sync_heat_rotation.py 复用)

设计: dashboard 用 streamlit 装饰器, cron 脚本要绕过 streamlit, 故把核心算法
单独放在这里, 两边都 import。

数据源: ~/stock_data/{kdata, stock_basic}.parquet
输出:   ~/stock_data/heat_rotation_daily.parquet (append-only, 幂等)
"""
import os
import duckdb
import pandas as pd

KDATA_PATH = os.path.expanduser("~/stock_data/kdata_5y.parquet")  # 默认优先用 window 版
BASIC_PATH = os.path.expanduser("~/stock_data/stock_basic.parquet")
HEAT_PATH = os.path.expanduser("~/stock_data/heat_rotation_daily.parquet")


def _open_con():
    """新开一个 in-memory DuckDB, 注册 kdata + stock_basic"""
    if not os.path.exists(KDATA_PATH):
        # 降级到全量 parquet
        kdata_alt = os.path.expanduser("~/stock_data/kdata.parquet")
        if os.path.exists(kdata_alt):
            kpath = kdata_alt
        else:
            raise FileNotFoundError(f"kdata parquet not found: {KDATA_PATH}")
    else:
        kpath = KDATA_PATH
    con = duckdb.connect()
    con.execute("SET threads TO 2; SET enable_object_cache TO true;")
    con.execute(f"CREATE OR REPLACE TABLE kdata AS SELECT * FROM read_parquet('{kpath}')")
    con.execute(f"CREATE VIEW stock_basic AS SELECT * FROM read_parquet('{BASIC_PATH}')")
    return con


def compute_heat_history(con, days=2, window_days=20, hot_th=80.0, cold_th=50.0):
    """
    多日热度: 返回最近 `days` 个交易日 的 long-format 数据 (date, symbol, heat_pct, amt, signal)
    signal: 'heating' / 'cooling' / 'staying' / 'normal'
    """
    lookback = days * 3 + window_days + 10
    sql = f"""
    WITH recent_per_symbol AS (
        SELECT k.symbol, k.date, k.amount, k.close,
               b.名称 AS name, b.细分行业 AS industry,
               ROW_NUMBER() OVER (PARTITION BY k.symbol ORDER BY k.date DESC) AS rn_global
        FROM kdata k
        JOIN stock_basic b ON k.symbol = b.代码
        WHERE k.date >= (SELECT MAX(date) FROM kdata) - INTERVAL '{lookback} days'
          AND b.名称 NOT LIKE '%ST%'
          AND b.上市日期 <= (SELECT MAX(date) FROM kdata) - INTERVAL '60 days'
    ),
    recent_dates AS (
        SELECT DISTINCT date FROM recent_per_symbol ORDER BY date DESC LIMIT {days}
    ),
    target_days AS (
        SELECT r.* FROM recent_per_symbol r
        JOIN recent_dates d ON r.date = d.date
    ),
    hist_pairs AS (
        SELECT t.symbol AS t_sym, t.date AS t_date,
               t.amount AS t_amount, t.close, t.name, t.industry,
               h.amount AS h_amount
        FROM target_days t
        JOIN recent_per_symbol h ON h.symbol = t.symbol
        WHERE h.rn_global BETWEEN (t.rn_global + 1) AND (t.rn_global + {window_days})
    ),
    heat_calc AS (
        SELECT t_sym AS symbol, t_date AS date, t_amount AS amount,
               close, name, industry,
               SUM(CASE WHEN h_amount <= t_amount THEN 1 ELSE 0 END) * 100.0
               / NULLIF(COUNT(*), 0) AS heat_pct
        FROM hist_pairs
        GROUP BY t_sym, t_date, t_amount, close, name, industry
    )
    SELECT symbol, date, heat_pct, amount AS amt, close, name, industry
    FROM heat_calc ORDER BY date DESC, symbol
    """
    df = con.execute(sql).df()
    if df.empty:
        return df
    df = df.sort_values(['symbol', 'date']).reset_index(drop=True)
    df['heat_prev'] = df.groupby('symbol')['heat_pct'].shift(1)
    df['ret_pct'] = df.groupby('symbol')['close'].pct_change() * 100
    df['signal'] = 'normal'
    df.loc[(df['heat_prev'] < cold_th) & (df['heat_pct'] >= hot_th), 'signal'] = 'heating'
    df.loc[(df['heat_prev'] >= hot_th) & (df['heat_pct'] < cold_th), 'signal'] = 'cooling'
    df.loc[(df['heat_prev'] >= hot_th) & (df['heat_pct'] >= hot_th), 'signal'] = 'staying'
    df = df.dropna(subset=['heat_prev'])
    return df


def append_heat_rotation_today(window_days=20, hot_th=80.0, cold_th=50.0):
    """
    把今天的轮动数据追加到 HEAT_PATH (幂等: dedup by date+symbol+window+hot+cold)
    Returns: (new_rows, today_str)
    """
    con = _open_con()
    today_str = str(con.execute("SELECT MAX(date) FROM kdata").fetchone()[0])
    # 取 days=2 是为了 heat_prev 有对照
    df_today = compute_heat_history(con, days=2, window_days=window_days,
                                     hot_th=hot_th, cold_th=cold_th)
    if df_today.empty:
        return 0, today_str
    max_date = pd.Timestamp(df_today['date'].max())
    df_today = df_today[df_today['date'] == max_date].copy()
    if df_today.empty:
        return 0, today_str
    df_today['window_days'] = window_days
    df_today['hot_th'] = hot_th
    df_today['cold_th'] = cold_th

    if os.path.exists(HEAT_PATH):
        existing = pd.read_parquet(HEAT_PATH)
        keys = ['date', 'symbol', 'window_days', 'hot_th', 'cold_th']
        existing_keys = existing[keys].apply(tuple, axis=1)
        new_keys = df_today[keys].apply(tuple, axis=1)
        dup_mask = new_keys.isin(existing_keys)
        new_rows = int((~dup_mask).sum())
        if new_rows == 0:
            return 0, today_str
        df_to_write = pd.concat([existing, df_today[~dup_mask]], ignore_index=True)
    else:
        df_to_write = df_today
        new_rows = len(df_today)

    df_to_write.to_parquet(HEAT_PATH, index=False)
    return new_rows, today_str