"""
A股数据可视化面板 (Streamlit)
- 个股 K线 / 成交量 / 资金 / 收益率分布
- ASI 年度排名 Top N
- 同业对比
- 行业强弱热力图

启动: streamlit run /home/hanshuang8902/stock/dashboard.py
默认: http://localhost:8501
"""
import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date, timedelta
import os

# ---------- 配置 ----------
# 数据源: Parquet 文件 (无锁, 持久, 原子替换)
# 2026-06-05 迁移: 从 DuckDB stock.db 改为 4 个独立 Parquet 文件
KDATA_PATH    = '/home/hanshuang8902/stock_data/kdata.parquet'
ASI_PATH      = '/home/hanshuang8902/stock_data/asi_yearly.parquet'
ASI_UP_PATH   = '/home/hanshuang8902/stock_data/asi_yearly_up.parquet'
BASIC_PATH    = '/home/hanshuang8902/stock_data/stock_basic.parquet'

# 中文字体 (Camoufox 缓存的 NotoSansSC) — plotly 自带字体回退机制
# 不需要 matplotlib（dashboard 全程用 plotly 画图）

st.set_page_config(
    page_title="A股数据面板",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------- 数据加载 ----------
# 2026-06-05: 改用 DuckDB in-memory 引擎直接读 Parquet 文件
# - 无 .db 文件 → 完全没有 DuckDB 锁问题
# - 每次新开 in-memory 连接, 加载 4 个 Parquet 为 VIEW, 立刻用完即弃
# - 性能与原 DuckDB 一样, 因为底层还是 DuckDB
# - writer 改 Parquet 时, dashboard 仍能读旧 fd 看到旧数据, 不报错

def get_con():
    """打开 in-memory DuckDB, 注册 4 个 Parquet 文件为 VIEW

    健壮性: 某个 Parquet 不存在时不崩, 跳过该 VIEW
    这样 dashboard 能在 kdata 未迁移前也启动 (asi/ranking 页面会报"表不存在")
    """
    con = duckdb.connect(':memory:')
    for view, path in [
        ('kdata',         KDATA_PATH),
        ('asi_yearly',    ASI_PATH),
        ('asi_yearly_up', ASI_UP_PATH),
        ('stock_basic',   BASIC_PATH),
    ]:
        if os.path.exists(path):
            con.execute(f"CREATE VIEW {view} AS SELECT * FROM read_parquet('{path}')")
    return con

def safe_query(sql, params=None, label=""):
    """执行 SQL (无需 lock 处理, Parquet 文件始终可读)"""
    con = get_con()
    return con.execute(sql, params or []).df()

@st.cache_data(ttl=600)
def load_stock_basic():
    return pd.read_parquet(BASIC_PATH)

@st.cache_data(ttl=300)
def load_kdata(symbol, start_date, end_date):
    """读单只股票K线"""
    con = get_con()
    df = con.execute("""
        SELECT date, open, high, low, close, volume, amount
        FROM kdata
        WHERE symbol = ? AND date BETWEEN ? AND ?
        ORDER BY date
    """, [symbol, start_date, end_date]).df()
    df['date'] = pd.to_datetime(df['date'])
    return df

@st.cache_data(ttl=300)
def load_kdata_with_asi(symbol, start_date, end_date, mode="v2"):
    """读单只股票K线 + 每日 ASI (成交额百分位排名 + 当日得分)
    mode: 'v2'=价格加权 (推荐), 'up'=仅上涨日, 'v0'=无加权
    """
    con = get_con()
    K = 3.0  # 与 asi_calculator_duckdb.py 保持一致

    if mode == "v2":
        up_filter = ""
        ret_expr = f"(1 + TANH(CASE WHEN k.open > 0 THEN (k.close - k.open) / k.open * 100 ELSE 0 END / {K}))"
    elif mode == "up":
        up_filter = "AND k.close > k.open"
        ret_expr = "1.0"
    else:  # v0
        up_filter = ""
        ret_expr = "1.0"

    # 扩展范围: RPS120 需要 120+22 ≈ 142 天的历史
    from datetime import timedelta
    ext_start = start_date - timedelta(days=125)
    df = con.execute(f"""
        WITH market_ranks AS (
            SELECT symbol, date, amount,
                   CAST(RANK() OVER (PARTITION BY date ORDER BY amount DESC) AS DOUBLE) AS amount_rank,
                   CAST(COUNT(*) OVER (PARTITION BY date) AS DOUBLE) AS total_stocks
            FROM kdata
            WHERE date BETWEEN ? AND ? AND amount > 0 {up_filter}
        ),
        kdata_range AS (
            SELECT symbol, date, open, high, low, close, volume, amount
            FROM kdata WHERE symbol = ? AND date BETWEEN ? AND ?
        ),
        -- RPS: 在全市场范围内对 N 日涨跌幅做 PERCENT_RANK
        -- 用 LAG 拿 N 天前 close, 一次性算 5 个 RPS
        all_returns AS (
            SELECT k.symbol, k.date,
                   k.close / NULLIF(LAG(k.close, 5)   OVER (PARTITION BY k.symbol ORDER BY k.date), 0) - 1 AS ret5,
                   k.close / NULLIF(LAG(k.close, 10)  OVER (PARTITION BY k.symbol ORDER BY k.date), 0) - 1 AS ret10,
                   k.close / NULLIF(LAG(k.close, 20)  OVER (PARTITION BY k.symbol ORDER BY k.date), 0) - 1 AS ret20,
                   k.close / NULLIF(LAG(k.close, 60)  OVER (PARTITION BY k.symbol ORDER BY k.date), 0) - 1 AS ret60,
                   k.close / NULLIF(LAG(k.close, 120) OVER (PARTITION BY k.symbol ORDER BY k.date), 0) - 1 AS ret120
            FROM kdata k
            WHERE k.date BETWEEN ? AND ?
        ),
        rps_calc AS (
            SELECT symbol, date,
                   100.0 * PERCENT_RANK() OVER (PARTITION BY date ORDER BY ret5)   AS rps5,
                   100.0 * PERCENT_RANK() OVER (PARTITION BY date ORDER BY ret10)  AS rps10,
                   100.0 * PERCENT_RANK() OVER (PARTITION BY date ORDER BY ret20)  AS rps20,
                   100.0 * PERCENT_RANK() OVER (PARTITION BY date ORDER BY ret60)  AS rps60,
                   100.0 * PERCENT_RANK() OVER (PARTITION BY date ORDER BY ret120) AS rps120
            FROM all_returns
            WHERE ret5 IS NOT NULL OR ret10 IS NOT NULL OR ret20 IS NOT NULL
               OR ret60 IS NOT NULL OR ret120 IS NOT NULL
        )
        SELECT
            k.date, k.open, k.high, k.low, k.close, k.volume, k.amount,
            m.amount_rank,
            m.total_stocks AS max_rank,
            CASE WHEN m.amount_rank IS NOT NULL AND m.total_stocks IS NOT NULL
                 THEN LN(m.total_stocks + 1 - m.amount_rank) / LN(m.total_stocks + 1) * 100 * {ret_expr}
                 ELSE NULL END AS asi_score,
            CASE WHEN m.amount_rank IS NOT NULL AND m.amount_rank <= 50  THEN 1 ELSE 0 END AS in_top50,
            CASE WHEN m.amount_rank IS NOT NULL AND m.amount_rank <= 100 THEN 1 ELSE 0 END AS in_top100,
            r.rps5, r.rps10, r.rps20, r.rps60, r.rps120
        FROM kdata_range k
        LEFT JOIN market_ranks m ON k.date = m.date AND m.symbol = k.symbol
        LEFT JOIN rps_calc r ON k.date = r.date AND k.symbol = r.symbol
        ORDER BY k.date
    """, [start_date, end_date, symbol, start_date, end_date, ext_start, end_date]).df()
    df['date'] = pd.to_datetime(df['date'])
    return df

@st.cache_data(ttl=600)
def load_symbol_asi_yearly(symbol, source='up'):
    """读单只股票各年 ASI 摘要
    source: 'up'=仅上涨日(asi_yearly_up), 'all'=全交易日(asi_yearly)
    注意: asi_best_rank/asi_avg_rank 仅 asi_yearly 有
    """
    con = get_con()
    table = 'asi_yearly_up' if source == 'up' else 'asi_yearly'
    # 看表实际有哪些列
    cols = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
    # 按需选择
    desired = ['year', 'asi_sum', 'asi_mean', 'asi_std', 'asi_trading_days',
               'asi_best_rank', 'asi_avg_rank', 'top50_days', 'top100_days',
               'asi_score_ratio', 'asi_yearly_rank']
    select_cols = [c for c in desired if c in cols]
    return con.execute(f"""
        SELECT {', '.join(select_cols)}
        FROM {table}
        WHERE symbol = ?
        ORDER BY year DESC
    """, [symbol]).df()

@st.cache_data(ttl=300)
def load_asi_top(year, top_n=50, source="up", listed_min_years=0):
    """从 asi_yearly (全交易日) 或 asi_yearly_up (仅上涨日) 读
    source: 'up' = 仅上涨日 (推荐) / 'all' = 全交易日
    listed_min_years: 上市至少 N 年 (默认 0 = 不限)
    """
    con = get_con()
    table = "asi_yearly_up" if source == "up" else "asi_yearly"
    # 上市满 N 年: basic.上市日期 + N 年 <= year 的最后一天 (12-31)
    cutoff = f"{year - listed_min_years}-12-31" if listed_min_years > 0 else None
    if cutoff:
        return con.execute(f"""
            SELECT t.* FROM {table} t
            JOIN stock_basic b ON t.symbol = b.代码
            WHERE t.year = ? AND b.上市日期 <= ?
            ORDER BY t.asi_yearly_rank
            LIMIT ?
        """, [year, cutoff, top_n]).df()
    return con.execute(f"""
        SELECT * FROM {table}
        WHERE year = ?
        ORDER BY asi_yearly_rank
        LIMIT ?
    """, [year, top_n]).df()

@st.cache_data(ttl=300)
def load_rps_top(end_date, periods=[5, 10, 20, 60, 120], top_n=50,
                 listed_min_years=0, min_ret=None):
    """RPS 排名 (相对价格强度)
    end_date: 截止日 (默认最新交易日)
    periods: RPS 周期列表 (5/10/20/60/120 日)
    top_n: 取前 N 名
    listed_min_years: 上市至少 N 年 (过滤新股)
    min_ret: 最低涨跌幅过滤 (None=不限)

    返回: 每个 symbol 在各周期的 RPS (0-100), 按 RPS_5 排名
    """
    con = get_con()
    # 用窗口函数 LAG 一次性取 5/10/20/60/120 天前的收盘价 (避免相关子查询)
    # 假设取最近 130 天的数据 (覆盖 120 周期 + buffer)
    periods_str = ','.join(str(p) for p in periods)
    max_p = max(periods)

    # 上市满 N 年: 上市日期 <= end_date - N 年
    if listed_min_years > 0:
        # 计算 cutoff: 截止日往前 N 年的同月同日
        from datetime import date, timedelta
        ed = date.fromisoformat(end_date)
        cutoff_date = ed.replace(year=ed.year - listed_min_years)
        listed_filter = f"AND sb.上市日期 <= '{cutoff_date}'"
    else:
        listed_filter = ""

    sql = f"""
        WITH recent AS (
            SELECT
                k.symbol,
                k.close,
                k.date,
                LAG(k.close, {periods_str.split(',')[0]}) OVER w AS close_first,
                LAG(k.close, {periods_str.split(',')[1] if len(periods) > 1 else periods_str.split(',')[0]})
                    OVER w AS close_second,
                LAG(k.close, {periods_str.split(',')[2] if len(periods) > 2 else periods_str.split(',')[-1]})
                    OVER w AS close_third,
                LAG(k.close, {periods_str.split(',')[3] if len(periods) > 3 else periods_str.split(',')[-1]})
                    OVER w AS close_fourth,
                LAG(k.close, {periods_str.split(',')[4] if len(periods) > 4 else periods_str.split(',')[-1]})
                    OVER w AS close_fifth
            FROM kdata k
            -- 日历日 = ceil(交易日 * 7/5) + buffer, 120 交易日 ≈ 170-185 天
            WHERE k.date > '{end_date}'::DATE - INTERVAL {int(max_p * 1.6) + 30} DAY
              AND k.date <= '{end_date}'
            WINDOW w AS (PARTITION BY k.symbol ORDER BY k.date)
        ),
        end_prices AS (
            SELECT * FROM recent WHERE date = '{end_date}'
        ),
        returns AS (
            SELECT
                symbol,
                close,
                (close - close_first)  / NULLIF(close_first, 0)  * 100 AS ret_0,
                (close - close_second) / NULLIF(close_second, 0) * 100 AS ret_1,
                (close - close_third)  / NULLIF(close_third, 0)  * 100 AS ret_2,
                (close - close_fourth) / NULLIF(close_fourth, 0) * 100 AS ret_3,
                (close - close_fifth)  / NULLIF(close_fifth, 0)  * 100 AS ret_4
            FROM end_prices
        ),
        ranked AS (
            SELECT
                r.symbol,
                r.close,
                ROUND(PERCENT_RANK() OVER (ORDER BY r.ret_0) * 100, 2) AS rps_0,
                ROUND(PERCENT_RANK() OVER (ORDER BY r.ret_1) * 100, 2) AS rps_1,
                ROUND(PERCENT_RANK() OVER (ORDER BY r.ret_2) * 100, 2) AS rps_2,
                ROUND(PERCENT_RANK() OVER (ORDER BY r.ret_3) * 100, 2) AS rps_3,
                ROUND(PERCENT_RANK() OVER (ORDER BY r.ret_4) * 100, 2) AS rps_4,
                r.ret_0, r.ret_1, r.ret_2, r.ret_3, r.ret_4
            FROM returns r
            JOIN stock_basic sb ON r.symbol = sb.代码
            WHERE r.ret_0 IS NOT NULL
              {listed_filter}
        )
        SELECT * FROM ranked
        ORDER BY rps_0 DESC
        LIMIT ?
    """
    df = con.execute(sql, [top_n]).df()
    # 重命名列为 period 实际值
    rename = {}
    for i, p in enumerate(periods):
        rename[f'ret_{i}'] = f'ret_{p}d'
        rename[f'rps_{i}'] = f'rps_{p}'
    return df.rename(columns=rename)

@st.cache_data(ttl=300)
def load_asi_top_live(year, top_n=50, up_only=False):
    """用 SQL 即时计算 ASI Top N (与 asi_calculator_duckdb.py 逻辑一致)
    up_only: True=仅上涨日参与排名和得分
    """
    con = get_con()
    up_filter = "AND close > open" if up_only else ""
    return con.execute(f"""
        WITH daily_scored AS (
            SELECT
                symbol,
                date,
                amount,
                close, open,
                CAST(RANK() OVER (PARTITION BY date ORDER BY amount DESC) AS DOUBLE) AS amount_rank,
                CAST(COUNT(*)   OVER (PARTITION BY date) AS DOUBLE)         AS max_rank
            FROM kdata
            WHERE year(date) = ?
              AND amount > 0
              {up_filter}
        ),
        daily_asi AS (
            SELECT
                symbol,
                CASE WHEN amount_rank <= max_rank
                     THEN LN(max_rank + 1 - amount_rank) / LN(max_rank + 1) * 100
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
                SUM(CASE WHEN in_top50  THEN 1 ELSE 0 END)   AS top50_days,
                SUM(CASE WHEN in_top100 THEN 1 ELSE 0 END)   AS top100_days
            FROM daily_asi
            GROUP BY symbol
        )
        SELECT
            symbol,
            asi_trading_days,
            asi_sum,
            asi_mean,
            asi_std,
            top50_days,
            top100_days,
            asi_sum / (asi_trading_days * 100.0) AS asi_score_ratio,
            RANK() OVER (ORDER BY asi_sum DESC)  AS asi_yearly_rank
        FROM yearly
        ORDER BY asi_yearly_rank
        LIMIT ?
    """, [year, top_n]).df()

@st.cache_data(ttl=300)
def load_industry_strength(date_from, date_to, top_n=20, up_only=True,
                           weight_by_return=True):
    """行业强度
    weight_by_return=False: 旧版，按 amount 百分位排名均值
    weight_by_return=True:  新版，强度 = pct_rank × return(%)  (上涨加分，下跌减分)
    up_only=True: 只统计上涨日
    """
    con = get_con()
    up_filter = "AND k.close > k.open" if up_only else ""

    if not weight_by_return:
        return con.execute(f"""
            WITH daily_ranks AS (
                SELECT
                    b.细分行业 AS industry,
                    k.date,
                    k.symbol,
                    k.amount,
                    PERCENT_RANK() OVER (PARTITION BY k.date ORDER BY k.amount) AS pct_rank
                FROM kdata k
                JOIN read_parquet(?) b ON k.symbol = b.代码
                WHERE k.date BETWEEN ? AND ? AND k.amount > 0 {up_filter}
            )
            SELECT
                industry,
                COUNT(DISTINCT symbol) AS stock_count,
                COUNT(*) AS sample_days,
                AVG(pct_rank) * 100 AS avg_strength,
                SUM(amount) / 1e8 AS total_amount_yi
            FROM daily_ranks
            GROUP BY industry
            HAVING stock_count >= 5
            ORDER BY avg_strength DESC
            LIMIT ?
        """, [BASIC_PATH, date_from, date_to, top_n]).df()

    # 新版: 强度 = pct_rank * 当日涨跌幅(%)  (上涨加分, 下跌扣分)
    # close ≈ open + 当日涨幅, 所以 (close - open) / open * 100 = 涨跌幅
    return con.execute(f"""
        WITH daily_ranks AS (
            SELECT
                b.细分行业 AS industry,
                k.date,
                k.symbol,
                k.amount,
                PERCENT_RANK() OVER (PARTITION BY k.date ORDER BY k.amount) AS pct_rank,
                CASE WHEN k.open > 0
                     THEN (k.close - k.open) / k.open * 100
                     ELSE 0 END AS ret_pct
            FROM kdata k
            JOIN read_parquet(?) b ON k.symbol = b.代码
            WHERE k.date BETWEEN ? AND ? AND k.amount > 0 {up_filter}
        )
        SELECT
            industry,
            COUNT(DISTINCT symbol) AS stock_count,
            COUNT(*) AS sample_days,
            AVG(pct_rank) AS avg_pct_rank,
            AVG(ret_pct)  AS avg_return_pct,
            SUM(pct_rank * ret_pct) / NULLIF(COUNT(*), 0) AS weighted_strength,
            SUM(amount) / 1e8 AS total_amount_yi
        FROM daily_ranks
        GROUP BY industry
        HAVING stock_count >= 5
        ORDER BY weighted_strength DESC
        LIMIT ?
    """, [BASIC_PATH, date_from, date_to, top_n]).df()

@st.cache_data(ttl=300)
def load_peer_compare(symbols, start_date, end_date):
    """同业对比 - 多只股票归一化净值"""
    con = get_con()
    placeholders = ','.join(['?'] * len(symbols))
    df = con.execute(f"""
        SELECT symbol, date, close
        FROM kdata
        WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ?
        ORDER BY date
    """, symbols + [start_date, end_date]).df()
    df['date'] = pd.to_datetime(df['date'])
    # 归一化: 起始日=1.0
    pivoted = df.pivot(index='date', columns='symbol', values='close')
    normalized = pivoted / pivoted.iloc[0]
    return normalized.reset_index().melt(id_vars='date', var_name='symbol', value_name='nav')

# ---------- 侧栏 ----------
with st.sidebar:
    st.title("📈 A股数据面板")
    st.caption(f"数据源: {os.path.basename(KDATA_PATH)}")
    st.caption(f"今日: {date.today()}")

    # 页面切换: st.radio 直接返回用户选择, 不绕 session_state
    PAGES = ["🏠 总览", "📊 个股K线", "🏆 ASI 排名", "📈 RPS 排名", "🎯 低吸观察池", "🏭 行业强度", "⚖️ 同业对比"]

    # 跳转按钮: 通过 st.query_params['page'] 改 page, 这里读出后用
    # (跳转按钮在 ASI 排名/RPS 排名页里, 调 st.query_params.update + st.rerun)
    qp = st.query_params
    qp_page = qp.get('page', None)
    if qp_page in PAGES:
        st.session_state._current_page = qp_page
        # 用完即清, 避免 radio 重渲染时仍被 query_params 抢占
        del st.query_params['page']
    if '_current_page' not in st.session_state or st.session_state._current_page not in PAGES:
        st.session_state._current_page = PAGES[0]

    page = st.radio(
        "📑 功能页面",
        PAGES,
        index=PAGES.index(st.session_state._current_page),
        key='nav_radio',  # 关键: 给 radio 自己的 widget state, 不需要手动同步
        label_visibility="collapsed",
    )
    st.session_state._current_page = page

    st.divider()
    if st.button("🔄 刷新缓存"):
        st.cache_data.clear()
        st.rerun()

# ---------- 主页 ----------
# 不用顶层 con, 每个查询新开 (in-memory DuckDB 启动 ~50ms, 可接受)
basic = load_stock_basic()
basic['代码'] = basic['代码'].astype(str).str.zfill(6)

# 数据概览 (用一次性 in-memory 连接)
_overview_con = get_con()
total_stocks = _overview_con.execute("SELECT COUNT(DISTINCT symbol) FROM kdata").fetchone()[0]
date_range = _overview_con.execute("SELECT MIN(date), MAX(date) FROM kdata").fetchone()
total_rows = _overview_con.execute("SELECT COUNT(*) FROM kdata").fetchone()[0]
_overview_con.close()

st.sidebar.info(
    f"📦 {total_stocks:,} 只股\n\n"
    f"📅 {date_range[0]} ~ {date_range[1]}\n\n"
    f"💾 {total_rows:,} 行"
)

# ============================================================
# 页面 1: 总览
# ============================================================
if page == "🏠 总览":
    st.header("🏠 数据总览")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("股票数", f"{total_stocks:,}")
    c2.metric("数据起始", str(date_range[0]))
    c3.metric("最新交易日", str(date_range[1]))
    c4.metric("总行数", f"{total_rows:,}")

    st.divider()

    # 最近 30 天每日全市场成交额
    st.subheader("📈 最近 30 个交易日全市场成交额")
    df_amt = safe_query("""
        SELECT date, SUM(amount) / 1e8 AS total_yi, COUNT(DISTINCT symbol) AS stocks
        FROM kdata
        WHERE amount > 0
        GROUP BY date
        ORDER BY date DESC
        LIMIT 30
    """)
    df_amt['date'] = pd.to_datetime(df_amt['date'])
    df_amt = df_amt.sort_values('date')

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=df_amt['date'], y=df_amt['total_yi'], name="成交额(亿元)",
               marker_color='steelblue', opacity=0.7),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=df_amt['date'], y=df_amt['stocks'], name="交易股数",
                   mode='lines+markers', line=dict(color='orange', width=2)),
        secondary_y=True,
    )
    fig.update_layout(title="市场活跃度", hovermode='x unified', height=400)
    fig.update_yaxes(title_text="成交额(亿)", secondary_y=False)
    fig.update_yaxes(title_text="股数", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # 行业分布
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🏭 行业股票数 Top 15")
        df_ind = basic['细分行业'].value_counts().head(15).reset_index()
        df_ind.columns = ['行业', '股票数']
        fig = go.Figure(go.Bar(
            x=df_ind['股票数'], y=df_ind['行业'], orientation='h',
            marker_color='teal', text=df_ind['股票数'], textposition='outside'
        ))
        fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("🌍 地区分布 Top 15")
        df_reg = basic['地区'].value_counts().head(15).reset_index()
        df_reg.columns = ['地区', '股票数']
        fig = go.Figure(go.Pie(
            labels=df_reg['地区'], values=df_reg['股票数'],
            hole=0.4, textinfo='label+percent'
        ))
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

# ============================================================
# 页面 2: 个股K线
# ============================================================
elif page == "📊 个股K线":
    st.header("📊 个股K线分析")

    # 返回按钮 (从同业对比/行业强度跳来的, 才能返回)
    return_to = st.session_state.get('return_to', None)
    if return_to:
        if st.button(f"↩️ 返回 {return_to}", key='kline_back'):
            st.session_state._current_page = return_to
            st.session_state.pop('return_to', None)
            st.rerun()

    # 读 session_state.symbol (跳转来的)
    qp_symbol = st.session_state.get('symbol', None)

    c1, c2 = st.columns([1, 3])
    with c1:
        # 选股票
        search = st.text_input("🔍 搜索股票 (代码/名称)", qp_symbol or "", key='kline_search')
        if search:
            mask = basic['代码'].str.contains(search) | basic['名称'].str.contains(search, na=False)
            options = basic[mask].head(50)
        else:
            options = basic.head(20)

        # 如果 session_state.symbol 命中但不在 options (例如长尾股), 把它加到 options 顶部
        if qp_symbol and (options['代码'] == qp_symbol).sum() == 0:
            extra = basic[basic['代码'] == qp_symbol]
            if len(extra) > 0:
                options = pd.concat([extra, options], ignore_index=True)

        options['label'] = options['代码'] + ' ' + options['名称']
        # 优先选中 session_state 命中的股
        labels = options['label'].tolist()
        default_idx = 0
        if qp_symbol:
            for i, lab in enumerate(labels):
                if lab.startswith(qp_symbol + ' '):
                    default_idx = i
                    break
        selected = st.selectbox("选择股票", labels, index=default_idx)
        symbol = selected.split(' ')[0]
        # 同步当前选择的 symbol 到 session_state (供"返回同业对比"用)
        st.session_state.symbol = symbol

    with c2:
        info = basic[basic['代码'] == symbol].iloc[0]
        st.markdown(f"### {info['名称']} ({symbol})")
        st.caption(f"行业: {info['细分行业']} | 地区: {info['地区']} | 上市: {info['上市日期']}")

    # ASI 口径
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        start = st.date_input("起始日", date(2025, 1, 1), min_value=date(1990, 1, 1))
    with c2:
        con_local = get_con()
        db_max = con_local.execute("SELECT MAX(date) FROM kdata").fetchone()[0]
        end = st.date_input("结束日", db_max, min_value=date(1990, 1, 1), max_value=db_max)
    with c3:
        show_vol = st.checkbox("显示成交量", True)
    with c4:
        show_amount = st.checkbox("显示成交额", True)
    with c5:
        asi_mode = st.radio("ASI口径", ["v2 加权", "v1 仅上涨日"], index=0, key='kline_asi_mode',
                            horizontal=True, help="v2 加权推荐, v1 旧版仅上涨日")
        asi_mode_key = "v2" if "v2" in asi_mode else "up"

    df = load_kdata_with_asi(symbol, start, end, asi_mode_key)
    if df.empty:
        st.warning("该日期范围无数据")
        st.stop()

    # K线图 (5 行: K线 / 成交量 / 成交额 / ASI / RPS)
    panels = [('k', 0.40)]
    if show_vol: panels.append(('vol', 0.15))
    if show_amount: panels.append(('amt', 0.15))
    panels.append(('asi', 0.15))  # ASI
    panels.append(('rps', 0.15))  # RPS
    row_heights = [h for _, h in panels]

    fig = make_subplots(
        rows=len(panels), cols=1, shared_xaxes=True,
        vertical_spacing=0.03, row_heights=row_heights
    )

    fig.add_trace(go.Candlestick(
        x=df['date'], open=df['open'], high=df['high'], low=df['low'], close=df['close'],
        name='K线', increasing_line_color='red', decreasing_line_color='green',
    ), row=1, col=1)

    cur_row = 2
    if show_vol:
        fig.add_trace(go.Bar(
            x=df['date'], y=df['volume'],
            name='成交量', marker_color='steelblue', opacity=0.6
        ), row=cur_row, col=1)
        cur_row += 1
    if show_amount:
        fig.add_trace(go.Bar(
            x=df['date'], y=df['amount'] / 1e8,
            name='成交额(亿)', marker_color='orange', opacity=0.6
        ), row=cur_row, col=1)
        cur_row += 1

    # ASI 得分 (有数据的点连线, NaN 跳过)
    df_asi = df.dropna(subset=['asi_score'])
    if len(df_asi) > 0:
        # ASI 得分 (面积)
        fig.add_trace(go.Scatter(
            x=df_asi['date'], y=df_asi['asi_score'],
            mode='lines', name='ASI 得分',
            line=dict(color='purple', width=1.5),
            fill='tozeroy', fillcolor='rgba(128,0,128,0.15)',
        ), row=cur_row, col=1)
        # 标记 Top50 日 (红点)
        top50 = df_asi[df_asi['in_top50'] == 1]
        if len(top50):
            fig.add_trace(go.Scatter(
                x=top50['date'], y=top50['asi_score'],
                mode='markers', name='Top50 日',
                marker=dict(color='red', size=7, symbol='star'),
            ), row=cur_row, col=1)
    cur_row += 1

    # RPS 多线图 (RPS5/10/20/60/120)
    rps_cols = ['rps5', 'rps10', 'rps20', 'rps60', 'rps120']
    rps_labels = ['RPS5', 'RPS10', 'RPS20', 'RPS60', 'RPS120']
    rps_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    rps_present = [c for c in rps_cols if c in df.columns and df[c].notna().any()]
    for i, col in enumerate(rps_present):
        df_rps = df.dropna(subset=[col])
        if len(df_rps) > 0:
            fig.add_trace(go.Scatter(
                x=df_rps['date'], y=df_rps[col],
                mode='lines', name=rps_labels[rps_cols.index(col)],
                line=dict(color=rps_colors[rps_cols.index(col)], width=1.2),
            ), row=cur_row, col=1)
    # RPS 80 阈值线 (强股分界)
    fig.add_hline(y=80, line_dash='dash', line_color='red', opacity=0.5, row=cur_row, col=1)

    fig.update_layout(
        title=f"{info['名称']} K线 + ASI + RPS",
        xaxis_rangeslider_visible=False, height=900,
        hovermode='x unified', legend=dict(orientation='h', y=1.02)
    )
    fig.update_yaxes(title_text="价格", row=1, col=1)
    if show_vol: fig.update_yaxes(title_text="量", row=2, col=1)
    if show_amount: fig.update_yaxes(title_text="额(亿)", row=show_vol+2, col=1)
    fig.update_yaxes(title_text="ASI(0-100)", row=cur_row-1, col=1)
    fig.update_yaxes(title_text="RPS(0-100)", row=cur_row, col=1)
    st.plotly_chart(fig, use_container_width=True)

    # 区间 ASI 摘要
    st.subheader("🏆 ASI 区间摘要")
    df_asi_valid = df.dropna(subset=['asi_score'])
    if len(df_asi_valid) == 0:
        st.info("区间内无 ASI 数据（可能 amount 全为 0）")
    else:
        # 区间内每日累计
        days_with_data = len(df_asi_valid)
        avg_daily = df_asi_valid['asi_score'].mean()
        best_day = df_asi_valid['asi_score'].max()
        best_day_date = df_asi_valid.loc[df_asi_valid['asi_score'].idxmax(), 'date']
        best_rank = df_asi_valid['amount_rank'].min()
        best_rank_date = df_asi_valid.loc[df_asi_valid['amount_rank'].idxmin(), 'date']
        n_top50 = int(df_asi_valid['in_top50'].sum())
        n_top100 = int(df_asi_valid['in_top100'].sum())

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("区间日均ASI", f"{avg_daily:.1f}")
        c2.metric("区间累计ASI", f"{df_asi_valid['asi_score'].sum():.0f}")
        c3.metric("最佳日ASI", f"{best_day:.1f}", help=str(best_day_date.date()))
        c4.metric("最佳排名", f"#{int(best_rank)}", help=str(best_rank_date.date()))
        c5.metric("Top50 天数", f"{n_top50}/{days_with_data}")
        c6.metric("Top100 天数", f"{n_top100}/{days_with_data}")

    # 区间 RPS 摘要
    st.subheader("📈 RPS 区间摘要 (最新一日)")
    rps_summary = []
    for col, label in zip(rps_cols, rps_labels):
        if col in df.columns and df[col].notna().any():
            last_val = df[col].dropna().iloc[-1] if df[col].notna().any() else None
            max_val = df[col].max()
            rps_summary.append((label, last_val, max_val))
    if rps_summary:
        cols_rps = st.columns(len(rps_summary))
        for i, (label, last_v, max_v) in enumerate(rps_summary):
            color = "🟢" if last_v and last_v >= 80 else ("🟡" if last_v and last_v >= 50 else "🔴")
            cols_rps[i].metric(f"{color} {label}", f"{last_v:.1f}" if last_v else "N/A",
                               help=f"区间最高 {max_v:.1f}")

    # 年度 ASI 摘要
    st.subheader("📅 年度 ASI 摘要")
    c1, c2 = st.columns([1, 3])
    with c1:
        asi_source = st.radio("数据源", ["v2 加权 (推荐)", "v1 仅上涨日"], index=0,
                              key='kline_asi_source', horizontal=True)
    source_key = 'up' if "仅上涨" in asi_source else 'all'
    yearly = load_symbol_asi_yearly(symbol, source_key)
    if len(yearly) == 0:
        st.info("该股票无年度 ASI 数据")
    else:
        # 展示用列 (与查询列动态匹配, 避免列数对不上)
        col_rename = {
            'year': '年份', 'asi_sum': 'ASI总分', 'asi_mean': 'ASI日均',
            'asi_std': 'ASI标准差', 'asi_trading_days': '交易日数',
            'asi_best_rank': '最佳日排名', 'asi_avg_rank': '平均日排名',
            'top50_days': 'Top50天数', 'top100_days': 'Top100天数',
            'asi_score_ratio': 'ASI分数比', 'asi_yearly_rank': '年度排名',
        }
        display = yearly.rename(columns=col_rename)
        # 浮点列四舍五入
        for c in display.columns:
            if display[c].dtype == 'float64':
                display[c] = display[c].round(2)
        st.dataframe(display, use_container_width=True, height=400)

    # 统计指标
    st.subheader("📊 区间统计")
    df_recent = df.copy()
    df_recent['returns'] = df_recent['close'].pct_change()
    total_ret = (df_recent['close'].iloc[-1] / df_recent['close'].iloc[0] - 1) * 100

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("区间收益", f"{total_ret:.2f}%")
    c2.metric("最高", f"{df['high'].max():.2f}")
    c3.metric("最低", f"{df['low'].min():.2f}")
    c4.metric("日均成交额(亿)", f"{df['amount'].mean()/1e8:.2f}")
    c5.metric("波动率(年化)", f"{df_recent['returns'].std() * (252**0.5) * 100:.2f}%")

    # 收益率分布
    st.subheader("📈 收益率分布")
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=df_recent['returns'].dropna() * 100,
        nbinsx=50, marker_color='steelblue', opacity=0.7,
        name='日收益率'
    ))
    fig.update_layout(xaxis_title="日收益率(%)", yaxis_title="频次", height=350)
    st.plotly_chart(fig, use_container_width=True)

# ============================================================
# 页面 3: ASI 排名
# ============================================================
elif page == "🏆 ASI 排名":
    st.header("🏆 ASI 年度排名")

    asi = pd.read_parquet(ASI_PATH)
    years = sorted(asi['year'].unique().tolist(), reverse=True)

    c1, c2, c3, c4 = st.columns([1, 1, 2, 1])
    with c1:
        year = st.selectbox("年份", years, index=0)
    with c2:
        top_n = st.slider("Top N", 10, 100, 50)
    with c3:
        source = st.radio("口径", ["v2 价格加权 (推荐)", "v1 仅上涨日"], index=0, horizontal=True,
                          help="v2: 基础排名 × 涨跌幅加权; v1: 仅上涨日排名")
    with c4:
        listed_years = st.selectbox("上市满 (年)", [0, 1, 2, 3, 5], index=1,
                                    key='asi_listed_years',
                                    help="0=不限, 1=默认排除上市 < 1 年的新股")

    if "v2" in source:
        df = load_asi_top(year, top_n, "all", listed_min_years=listed_years)
        table_name = "asi_yearly (v2 加权)"
        mode_label = "v2 价格加权 (推荐)"
    else:  # v1
        df = load_asi_top(year, top_n, "up", listed_min_years=listed_years)
        table_name = "asi_yearly_up (v1 仅上涨日)"
        mode_label = "v1 仅上涨日"

    st.caption(f"口径: **{mode_label}** | 表: {table_name} | 上市满 {listed_years} 年")

    # merge 名称
    df = df.merge(basic[['代码', '名称', '细分行业']], left_on='symbol', right_on='代码', how='left')

    st.subheader(f"{year} 年 ASI Top {top_n}")

    # 显示表格
    # 注意: asi_best_rank / asi_avg_rank 仅 asi_yearly 表有，asi_yearly_up 和 load_asi_top_live 没有
    # 用 [c for c in cols if c in df.columns] 防御性选择
    desired_cols = ['asi_yearly_rank', 'symbol', '名称', '细分行业',
                    'asi_sum', 'asi_mean', 'asi_best_rank', 'asi_avg_rank',
                    'top50_days', 'top100_days', 'asi_score_ratio']
    available_cols = [c for c in desired_cols if c in df.columns or c in {'名称', '细分行业'}]
    display = df[[c for c in desired_cols if c in df.columns]].copy()
    rename_map = {
        'asi_yearly_rank': '排名', 'symbol': '代码', '名称': '名称', '细分行业': '行业',
        'asi_sum': 'ASI总分', 'asi_mean': 'ASI日均',
        'asi_best_rank': '最佳日排名', 'asi_avg_rank': '平均日排名',
        'top50_days': 'Top50天数', 'top100_days': 'Top100天数',
        'asi_score_ratio': 'ASI分数比',
    }
    display = display.rename(columns=rename_map)
    st.dataframe(display, use_container_width=True, height=500)

    # 个股跳转
    st.subheader("🔍 跳转到个股详情")
    c1, c2, c3 = st.columns([3, 1, 1])
    with c1:
        # 用 selectbox 让用户选要查看的股
        sel = st.selectbox(
            "从表中选一只股查看 K线 + ASI + RPS",
            options=df['symbol'].tolist(),
            format_func=lambda s: f"{s} {df[df['symbol']==s]['名称'].iloc[0]}",
            key='asi_jump_select'
        )
    with c2:
        st.write("")  # 间距
        if st.button("📊 查看个股详情", use_container_width=True):
            st.query_params.update(page="📊 个股K线", symbol=sel)
            st.rerun()
    with c3:
        st.write("")
        if st.button("🏭 查看个股所属行业强度", use_container_width=True):
            st.query_params.update(page="🏭 行业强度")
            st.rerun()

    # 可视化
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("ASI 总分 Top 20")
        fig = go.Figure(go.Bar(
            x=df.head(20)['asi_sum'],
            y=df.head(20)['名称'] + ' (' + df.head(20)['symbol'] + ')',
            orientation='h', marker_color='steelblue',
            text=df.head(20)['asi_sum'].round(1), textposition='outside'
        ))
        fig.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("行业分布")
        ind_dist = df['细分行业'].value_counts().head(15).reset_index()
        ind_dist.columns = ['行业', '股票数']
        fig = go.Figure(go.Pie(
            labels=ind_dist['行业'], values=ind_dist['股票数'],
            hole=0.4, textinfo='label+percent'
        ))
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)

# ============================================================
# 页面 4: RPS 排名
# ============================================================
elif page == "📈 RPS 排名":
    st.header("📈 RPS 相对价格强度排名")
    st.caption("RPS = Relative Price Strength，按涨跌幅百分位排名（0-100）。"
               "多周期交叉验证：5/10 日是短期动量，60/120 日是中长期强度。")

    con = get_con()
    db_max = con.execute("SELECT MAX(date) FROM kdata").fetchone()[0]

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
    with c1:
        end_date = st.date_input("截止日", db_max,
                                 min_value=date(2025, 1, 1), max_value=db_max)
    with c2:
        top_n = st.slider("Top N", 10, 100, 50)
    with c3:
        # 周期多选
        period_choice = st.selectbox("周期组合", [
            "标准 5/10/20/60/120",
            "短期 5/10/20",
            "长期 60/120/250",
        ], index=0)
        periods_map = {
            "标准 5/10/20/60/120": [5, 10, 20, 60, 120],
            "短期 5/10/20": [5, 10, 20],
            "长期 60/120/250": [60, 120, 250],
        }
        periods = periods_map[period_choice]
    with c4:
        listed_years = st.selectbox("上市满 (年)", [0, 1, 2, 3, 5], index=1,
                                    help="0=不限, 1=默认排除上市 < 1 年的新股")

    df = load_rps_top(str(end_date), periods=periods, top_n=top_n,
                      listed_min_years=listed_years)

    if df.empty:
        st.warning("无数据")
        st.stop()

    # merge 名称
    df = df.merge(basic[['代码', '名称', '细分行业']], left_on='symbol', right_on='代码', how='left')

    # 显示列
    rps_cols = [f'rps_{p}' for p in periods]
    ret_cols = [f'ret_{p}d' for p in periods]
    display = df[['symbol', '名称', '细分行业', 'close'] + rps_cols + ret_cols].copy()
    display.columns = (['代码', '名称', '行业', '现价'] +
                      [f'RPS_{p}日' for p in periods] +
                      [f'涨跌幅_{p}日(%)' for p in periods])
    # 排序按主周期 (periods[0])
    display = display.sort_values(f'RPS_{periods[0]}日', ascending=False).reset_index(drop=True)
    display.index = display.index + 1
    st.caption(f"截止: {end_date} | 上市满 {listed_years} 年 | 周期: {periods}")
    st.dataframe(display, use_container_width=True, height=500)

    # 跳转入口
    st.subheader("🔍 跳转到个股详情")
    c1, c2 = st.columns([3, 1])
    with c1:
        rps_jump = st.selectbox("选择要查看的股", display['代码'].tolist(),
                                format_func=lambda s: f"{s} {display[display['代码']==s]['名称'].iloc[0]}",
                                key='rps_jump')
    with c2:
        st.write("")
        if st.button("📊 查看该股 K线 + ASI + RPS", key='rps_jump_btn', use_container_width=True):
            st.query_params.update(page="📊 个股K线", symbol=rps_jump)
            st.rerun()

    # 多周期 RPS 分布散点图 (5 vs 60)
    if len(periods) >= 2 and 5 in periods and 60 in periods:
        st.subheader("短期 vs 中长期 RPS 散点")
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['rps_5'], y=df['rps_60'],
            mode='markers+text',
            text=df['名称'] + '<br>' + df['symbol'],
            textposition='top center',
            textfont=dict(size=9),
            marker=dict(
                size=df['close'] / df['close'].max() * 30 + 8,
                color=df['rps_120'] if 120 in periods else df['rps_60'],
                colorscale='RdYlGn', cmin=0, cmax=100,
                showscale=True, colorbar=dict(title="RPS_120"),
                line=dict(width=0.5, color='white')
            ),
            name='',
            hovertemplate='<b>%{text}</b><br>RPS_5=%{x}<br>RPS_60=%{y}<extra></extra>'
        ))
        # 象限分割线
        for v in [50]:
            fig.add_hline(y=v, line_dash="dash", line_color="gray", opacity=0.5)
            fig.add_vline(x=v, line_dash="dash", line_color="gray", opacity=0.5)
        # 象限标签
        fig.add_annotation(x=75, y=75, text="双强", showarrow=False,
                          font=dict(size=14, color="green"), opacity=0.6)
        fig.add_annotation(x=25, y=25, text="双弱", showarrow=False,
                          font=dict(size=14, color="red"), opacity=0.6)
        fig.add_annotation(x=75, y=25, text="短期反弹", showarrow=False,
                          font=dict(size=12, color="orange"), opacity=0.6)
        fig.add_annotation(x=25, y=75, text="调整中", showarrow=False,
                          font=dict(size=12, color="blue"), opacity=0.6)
        fig.update_layout(
            xaxis_title="RPS_5 (短期强度)", yaxis_title="RPS_60 (中长期强度)",
            height=600, xaxis=dict(range=[0, 100]), yaxis=dict(range=[0, 100]),
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)

    # RPS Top 20 横向柱图
    st.subheader(f"RPS_{periods[0]}日 Top 20")
    fig = go.Figure(go.Bar(
        x=display.head(20)[f'RPS_{periods[0]}日'],
        y=display.head(20)['名称'] + ' (' + display.head(20)['代码'] + ')',
        orientation='h',
        marker_color=display.head(20)[f'RPS_{periods[0]}日'],
        marker_colorscale='RdYlGn', marker_cmin=80, marker_cmax=100,
        text=display.head(20)[f'RPS_{periods[0]}日'].astype(str),
        textposition='inside',
    ))
    fig.update_layout(height=500, xaxis=dict(range=[80, 100]),
                      yaxis=dict(autorange='reversed'))
    st.plotly_chart(fig, use_container_width=True)

    # 提示
    st.info("💡 **多周期交叉验证**："
            "RPS_5 高 + RPS_60 高 = 真强势；"
            "RPS_5 高 + RPS_60 低 = 短期反弹长期下跌（追高风险）；"
            "上市满 1 年过滤避免新股剧烈波动干扰。")

# ============================================================
# 页面 5: 行业强度
# ============================================================
elif page == "🏭 行业强度":
    st.header("🏭 行业强度")

    # 数据最新日（避免空数据）
    con_local = get_con()
    db_max_date = con_local.execute("SELECT MAX(date) FROM kdata").fetchone()[0]
    db_min_date = con_local.execute("SELECT MIN(date) FROM kdata").fetchone()[0]

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        d_from = st.date_input("起始日", date(2026, 1, 1),
                               min_value=db_min_date, max_value=db_max_date)
    with c2:
        d_to = st.date_input("结束日", db_max_date,
                             min_value=db_min_date, max_value=db_max_date)
    with c3:
        top_n = st.slider("行业数", 5, 50, 20)
    with c4:
        up_only = st.checkbox("仅上涨日", value=True,
                              help="勾选=只看 close>open 的日子，跳过下跌高成交（恐慌出货噪音）")
    with c5:
        weighted = st.checkbox("涨跌幅加权", value=True,
                              help="勾选=强度=pct_rank × 涨跌幅(%)，下跌日扣分；不勾选=纯成交活跃度")

    if d_to > db_max_date:
        st.warning(f"⚠️ 数据最新日是 {db_max_date}，{d_to} 之后无数据")
    if d_from > d_to:
        st.warning("起始日不能晚于结束日")
        st.stop()

    df = load_industry_strength(d_from, d_to, top_n, up_only, weighted)
    if df.empty:
        st.warning("该日期范围无数据")
        st.stop()

    mode_label = "上涨日" if up_only else "全交易日"
    algo_label = "涨跌幅加权" if weighted else "纯成交活跃度"
    strength_col = 'weighted_strength' if weighted else 'avg_strength'
    st.caption(f"模式: **{mode_label}** | 算法: **{algo_label}**")

    c1, c2 = st.columns([2, 1])
    with c1:
        st.subheader(f"行业强度排名 ({d_from} ~ {d_to})")
        # weighted_strength 范围 -100~+100 (近似), avg_strength 0-100
        # 颜色: 5 档 (深绿/浅绿/黄/橙/红)
        def strength_color(v, weighted):
            if weighted:
                if v >= 30: return '#1a9850'
                if v >= 10: return '#66bd63'
                if v >= 0:  return '#fee08b'
                if v >= -10: return '#fdae61'
                return '#d73027'
            else:
                if v >= 90: return '#1a9850'
                if v >= 75: return '#66bd63'
                if v >= 60: return '#fee08b'
                if v >= 40: return '#fdae61'
                return '#d73027'
        bar_colors = [strength_color(v, weighted) for v in df[strength_col]]
        fig = go.Figure(go.Bar(
            x=df[strength_col],
            y=df['industry'],
            orientation='h',
            marker=dict(color=bar_colors, line=dict(color='rgba(0,0,0,0.2)', width=0.5)),
            text=df[strength_col].round(2),
            textposition='outside'
        ))
        xaxis_title = "加权强度 (pct_rank × 涨跌幅%)" if weighted else "平均强度 (0-100)"
        fig.update_layout(
            xaxis_title=xaxis_title, height=600,
            yaxis={'categoryorder': 'total ascending'}
        )
        fig.add_vline(x=0, line_dash="dash", line_color="gray", opacity=0.5)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("明细")
        display = df.copy()
        if weighted:
            display.columns = ['行业', '股票数', '样本日数', '平均pct_rank',
                               '平均涨幅%', '加权强度', '总成交额(亿)']
            display['平均pct_rank'] = display['平均pct_rank'].round(3)
            display['平均涨幅%'] = display['平均涨幅%'].round(2)
        else:
            display.columns = ['行业', '股票数', '样本日数', '平均强度', '总成交额(亿)']
            display['平均强度'] = display['平均强度'].round(2)
        display['加权强度' if weighted else '总成交额(亿)'] = (
            display['加权强度' if weighted else '总成交额(亿)'].round(2)
        )
        st.dataframe(display, use_container_width=True, height=600)

    # 跳转: 查看该行业的个股
    st.subheader("🔍 跳转到同业对比")
    c1, c2, c3 = st.columns([3, 1, 1])
    with c1:
        # 用 '股票数' 列 (weighted 时叫 stock_count, 不叫 n)
        if 'stock_count' in df.columns:
            n_col = 'stock_count'
        else:
            n_col = [c for c in df.columns if 'n' in c.lower() or 'count' in c.lower()][0]
        ind_jump = st.selectbox("选一个行业查看其个股", df['industry'].tolist(),
                                format_func=lambda s: f"{s} (股票数 {df[df['industry']==s][n_col].iloc[0]})",
                                key='ind_jump')
    with c2:
        st.write("")
        if st.button("⚖️ 查看该行业个股", key='ind_jump_btn', use_container_width=True):
            # 通过 session_state 传行业, 同业对比页读这个
            st.session_state['peer_industry_preset'] = ind_jump
            st.session_state._current_page = "⚖️ 同业对比"
            st.rerun()
    with c3:
        st.write("")
        # 选该行业第一只股, 直接跳到个股
        first_sym = basic[basic['细分行业'] == ind_jump]['代码'].iloc[0] if len(basic[basic['细分行业'] == ind_jump]) else None
        if first_sym and st.button("📊 看代表股 K线", key='ind_jump_rep', use_container_width=True):
            st.session_state._current_page = "📊 个股K线"
            st.session_state.symbol = first_sym
            st.session_state.return_to = "🏭 行业强度"
            st.rerun()

# ============================================================
# 页面 5: 低吸观察池 (长期强势 + 高关注 + 阶段性低位)
# ============================================================
elif page == "🎯 低吸观察池":
    st.header("🎯 低吸观察池")
    st.caption("筛选逻辑: 最近 2 年连续强势 (2024+2025 都 ASI 年度排名 p90+) + 当年高关注 (top100≥25) + 价格阶段性回调")

    @st.cache_data(ttl=3600)
    def compute_dip_pool():
        """v2 低吸观察池: 5 个切片, 每次切都用同一基础数据 (asi + kdata + basic)
        返回 dict: slice_name -> DataFrame
        """
        KDATA = '/home/hanshuang8902/stock_data/kdata.parquet'
        ASI   = '/home/hanshuang8902/stock_data/asi_yearly.parquet'

        con = duckdb.connect(':memory:')
        con.execute(f"CREATE VIEW kdata AS SELECT * FROM read_parquet('{KDATA}')")
        con.execute(f"CREATE VIEW asi    AS SELECT * FROM read_parquet('{ASI}')")

        # 1) 最近 2 年连续强势 (2024 + 2025 都 > 当年 p90)
        # 目的: 排除"以前牛但近年已不牛"的票, 避免不同市场阶段的票混淆
        strong_df = con.execute("""
            WITH p AS (SELECT year, QUANTILE_CONT(asi_sum, 0.90) AS p90 FROM asi GROUP BY year),
                 a AS (
                    SELECT a.symbol, a.year,
                           CASE WHEN a.asi_sum > p.p90 THEN 1 ELSE 0 END AS above
                    FROM asi a JOIN p ON a.year = p.year
                    WHERE a.year IN (2024, 2025)
                 )
            SELECT symbol, SUM(above) AS recent_strong_years
            FROM a GROUP BY symbol HAVING SUM(above) = 2
        """).df()
        strong_set = set(strong_df['symbol'].tolist())

        # 2) 基础数据
        last_date = con.execute("SELECT MAX(date) FROM kdata").fetchone()[0]
        w60_start = last_date - timedelta(days=80)
        w20_start = last_date - timedelta(days=30)
        w250_start = last_date - timedelta(days=400)

        asi_2026 = con.execute("SELECT symbol, asi_sum, asi_avg_rank, top50_days, top100_days FROM asi WHERE year=2026").df()
        latest = con.execute(f"SELECT symbol, close, amount AS amt_latest FROM kdata WHERE date='{last_date}'").df()
        hi_60 = con.execute(f"SELECT symbol, MAX(close) AS hi_60 FROM kdata WHERE date>='{w60_start}' AND date<='{last_date}' GROUP BY symbol").df()
        hi_20 = con.execute(f"SELECT symbol, MAX(close) AS hi_20 FROM kdata WHERE date>='{w20_start}' AND date<='{last_date}' GROUP BY symbol").df()
        hi_250 = con.execute(f"SELECT symbol, MAX(close) AS hi_250 FROM kdata WHERE date>='{w250_start}' AND date<='{last_date}' GROUP BY symbol").df()

        ret20 = con.execute(f"""
            WITH r AS (
                SELECT symbol, date, close,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rk
                FROM kdata WHERE date<='{last_date}' AND date>='{w20_start}'
            )
            SELECT symbol,
                   MAX(CASE WHEN rk=1  THEN close END) AS close_today,
                   MAX(CASE WHEN rk=20 THEN close END) AS close_20d
            FROM r WHERE rk IN (1,20) GROUP BY symbol
        """).df()
        ret20['ret_20d_pct'] = (ret20['close_today']/ret20['close_20d'] - 1) * 100

        rsi14 = con.execute(f"""
            WITH d AS (
                SELECT symbol, date, close,
                       LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_c
                FROM kdata
                WHERE date <= CAST('{last_date}' AS DATE)
                  AND date >= CAST('{last_date}' AS DATE) - INTERVAL 25 DAY
            ),
            chg AS (
                SELECT symbol, (close - prev_c) / prev_c AS d_ret
                FROM d WHERE prev_c IS NOT NULL
            )
            SELECT symbol,
                   100 * SUM(CASE WHEN d_ret>0 THEN d_ret ELSE 0 END) /
                   NULLIF(SUM(ABS(d_ret)), 0) AS rsi14_proxy
            FROM chg GROUP BY symbol
        """).df()

        m = asi_2026.merge(latest, on='symbol') \
                    .merge(hi_60, on='symbol') \
                    .merge(hi_20, on='symbol') \
                    .merge(hi_250, on='symbol') \
                    .merge(ret20[['symbol','ret_20d_pct']], on='symbol') \
                    .merge(rsi14, on='symbol')
        m['draw_60_pct']  = (m['close']/m['hi_60'] - 1) * 100
        m['draw_20_pct']  = (m['close']/m['hi_20'] - 1) * 100
        m['draw_250_pct'] = (m['close']/m['hi_250'] - 1) * 100

        # 名称 + 行业
        m = m.merge(basic[['代码','名称','细分行业','地区']].rename(columns={'代码':'symbol'}),
                    on='symbol', how='left')
        # recent_strong_years (2024+2025 连续 2 年 > p90)
        m['strong_years'] = m['symbol'].map(lambda s: int(strong_df[strong_df['symbol']==s]['recent_strong_years'].iloc[0]) if s in strong_set else 0)

        # 只保留最近 2 年连续强势
        m = m[m['strong_years'] >= 2].copy()

        slices = {}

        # 切片 1: 60 日回撤
        s1 = m[(m['top100_days'] >= 30) & (m['draw_60_pct'] <= -15)]
        s1 = s1.sort_values(['top100_days','draw_60_pct'], ascending=[False, True])
        slices['v2_1_60日回撤'] = s1

        # 切片 2: RSI 超卖
        s2 = m[(m['top100_days'] >= 30) & (m['rsi14_proxy'] <= 30)]
        s2 = s2.sort_values('rsi14_proxy', ascending=True)
        slices['v2_2_RSI超卖'] = s2

        # 切片 5: 组合
        s5 = m[(m['top100_days'] >= 25) & (m['draw_60_pct'] <= -12)
               & (m['rsi14_proxy'] <= 40) & (m['ret_20d_pct'] < 0)]
        s5 = s5.sort_values('draw_60_pct', ascending=True)
        slices['v2_5_组合多条件'] = s5

        # 切片 6: 大周期回调
        s6 = m[(m['top100_days'] >= 25) & (m['draw_250_pct'] <= -20)]
        s6 = s6.sort_values('draw_250_pct', ascending=True)
        slices['v2_6_大周期回撤'] = s6

        # 切片 7: 温和杀跌
        s7 = m[(m['top100_days'] >= 20) & (m['draw_60_pct'] <= -10)
               & (m['ret_20d_pct'] <= -5) & (m['rsi14_proxy'] <= 50)]
        s7 = s7.sort_values('draw_60_pct', ascending=True)
        slices['v2_7_温和杀跌'] = s7

        # 数值列保留两位小数 (供显示)
        for k, df in slices.items():
            for c in ['draw_60_pct','draw_20_pct','draw_250_pct','ret_20d_pct','rsi14_proxy']:
                if c in df.columns: df[c] = df[c].round(2)

        return slices, last_date

    slices, last_date = compute_dip_pool()
    st.caption(f"基础数据日: {last_date}  |  长期强势样本: 2024+2025 连续 2 年 ASI 年度排名 p90+ 的股票")
    st.info("💡 切到 K线页/ASI 排名页可继续研究每只票; 表里点击行 = 跳转")

    slice_titles = {
        'v2_1_60日回撤':      ('🔻 v2-1 高关注 + 60 日回撤 ≥ 15%', 'top100_days≥30, draw_60_pct≤-15'),
        'v2_2_RSI超卖':      ('🔻 v2-2 高关注 + RSI(14) ≤ 30 (超卖)', 'top100_days≥30, rsi14_proxy≤30'),
        'v2_5_组合多条件':    ('🔻 v2-5 组合: 4 条件交集', 'top100≥25 + 60日≥12% + RSI≤40 + 20日跌'),
        'v2_6_大周期回撤':    ('🔻 v2-6 长期强势 + 大周期回调 (250 日 ≥ 20%)', 'top100≥25, draw_250_pct≤-20'),
        'v2_7_温和杀跌':      ('🔻 v2-7 温和杀跌 (放宽)', 'top100≥20 + 60日≥10% + 20日跌≥5% + RSI≤50'),
    }

    display_cols = ['symbol','名称','细分行业','strong_years','close','top100_days','asi_sum',
                    'draw_60_pct','draw_20_pct','draw_250_pct','ret_20d_pct','rsi14_proxy']

    for k, df in slices.items():
        title, cond = slice_titles[k]
        st.subheader(f"{title}   ({len(df)} 只)")
        st.caption(f"条件: {cond}")
        if len(df) == 0:
            st.warning("无候选")
            continue
        show_cols = [c for c in display_cols if c in df.columns]
        view = df[show_cols].reset_index(drop=True)
        # 用 st.dataframe 渲染 (可点击查看; 跳转用 session_state)
        st.dataframe(
            view,
            use_container_width=True,
            hide_index=True,
            column_config={
                'symbol': st.column_config.TextColumn('代码', width='small'),
                '名称':   st.column_config.TextColumn('名称', width='small'),
                '细分行业': st.column_config.TextColumn('行业', width='medium'),
                'strong_years': st.column_config.NumberColumn('强势年数', width='small'),
                'close':   st.column_config.NumberColumn('现价', format='%.2f'),
                'top100_days': st.column_config.NumberColumn('top100天数', width='small'),
                'asi_sum': st.column_config.NumberColumn('2026 asi_sum', format='%.0f'),
                'draw_60_pct':  st.column_config.NumberColumn('60日回撤%', format='%.2f'),
                'draw_20_pct':  st.column_config.NumberColumn('20日回撤%', format='%.2f'),
                'draw_250_pct': st.column_config.NumberColumn('250日回撤%', format='%.2f'),
                'ret_20d_pct':  st.column_config.NumberColumn('20日涨幅%', format='%.2f'),
                'rsi14_proxy':  st.column_config.NumberColumn('RSI(14)', format='%.2f'),
            },
        )
        # 跳转按钮
        c1, c2 = st.columns([1, 5])
        with c1:
            target = st.selectbox(
                "跳到 K线",
                view['symbol'].tolist(),
                format_func=lambda s: f"{s} {view[view['symbol']==s]['名称'].iloc[0]}",
                key=f'jump_{k}',
                label_visibility='collapsed',
            )
        with c2:
            if st.button(f"📊 查看 {target} K线", key=f'btn_jump_{k}'):
                st.query_params.update(page='📊 个股K线', symbol=target)
                st.rerun()
        st.divider()

# ============================================================
# 页面 6: 同业对比
# ============================================================
elif page == "⚖️ 同业对比":
    st.header("⚖️ 同业对比 (归一化净值)")

    # 读 session_state 预设 (从行业强度页跳来的)
    preset_ind = st.session_state.pop('peer_industry_preset', None) if 'peer_industry_preset' in st.session_state else None

    c1, c2 = st.columns([1, 2])
    with c1:
        industries = ['全部'] + sorted(basic['细分行业'].dropna().unique().tolist())
        default_ind_idx = 0
        if preset_ind and preset_ind in industries:
            default_ind_idx = industries.index(preset_ind)
        ind_choice = st.selectbox("按行业筛选", industries, index=default_ind_idx)

        if ind_choice == '全部':
            pool = basic.head(100)
        else:
            pool = basic[basic['细分行业'] == ind_choice].head(50)

        selected_names = st.multiselect(
            "选择对比股票 (最多 10)",
            (pool['代码'] + ' ' + pool['名称']).tolist(),
            default=(pool['代码'] + ' ' + pool['名称']).head(3).tolist()
        )

    with c2:
        c21, c22 = st.columns(2)
        with c21:
            d_from = st.date_input("起始日", date(2025, 1, 1), key='peer_from')
        with c22:
            d_to = st.date_input("结束日", date.today(), key='peer_to')

    if not selected_names:
        st.info("请选择至少一只股票")
        st.stop()

    symbols = [s.split(' ')[0] for s in selected_names[:10]]
    df = load_peer_compare(symbols, d_from, d_to)

    fig = go.Figure()
    for sym in symbols:
        sub = df[df['symbol'] == sym]
        name = basic[basic['代码'] == sym]['名称'].iloc[0] if len(basic[basic['代码'] == sym]) else sym
        fig.add_trace(go.Scatter(
            x=sub['date'], y=sub['nav'],
            mode='lines', name=f"{name} ({sym})", line=dict(width=2)
        ))

    fig.update_layout(
        title=f"归一化净值对比 (起始日=1.0)",
        xaxis_title="日期", yaxis_title="净值",
        hovermode='x unified', height=600
    )
    fig.add_hline(y=1.0, line_dash="dash", line_color="gray", opacity=0.5)
    st.plotly_chart(fig, use_container_width=True)

    # 收益对比表
    st.subheader("📊 收益对比")
    rows = []
    for sym in symbols:
        sub = df[df['symbol'] == sym]
        if len(sub) > 1:
            nav_start = sub['nav'].iloc[0]
            nav_end = sub['nav'].iloc[-1]
            ret = (nav_end / nav_start - 1) * 100
            name = basic[basic['代码'] == sym]['名称'].iloc[0] if len(basic[basic['代码'] == sym]) else sym
            rows.append({'代码': sym, '名称': name, '区间收益(%)': round(ret, 2),
                         '最大净值': round(sub['nav'].max(), 3),
                         '最小净值': round(sub['nav'].min(), 3)})
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

    # 跳转入口
    st.subheader("🔍 跳转到个股详情")
    c1, c2, c3 = st.columns([3, 1, 1])
    with c1:
        jump_sym = st.selectbox("选择要查看的股", symbols,
                                format_func=lambda s: f"{s} {basic[basic['代码']==s]['名称'].iloc[0] if len(basic[basic['代码']==s]) else s}",
                                key='peer_jump')
    with c2:
        st.write("")
        if st.button("📊 K线 + ASI", key='peer_jump_btn', use_container_width=True):
            st.session_state._current_page = "📊 个股K线"
            st.session_state.symbol = jump_sym
            st.session_state.return_to = "⚖️ 同业对比"
            st.session_state.pop('peer_industry_preset', None)
            st.rerun()
    with c3:
        st.write("")
        if st.button("↩️ 跳到行业强度", key='peer_back_ind', use_container_width=True):
            st.session_state._current_page = "🏭 行业强度"
            st.rerun()

st.caption("--- 数据源: ~/stock_data/stock.db | Powered by Streamlit + DuckDB + Plotly")
