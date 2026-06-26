"""
A股数据可视化面板 (Streamlit)
- 个股 K线 / 成交量 / 资金 / 收益率分布
- ASI 年度排名 Top N
- 同业对比
- 行业强弱热力图

启动: ~/stock/.venv/bin/streamlit run ~/stock/dashboard.py --server.port 8502
默认: http://localhost:8501
"""
import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date, timedelta
from pathlib import Path
import os
import time
import threading

# ---------- 配置 ----------
# 数据源: Parquet 文件 (无锁, 持久, 原子替换)
# 2026-06-05 迁移: 从 DuckDB stock.db 改为 4 个独立 Parquet 文件
# 支持 STOCK_KDATA / STOCK_KDATA_WINDOW / STOCK_ASI / STOCK_ASI_UP / STOCK_BASIC 环境变量覆盖 (默认 ~/stock_data/...)
KDATA_PATH    = os.path.expanduser(os.environ.get('STOCK_KDATA',        '~/stock_data/kdata.parquet'))
# 2026-06-23 窗口 parquet: 裁掉 1990s 历史数据, 物化内存从 ~3.7GB 降到 ~1.3GB (5y) / ~530MB (2y)
# 优先用 window 版 (~/stock_data/kdata_5y.parquet), 不存在时降级到 STOCK_KDATA + WHERE 过滤
WINDOW_YEARS  = int(os.environ.get('STOCK_WINDOW_YEARS', '5'))
WINDOW_PATH   = os.path.expanduser(os.environ.get('STOCK_KDATA_WINDOW', f'~/stock_data/kdata_{WINDOW_YEARS}y.parquet'))
ASI_PATH      = os.path.expanduser(os.environ.get('STOCK_ASI',     '~/stock_data/asi_yearly.parquet'))
ASI_UP_PATH   = os.path.expanduser(os.environ.get('STOCK_ASI_UP',  '~/stock_data/asi_yearly_up.parquet'))
BASIC_PATH    = os.path.expanduser(os.environ.get('STOCK_BASIC',   '~/stock_data/stock_basic.parquet'))
HEAT_PATH     = os.path.expanduser(os.environ.get('STOCK_HEAT',     '~/stock_data/heat_rotation_daily.parquet'))

# 中文字体 (Camoufox 缓存的 NotoSansSC) — plotly 自带字体回退机制
# 不需要 matplotlib（dashboard 全程用 plotly 画图）

st.set_page_config(
    page_title="A股数据面板",
    page_icon="📈",
    layout="wide",
)

# ---------- 页面说明 (可折叠, 默认收起) ----------
# 12 个页面每个一份说明, 解释功能/计算方式/如何观察
# 用法: 在每个页面 st.header 后调用 page_help("个股K线")
PAGE_HELP = {
    "🏠 总览": """
**功能**：一眼看当前 A 股大盘 + 自选股的整体状况。

**统计项**：
- 📦 总股票数：过滤 ST 与停牌后的活跃标的数
- 📅 数据范围：kdata parquet 中最早到最晚的交易日
- 💾 总行数：所有 K 线记录的累计行数（包含 1990 年至今）

**观察方式**：先看"今日成交额"折线趋势判断市场冷热；右侧行业/地区分布了解结构。
""",

    "📊 个股K线": """
**功能**：单只票的完整 K 线 + 衍生指标（ASI / RPS）。

**统计项**：
- ASI (0-100)：基于成交额百分位排名 + 涨跌幅加权的日强度，年度聚合为 ASI_sum
- RPS (5/10/20/60/120 周期)：个股涨幅在 N 日窗口内的全市场百分位排名
- 个股 K 线子图：开盘/收盘/最高/最低 + 成交量 + 成交额 + ASI + RPS

**观察方式**：RPS > 80 视为强势区；ASI 突变放大常对应放量事件。
""",

    "🏆 ASI 排名": """
**功能**：按 ASI 得分排序的个股年度榜单。

**统计项**：
- asi_sum：年内每日 ASI 得分（0-100 加权）总和
- asi_yearly_rank：年度 ASI 排名（1 = 年度最强）
- top50_days / top100_days：年内进入全市场前 50/100 名次数
- 上市满 N 年过滤：剔除次新股（默认 1 年）

**计算方式**：`asi_yearly.parquet` 预计算，公式见 `~/stock/asi_calculator_parquet.py`。

**观察方式**：asi_yearly_rank 越小越强；asi_sum + rank 同时看能过滤"偶然某天爆发"的票。
""",

    "📈 RPS 排名": """
**功能**：多周期 RPS 排名榜单（5/10/20/60/120 日）。

**统计项**：
- rps_5 / rps_10 / rps_20 / rps_60 / rps_120：N 日涨跌幅的全市场百分位
- ret_5d / ret_10d ...：对应周期内累计涨幅 %

**计算方式**：实时 SQL 算，每只票 `PERCENT_RANK() OVER (PARTITION BY date ORDER BY ret_N)`。

**观察方式**：多周期 RPS 同时 > 80 才是真强势；单周期高可能是反弹。
""",

    "🎯 低吸观察池": """
**功能**：基于"长期强势 + 近期回调"逻辑的 5 切片候选池。

**统计项**：v2-1 / v2-2 / v2-5 / v2-6 / v2-7 五个阈值组合
- draw_60 / draw_250：从 60 / 250 日高点回撤幅度
- rsi14：14 日相对强弱指标
- amt_ratio：当日成交额 / 60 日均成交额
- strong_years：连续强势年数（默认最近 2 年都进入 ASI Top 100）

**计算方式**：详见 `~/.hermes/skills/stock-dip-pool/SKILL.md`。

**观察方式**：每个 slice 输出控制在 30-50 只；多 slice 重叠的票优先关注。
""",

    "🏭 行业强度": """
**功能**：按申万一级行业聚合的多日强度排名。

**统计项**：
- weighted_strength（涨跌幅加权）：avg(pct_rank × 涨跌幅%)，正值=强势
- avg_strength（纯成交活跃度）：行业平均成交额百分位
- stock_count：行业内股票数（过滤样本不足的行业）

**计算方式**：`load_industry_strength()` 用 SQL 聚合 + 行业代码 join。

**观察方式**：weighted_strength > 30 是强行业，< -10 是弱势行业；勾"仅上涨日"过滤恐慌出货噪音。
""",

    "⚖️ 同业对比": """
**功能**：多只股票归一化净值对比图（起点 = 1.0）。

**统计项**：
- nav：每只票当日收盘价 / 起始日收盘价
- 所有股票按同一基准归一化，可直接对比"谁跑赢"

**计算方式**：`load_peer_compare()` pivot + 除以第 0 行。

**观察方式**：从行业强度页跳来时已预设行业全部股票；从个股 K 线跳来时可手动选同业对手。
""",

    "🔥 热度轮动": """
**功能**：观察资金/成交额在"昨天 → 今天"的迁移，捕捉升温/降温/留存信号。

**统计项**：
- heat_pct (0-100)：当日成交额在过去 N 个交易日窗口里的百分位排名
- heating：昨天 heat < 50 AND 今天 heat >= 80（新爆发）
- cooling：昨天 heat >= 80 AND 今天 heat < 50（失宠）
- staying：两日都 >= 80（持续热门）
- net_amt（资金加权净流入）：升温股成交额总和 - 降温股成交额总和

**计算方式**：详见 `~/.hermes/skills/stock-heat-rotation/SKILL.md`。

**观察方式**：升温榜 + 降温榜交叉看，能找到"资金从 X 行业流向 Y 行业"的板块轮动；多日趋势 tab 看连续 heating/cooling 的票更可靠（单日可能是噪声）。
""",

    "🔍 筛选检索": """
**功能**：多维度组合筛选（财务指标 + 价格指标）。

**统计项**：基于 stock_basic 静态基本面 + kdata 当日行情。
- PB / PS / PCF / PE-TTM：估值指标
- ROE / 毛利率 / 净利率：盈利能力
- 收入同比 / 利润同比：成长性

**观察方式**：先粗筛（市值 + 行业），再精筛（财务 + 价格），避免组合条件过严导致无结果。
""",

    "🏆 排行榜": """
**功能**：常用排行榜（涨幅/成交额/换手率 Top N）。

**统计项**：基于当日行情的多维度排序。
- 涨幅榜 / 跌幅榜
- 成交额榜 / 换手率榜
- 量比榜（当日量 / 5 日均量）

**观察方式**：结合"是否突破前期高点"判断持续性；纯涨幅榜容易捕捉到一字板无量上涨。
""",

    "🏭 行业概览": """
**功能**：行业层面多维度扫描（涨幅 + 财务 + 资金）。

**统计项**：
- 行业涨跌幅 + 成交额 + 股票数
- 行业平均 ROE / PB / 营收同比
- 行业 vs 行业散点图（ROE × 涨幅）

**观察方式**：找"高 ROE + 涨幅靠前"的行业 = 戴维斯双击候选；找"低 PB + 高 ROE"是被低估的洼地。
""",

    "💻 SQL 控制台": """
**功能**：直接对 4 个 Parquet 写 SQL 查询，绕开 dashboard 封装。

**可用视图/表**：
- `kdata` (TABLE：symbol/date/open/high/low/close/volume/amount)
- `asi_yearly` / `asi_yearly_up` (VIEW：年度 ASI 摘要)
- `stock_basic` (VIEW：53 列静态基本面)
- `stock_slim` (TABLE：symbol/name/industry/region/listing_date，物化加速)

**观察方式**：debug 性质用，平时不必；想要的功能在前面 11 个页面找不到时来这里写 SQL。
""",
}

def page_help(page_name):
    """在每个页面 st.header 后调用，渲染可折叠说明。"""
    text = PAGE_HELP.get(page_name)
    if not text:
        return
    with st.expander("📖 功能说明", expanded=False):
        st.markdown(text)

# ---------- 数据加载 ----------
# 2026-06-05: 改用 DuckDB in-memory 引擎直接读 Parquet 文件
# - 无 .db 文件 → 完全没有 DuckDB 锁问题
# - 每次新开 in-memory 连接, 加载 4 个 Parquet 为 VIEW, 立刻用完即弃
# - 性能与原 DuckDB 一样, 因为底层还是 DuckDB
# - writer 改 Parquet 时, dashboard 仍能读旧 fd 看到旧数据, 不报错

@st.cache_resource
def get_con():
    """打开 in-memory DuckDB, 注册 4 个 Parquet 文件

    2026-06-19 v2 优化 (响应速度):
    - @st.cache_resource: 跨页面/跨用户共享同一个 DuckDB 连接, 避免每次重连
      重建 5.8s 物化表。Streamlit 1.x + duckdb 1.5+ 线程安全。
    - kdata VIEW → TABLE 物化 (16M 行 一次性 O(N) 扫描, 之后查询 50-100x 加速)
    - SET threads=8 (并行扫描)
    - SET enable_object_cache=true (parquet metadata 跨查询缓存)
    - stock_basic 物化出 slim 表 (代码/名称/行业/地区/上市日期) — 各排名页 JOIN 必备

    2026-06-23 窗口优化:
    - 优先读 kdata_{N}y.parquet (裁掉 1990s 历史), 内存峰值从 ~3.7GB 降到 ~1.3GB (5y)
    - 不存在时降级到 KDATA_PATH + WHERE date >= MAX(date) - INTERVAL (零额外磁盘)
    - threads=2 (本地 2 核, 8 线程会线程争抢)

    线程安全说明:
    - DuckDB 1.5+ 默认每个 connection 独立 state, 单线程内安全
    - Streamlit 偶发同 session 多线程 (eg 按钮 click 与 auto-refresh 重叠)
      会触发 "Different thread" 错误, 所以加 RLock 保护
    """
    con = duckdb.connect(':memory:')
    # 性能调优 (本地 2 核机器: threads=2 足够, 8 会争抢)
    con.execute("SET threads TO 2")
    con.execute("SET enable_object_cache TO true")

    # kdata: 优先用 window parquet (裁掉历史数据, 内存峰值显著降低)
    if os.path.exists(WINDOW_PATH):
        # 直接物化小文件, 不需要 filter
        con.execute(f"""
            CREATE OR REPLACE TABLE kdata AS
            SELECT symbol, date, open, high, low, close, volume, amount
            FROM read_parquet('{WINDOW_PATH}')
        """)
    elif os.path.exists(KDATA_PATH):
        # 降级: 全量 + WHERE 过滤 (内存峰值不变, 但兼容性更好)
        con.execute(f"""
            CREATE OR REPLACE TABLE kdata AS
            SELECT symbol, date, open, high, low, close, volume, amount
            FROM read_parquet('{KDATA_PATH}')
            WHERE date >= (SELECT MAX(date) FROM read_parquet('{KDATA_PATH}')) - INTERVAL '{WINDOW_YEARS} years'
        """)

    # asi_yearly 两个口径 — 体积小, VIEW 即可
    for view, path in [('asi_yearly', ASI_PATH), ('asi_yearly_up', ASI_UP_PATH)]:
        if os.path.exists(path):
            con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet('{path}')")

    # stock_basic: 全字段 50+ 列, 物化 slim 版本 (代码/名称/行业/地区/上市日期) 给排名页用
    if os.path.exists(BASIC_PATH):
        con.execute(f"""
            CREATE OR REPLACE VIEW stock_basic AS
            SELECT * FROM read_parquet('{BASIC_PATH}')
        """)
        con.execute(f"""
            CREATE OR REPLACE TABLE stock_slim AS
            SELECT 代码 AS symbol,
                   名称 AS name,
                   细分行业 AS industry,
                   地区 AS region,
                   上市日期 AS listing_date
            FROM read_parquet('{BASIC_PATH}')
        """)
    # 加 RLock 保护跨线程访问 (Streamlit 单 session 偶发多线程)
    # duckdb 1.5 C 扩展对象不能加属性, 用 module-level dict 存 lock
    con_id = id(con)
    _CON_LOCKS[con_id] = threading.RLock()
    return con

def get_lock(con):
    """获取 connection 对应的 lock (目前 load_* 函数未使用, 留作未来扩展)"""
    return _CON_LOCKS.get(id(con))

_CON_LOCKS = {}  # id(con) -> RLock

def safe_query(sql, params=None, label=""):
    """执行 SQL (无需 lock 处理, Parquet 文件始终可读)"""
    con = get_con()
    lock = get_lock(con)
    if lock:
        with lock:
            return con.execute(sql, params or []).df()
    return con.execute(sql, params or []).df()

# ── 缓存下拉数据 (高频小查询) ──
@st.cache_data(ttl=3600)
def get_symbols() -> list:
    """返回 [{symbol, name, industry, region}, ...] 用于下拉, 1h 缓存"""
    con = get_con()
    df = con.execute("""
        SELECT s.symbol, s.name, s.industry, s.region
        FROM stock_slim s
        WHERE s.symbol IN (SELECT DISTINCT symbol FROM kdata)
        ORDER BY s.symbol
    """).df()
    return df.to_dict("records")

@st.cache_data(ttl=3600)
def get_industries() -> list:
    con = get_con()
    return [r[0] for r in con.execute(
        "SELECT DISTINCT industry FROM stock_slim WHERE industry IS NOT NULL AND industry != '' ORDER BY industry"
    ).fetchall()]

@st.cache_data(ttl=3600)
def get_regions() -> list:
    con = get_con()
    return [r[0] for r in con.execute(
        "SELECT DISTINCT region FROM stock_slim WHERE region IS NOT NULL AND region != '' ORDER BY region"
    ).fetchall()]

@st.cache_data(ttl=3600)
def get_date_range() -> tuple:
    con = get_con()
    mn, mx = con.execute("SELECT MIN(date), MAX(date) FROM kdata").fetchone()
    return mn.date() if hasattr(mn, 'date') else mn, mx.date() if hasattr(mx, 'date') else mx

@st.cache_data(ttl=3600)
def get_kpi() -> dict:
    """侧栏用总览数据 — 一次性取齐"""
    con = get_con()
    total_stocks = con.execute("SELECT COUNT(DISTINCT symbol) FROM kdata").fetchone()[0]
    date_range = con.execute("SELECT MIN(date), MAX(date) FROM kdata").fetchone()
    date_range = (date_range[0].date() if hasattr(date_range[0], 'date') else date_range[0],
                   date_range[1].date() if hasattr(date_range[1], 'date') else date_range[1])
    total_rows = con.execute("SELECT COUNT(*) FROM kdata").fetchone()[0]
    return dict(total_stocks=total_stocks, date_range=date_range, total_rows=total_rows)

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

@st.cache_data(ttl=600)
def compute_heat_rotation(window_days: int = 20, hot_th: float = 80.0, cold_th: float = 50.0):
    """
    热度轮动: 每只票每天一个热度分 (成交额在过去 window_days 天的百分位排名 0-100)
    返回 side-by-side 两天对比 + 升温/降温/留存三类信号
    过滤: 剔除 ST, 上市不足 60 天
    """
    con = get_con()
    sql = f"""
    WITH recent_per_symbol AS (
        SELECT k.symbol, k.date, k.amount, k.close,
               b.名称 AS name, b.细分行业 AS industry,
               ROW_NUMBER() OVER (PARTITION BY k.symbol ORDER BY k.date DESC) AS rn_global
        FROM kdata k
        JOIN stock_basic b ON k.symbol = b.代码
        WHERE k.date >= (SELECT MAX(date) FROM kdata) - INTERVAL '{window_days + 15} days'
          AND b.名称 NOT LIKE '%ST%'
          AND b.上市日期 <= (SELECT MAX(date) FROM kdata) - INTERVAL '60 days'
    ),
    recent_dates AS (
        SELECT DISTINCT date FROM recent_per_symbol ORDER BY date DESC LIMIT 2
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
    heat_t AS (
        SELECT t_sym AS symbol, t_date AS eval_date, t_amount AS eval_amount,
               close AS eval_close, name, industry,
               SUM(CASE WHEN h_amount <= t_amount THEN 1 ELSE 0 END) * 100.0
               / NULLIF(COUNT(*), 0) AS heat_pct
        FROM hist_pairs
        GROUP BY t_sym, t_date, t_amount, close, name, industry
    ),
    side_by_side AS (
        SELECT symbol, name, industry,
               MAX(CASE WHEN eval_date = (SELECT MAX(date) FROM kdata) THEN heat_pct END) AS heat_today,
               MAX(CASE WHEN eval_date = (SELECT MAX(date) FROM kdata) THEN eval_close END) AS close_today,
               MAX(CASE WHEN eval_date = (SELECT MAX(date) FROM kdata) THEN eval_amount END) AS amt_today,
               MAX(CASE WHEN eval_date = (SELECT MAX(date) FROM kdata) - INTERVAL '1 day' THEN heat_pct END) AS heat_yest,
               MAX(CASE WHEN eval_date = (SELECT MAX(date) FROM kdata) - INTERVAL '1 day' THEN eval_close END) AS close_yest,
               MAX(CASE WHEN eval_date = (SELECT MAX(date) FROM kdata) - INTERVAL '1 day' THEN eval_amount END) AS amt_yest
        FROM heat_t GROUP BY symbol, name, industry
    )
    SELECT *,
           (close_today - close_yest) / close_yest * 100 AS ret_pct
    FROM side_by_side
    """
    df = con.execute(sql).df()
    today_str = str(con.execute("SELECT MAX(date) FROM kdata").fetchone()[0])
    heating = df[(df['heat_yest'] < cold_th) & (df['heat_today'] >= hot_th)].copy()
    cooling = df[(df['heat_yest'] >= hot_th) & (df['heat_today'] < cold_th)].copy()
    staying = df[(df['heat_yest'] >= hot_th) & (df['heat_today'] >= hot_th)].copy()
    heating = heating.sort_values('heat_today', ascending=False).reset_index(drop=True)
    cooling = cooling.sort_values('heat_yest', ascending=False).reset_index(drop=True)
    staying = staying.sort_values(['heat_today', 'heat_yest'], ascending=[False, False]).reset_index(drop=True)

    # 行业汇总: 升温/降温/留存 按行业聚合 (净流入 = 升温股数 - 降温股数)
    base_industry = df.dropna(subset=['industry']).copy()
    base_industry['is_heating'] = (base_industry['heat_yest'] < cold_th) & (base_industry['heat_today'] >= hot_th)
    base_industry['is_cooling'] = (base_industry['heat_yest'] >= hot_th) & (base_industry['heat_today'] < cold_th)
    base_industry['is_staying'] = (base_industry['heat_yest'] >= hot_th) & (base_industry['heat_today'] >= hot_th)
    base_industry['heating_amt'] = base_industry['amt_today'].where(base_industry['is_heating'], 0)
    base_industry['cooling_amt'] = base_industry['amt_today'].where(base_industry['is_cooling'], 0)
    industry_summary = base_industry.groupby('industry').agg(
        total_stocks=('symbol', 'count'),
        heating_n=('is_heating', 'sum'),
        cooling_n=('is_cooling', 'sum'),
        staying_n=('is_staying', 'sum'),
        heating_amt=('heating_amt', 'sum'),
        cooling_amt=('cooling_amt', 'sum'),
    ).reset_index()
    industry_summary['net_flow'] = industry_summary['heating_n'] - industry_summary['cooling_n']
    industry_summary['net_amt'] = industry_summary['heating_amt'] - industry_summary['cooling_amt']
    industry_summary['heat_ratio'] = industry_summary['heating_n'] / industry_summary['total_stocks']
    # 默认按 net_amt 排序 (资金加权, 更接近真实资金流向)
    industry_summary = industry_summary.sort_values('net_amt', ascending=False).reset_index(drop=True)

    return heating, cooling, staying, industry_summary, today_str

HEAT_PATH = os.path.expanduser("~/stock_data/heat_rotation_daily.parquet")

@st.cache_data(ttl=600)
def compute_heat_history(days: int = 5, window_days: int = 20,
                         hot_th: float = 80.0, cold_th: float = 50.0):
    """
    多日热度: 返回最近 `days` 个交易日 的 long-format 数据 (date, symbol, heat_pct, amt, signal)
    signal: 'heating' (前一日 heat<cold, 当日 heat>=hot) / 'cooling' (反向) /
            'staying' (两日都 >= hot) / 'normal' (其他)
    用 DENSE_RANK 取最近 `days` 个交易日, 避开周末/假期造成的 INTERVAL 误差
    """
    con = get_con()
    lookback = days * 3 + window_days + 10  # 涵盖周末 + 节假日的安全余量
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
        -- 最近 N 个交易日 (DENSE_RANK, 跳过周末)
        SELECT DISTINCT date FROM recent_per_symbol
        ORDER BY date DESC LIMIT {days}
    ),
    target_days AS (
        SELECT r.* FROM recent_per_symbol r
        JOIN recent_dates d ON r.date = d.date
    ),
    hist_pairs AS (
        -- 一次 join: target 的每条记录配 window_days 条历史 amount
        SELECT t.symbol AS t_sym, t.date AS t_date,
               t.amount AS t_amount, t.close, t.name, t.industry,
               h.amount AS h_amount
        FROM target_days t
        JOIN recent_per_symbol h ON h.symbol = t.symbol
        WHERE h.rn_global BETWEEN (t.rn_global + 1) AND (t.rn_global + {window_days})
    ),
    heat_calc AS (
        -- 一次 GROUP BY 算 heat_pct, 避开相关子查询 (days=30 时性能提升 100x)
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
    # 算每只票每天的 signal: 需要与前一日对比
    df = df.sort_values(['symbol', 'date']).reset_index(drop=True)
    df['heat_prev'] = df.groupby('symbol')['heat_pct'].shift(1)
    df['ret_pct'] = df.groupby('symbol')['close'].pct_change() * 100
    df['signal'] = 'normal'
    df.loc[(df['heat_prev'] < cold_th) & (df['heat_pct'] >= hot_th), 'signal'] = 'heating'
    df.loc[(df['heat_prev'] >= hot_th) & (df['heat_pct'] < cold_th), 'signal'] = 'cooling'
    df.loc[(df['heat_prev'] >= hot_th) & (df['heat_pct'] >= hot_th), 'signal'] = 'staying'
    df = df.dropna(subset=['heat_prev'])  # 丢掉每个 symbol 第一条 (无前日对比)
    df = df.sort_values(['date', 'heat_pct'], ascending=[False, False]).reset_index(drop=True)
    return df

@st.cache_data(ttl=300)
def append_heat_rotation_today(window_days: int = 20, hot_th: float = 80.0, cold_th: float = 50.0):
    """
    把今天的轮动数据追加到 ~/stock_data/heat_rotation_daily.parquet
    幂等: 写入前 dedup by (date, symbol, window, hot, cold)
    注: 取 days=2 拿前一天作为 heat_prev 对照基准
    """
    con = get_con()
    today_str = str(con.execute("SELECT MAX(date) FROM kdata").fetchone()[0])

    df_today = compute_heat_history(days=2, window_days=window_days,
                                     hot_th=hot_th, cold_th=cold_th)
    if df_today.empty:
        return 0, today_str
    # 只保留今天 (最后一天) 的记录
    from datetime import datetime as _dt
    max_date = pd.Timestamp(df_today['date'].max())
    df_today = df_today[df_today['date'] == max_date].copy()
    if df_today.empty:
        return 0, today_str
    df_today['window_days'] = window_days
    df_today['hot_th'] = hot_th
    df_today['cold_th'] = cold_th

    # 读取已有 parquet
    if os.path.exists(HEAT_PATH):
        existing = pd.read_parquet(HEAT_PATH)
        # dedup keys
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

@st.cache_data(ttl=300)
def load_heat_rotation_backtest(min_window_days: int = 20, hot_th: float = 80.0, cold_th: float = 50.0):
    """
    回测: 历史上所有被标记为 heating 的票, 次日 (T+1) 的 ret_pct 表现
    """
    if not os.path.exists(HEAT_PATH):
        return pd.DataFrame()
    df = pd.read_parquet(HEAT_PATH)
    # 只看同参数的历史数据 (避免不同阈值混淆)
    df = df[(df['window_days'] == min_window_days) &
            (df['hot_th'] == hot_th) &
            (df['cold_th'] == cold_th)].copy()
    if df.empty:
        return df
    # 次日 ret_pct: 同一只票同一天, 后一天的 ret_pct
    df = df.sort_values(['symbol', 'date']).reset_index(drop=True)
    df['next_ret'] = df.groupby('symbol')['ret_pct'].shift(-1)
    return df

# ---------- 侧栏 ----------
with st.sidebar:
    st.title("📈 A股数据面板")
    st.caption(f"数据源: {os.path.basename(KDATA_PATH)}")
    st.caption(f"今日: {date.today()}")

    # 页面切换: st.radio 直接返回用户选择, 不绕 session_state
    PAGES = ["🏠 总览", "📊 个股K线", "🏆 ASI 排名", "📈 RPS 排名", "🎯 低吸观察池", "🏭 行业强度", "⚖️ 同业对比",
             "🔥 热度轮动", "🔍 筛选检索", "🏆 排行榜", "🏭 行业概览", "💻 SQL 控制台"]

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
    # mtime 自动清缓存 (数据更新后下次访问自动重查)
    # 注意: 必须包含 dashboard 实际读的 WINDOW_PATH 和 HEAT_PATH,
    # 否则 writer 重建 window 或 sync_heat 写盘不会触发 cache_resource 失效
    DATA_SOURCES = [
        Path(KDATA_PATH),
        Path(WINDOW_PATH) if os.path.exists(WINDOW_PATH) else None,
        Path(ASI_PATH), Path(ASI_UP_PATH), Path(BASIC_PATH),
        Path(HEAT_PATH) if os.path.exists(HEAT_PATH) else None,
    ]
    DATA_SOURCES = [p for p in DATA_SOURCES if p is not None]
    def _latest_mtime() -> float:
        ms = []
        for p in DATA_SOURCES:
            if p.exists():
                ms.append(p.stat().st_mtime)
        return max(ms) if ms else 0.0
    def _fmt_mtime(ts: float) -> str:
        return datetime.fromtimestamp(ts).strftime("%m-%d %H:%M") if ts > 0 else "—"
    current_mtime = _latest_mtime()
    if "_data_mtime_seen" not in st.session_state:
        st.session_state._data_mtime_seen = current_mtime
    if current_mtime > st.session_state._data_mtime_seen:
        st.cache_data.clear()
        st.session_state._data_mtime_seen = current_mtime
        st.toast(f"🔄 数据已更新 ({_fmt_mtime(current_mtime)})", icon="📈")
    st.caption(f"🕒 数据快照: {_fmt_mtime(current_mtime)}")
    if st.button("🔄 刷新缓存"):
        st.cache_data.clear()
        st.rerun()

# ---------- 主页 ----------
# KPI 一次性取齐 (1h 缓存)
kpi = get_kpi()
total_stocks = kpi['total_stocks']
date_range = kpi['date_range']
total_rows = kpi['total_rows']

# 股票基础信息 (代码/名称/行业/地区) — 多个页面要用
@st.cache_data(ttl=3600)
def _basic():
    return load_stock_basic()
basic = _basic()
basic['代码'] = basic['代码'].astype(str).str.zfill(6)

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
    page_help("🏠 总览")

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
    st.plotly_chart(fig, width='stretch')

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
        st.plotly_chart(fig, width='stretch')

    with c2:
        st.subheader("🌍 地区分布 Top 15")
        df_reg = basic['地区'].value_counts().head(15).reset_index()
        df_reg.columns = ['地区', '股票数']
        fig = go.Figure(go.Pie(
            labels=df_reg['地区'], values=df_reg['股票数'],
            hole=0.4, textinfo='label+percent'
        ))
        fig.update_layout(height=500)
        st.plotly_chart(fig, width='stretch')

# ============================================================
# 页面 2: 个股K线
# ============================================================
elif page == "📊 个股K线":
    st.header("📊 个股K线分析")
    page_help("📊 个股K线")

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
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        start = st.date_input("起始日", date(2025, 1, 1), min_value=date(1990, 1, 1))
    with c2:
        con_local = get_con()
        db_max = con_local.execute("SELECT MAX(date) FROM kdata").fetchone()[0]
        db_max = db_max.date() if hasattr(db_max, 'date') else db_max  # datetime → date
        end = st.date_input("结束日", db_max, min_value=date(1990, 1, 1), max_value=db_max)
    with c3:
        period = st.selectbox("周期", ["日", "周", "月"], index=0, key='kline_period')
    with c4:
        show_vol = st.checkbox("显示成交量", True)
    with c5:
        show_amount = st.checkbox("显示成交额", True)
    with c6:
        asi_mode = st.radio("ASI口径", ["v2 加权", "v1 仅上涨日"], index=0, key='kline_asi_mode',
                            horizontal=True, help="v2 加权推荐, v1 旧版仅上涨日")
        asi_mode_key = "v2" if "v2" in asi_mode else "up"

    df = load_kdata_with_asi(symbol, start, end, asi_mode_key)
    if df.empty:
        st.warning("该日期范围无数据")
        st.stop()

    # 周/月 K 线 resample (2026-06-19 整合自老 app.py)
    if period != "日":
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        rule = "W" if period == "周" else "ME"
        df = df.set_index("date").resample(rule).agg({
            "open": "first", "high": "max", "low": "min", "close": "last",
            "volume": "sum", "amount": "sum",
            "amount_rank": "mean", "max_rank": "mean", "asi_score": "sum",
            "in_top50": "sum", "in_top100": "sum",
            "rps5": "last", "rps10": "last", "rps20": "last", "rps60": "last", "rps120": "last",
        }).dropna(subset=["open"]).reset_index()
        # 排名类列重置 (mean 后不再有 0/1 意义, 转成 0/1 表示"周期内 ≥1 天上榜")
        df['in_top50'] = (df['in_top50'] > 0).astype(int)
        df['in_top100'] = (df['in_top100'] > 0).astype(int)

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
    st.plotly_chart(fig, width='stretch')

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
        st.dataframe(display, width='stretch', height=400)

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
    st.plotly_chart(fig, width='stretch')

# ============================================================
# 页面 3: ASI 排名
# ============================================================
elif page == "🏆 ASI 排名":
    st.header("🏆 ASI 年度排名")
    page_help("🏆 ASI 排名")

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
    st.dataframe(display, width='stretch', height=500)

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
        if st.button("📊 查看个股详情", width='stretch'):
            st.query_params.update(page="📊 个股K线", symbol=sel)
            st.rerun()
    with c3:
        st.write("")
        if st.button("🏭 查看个股所属行业强度", width='stretch'):
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
        st.plotly_chart(fig, width='stretch')

    with c2:
        st.subheader("行业分布")
        ind_dist = df['细分行业'].value_counts().head(15).reset_index()
        ind_dist.columns = ['行业', '股票数']
        fig = go.Figure(go.Pie(
            labels=ind_dist['行业'], values=ind_dist['股票数'],
            hole=0.4, textinfo='label+percent'
        ))
        fig.update_layout(height=600)
        st.plotly_chart(fig, width='stretch')

# ============================================================
# 页面 4: RPS 排名
# ============================================================
elif page == "📈 RPS 排名":
    st.header("📈 RPS 相对价格强度排名")
    page_help("📈 RPS 排名")
    st.caption("RPS = Relative Price Strength，按涨跌幅百分位排名（0-100）。"
               "多周期交叉验证：5/10 日是短期动量，60/120 日是中长期强度。")

    con = get_con()
    db_max = con.execute("SELECT MAX(date) FROM kdata").fetchone()[0]
    db_max = db_max.date() if hasattr(db_max, 'date') else db_max  # datetime → date

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
    st.dataframe(display, width='stretch', height=500)

    # 跳转入口
    st.subheader("🔍 跳转到个股详情")
    c1, c2 = st.columns([3, 1])
    with c1:
        rps_jump = st.selectbox("选择要查看的股", display['代码'].tolist(),
                                format_func=lambda s: f"{s} {display[display['代码']==s]['名称'].iloc[0]}",
                                key='rps_jump')
    with c2:
        st.write("")
        if st.button("📊 查看该股 K线 + ASI + RPS", key='rps_jump_btn', width='stretch'):
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
        st.plotly_chart(fig, width='stretch')

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
    st.plotly_chart(fig, width='stretch')

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
    page_help("🏭 行业强度")

    # 数据最新日（避免空数据）
    con_local = get_con()
    db_max_date = con_local.execute("SELECT MAX(date) FROM kdata").fetchone()[0]
    db_min_date = con_local.execute("SELECT MIN(date) FROM kdata").fetchone()[0]
    # DuckDB 返回 datetime,streamlit.date_input 是 date — 转 date 才能比较
    db_max_date = db_max_date.date() if hasattr(db_max_date, 'date') else db_max_date
    db_min_date = db_min_date.date() if hasattr(db_min_date, 'date') else db_min_date

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
        st.plotly_chart(fig, width='stretch')

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
        st.dataframe(display, width='stretch', height=600)

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
        if st.button("⚖️ 查看该行业个股", key='ind_jump_btn', width='stretch'):
            # 通过 session_state 传行业, 同业对比页读这个
            st.session_state['peer_industry_preset'] = ind_jump
            st.session_state._current_page = "⚖️ 同业对比"
            st.rerun()
    with c3:
        st.write("")
        # 选该行业第一只股, 直接跳到个股
        first_sym = basic[basic['细分行业'] == ind_jump]['代码'].iloc[0] if len(basic[basic['细分行业'] == ind_jump]) else None
        if first_sym and st.button("📊 看代表股 K线", key='ind_jump_rep', width='stretch'):
            st.session_state._current_page = "📊 个股K线"
            st.session_state.symbol = first_sym
            st.session_state.return_to = "🏭 行业强度"
            st.rerun()

# ============================================================
# 页面 5: 低吸观察池 (长期强势 + 高关注 + 阶段性低位)
# ============================================================
elif page == "🎯 低吸观察池":
    st.header("🎯 低吸观察池")
    page_help("🎯 低吸观察池")
    st.caption("筛选逻辑: 最近 2 年连续强势 (2024+2025 都 ASI 年度排名 p90+) + 当年高关注 (top100≥25) + 价格阶段性回调")

    @st.cache_data(ttl=3600)
    def compute_dip_pool():
        """v2 低吸观察池: 5 个切片, 每次切都用同一基础数据 (asi + kdata + basic)
        返回 dict: slice_name -> DataFrame
        """
        KDATA = os.path.expanduser(os.environ.get('STOCK_KDATA', '~/stock_data/kdata.parquet'))
        ASI   = os.path.expanduser(os.environ.get('STOCK_ASI', '~/stock_data/asi_yearly.parquet'))

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
            width='stretch',
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
    page_help("⚖️ 同业对比")

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
    st.plotly_chart(fig, width='stretch')

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
    st.dataframe(pd.DataFrame(rows), width='stretch')

    # 跳转入口
    st.subheader("🔍 跳转到个股详情")
    c1, c2, c3 = st.columns([3, 1, 1])
    with c1:
        jump_sym = st.selectbox("选择要查看的股", symbols,
                                format_func=lambda s: f"{s} {basic[basic['代码']==s]['名称'].iloc[0] if len(basic[basic['代码']==s]) else s}",
                                key='peer_jump')
    with c2:
        st.write("")
        if st.button("📊 K线 + ASI", key='peer_jump_btn', width='stretch'):
            st.session_state._current_page = "📊 个股K线"
            st.session_state.symbol = jump_sym
            st.session_state.return_to = "⚖️ 同业对比"
            st.session_state.pop('peer_industry_preset', None)
            st.rerun()
    with c3:
        st.write("")
        if st.button("↩️ 跳到行业强度", key='peer_back_ind', width='stretch'):
            st.session_state._current_page = "🏭 行业强度"
            st.rerun()

# ============================================================
# 页面 8: 筛选检索 (2026-06-19 整合自老 app.py)
# 注意: stock_basic 是静态基本面 (53 列), 没有 PE/换手率/涨跌幅 等动态字段
# 涨跌幅 用 kdata 自己算, 静态指标用 stock_basic
# ============================================================
elif page == "🔍 筛选检索":
    st.header("🔍 筛选检索 (基本面 + 行情交叉)")
    page_help("🔍 筛选检索")

    c1, c2 = st.columns(2)
    with c1:
        d_start = st.date_input("起始日", date.today() - timedelta(days=30),
                                min_value=date_range[0], max_value=date_range[1], key='flt_start')
    with c2:
        d_end = st.date_input("结束日", date_range[1],
                              min_value=date_range[0], max_value=date_range[1], key='flt_end')

    c3, c4, c5, c6 = st.columns(4)
    with c3:
        sel_industries = st.multiselect("行业", get_industries(), key='flt_ind')
    with c4:
        sel_regions = st.multiselect("地区", get_regions(), key='flt_reg')
    with c5:
        min_close = st.number_input("最小收盘价", 0.0, 10000.0, 0.0, key='flt_minclose')
    with c6:
        sort_by = st.selectbox("排序", ["成交额(亿)", "总市值(亿)", "涨幅%", "净利润率%", "PB倒数"],
                               key='flt_sort', help="PB倒数=市净率倒序, 越大越便宜; 净利润率=净利润/营收")

    con = get_con()
    conds, params = [], [d_start, d_end]
    if sel_industries:
        ph = ",".join(["?"] * len(sel_industries))
        conds.append(f"b.细分行业 IN ({ph})")
        params.extend(sel_industries)
    if sel_regions:
        ph = ",".join(["?"] * len(sel_regions))
        conds.append(f"b.地区 IN ({ph})")
        params.extend(sel_regions)
    if min_close > 0:
        conds.append("t.close >= ?")
        params.append(min_close)
    extra = " AND ".join(conds)
    if extra:
        extra = "AND " + extra

    sort_map = {
        "成交额(亿)":     "rb.amount_avg DESC",
        "总市值(亿)":     "(b.总股本_亿 * t.close) DESC",
        "涨幅%":          "(t.close - p.prev_close) / NULLIF(p.prev_close, 0) * 100 DESC",
        "净利润率%":      "b.净利润率 DESC",
        "PB倒数":         "1.0 / NULLIF(b.市净率, 0) DESC",
    }

    sql = f"""
        WITH range_bars AS (
            SELECT symbol, AVG(amount) AS amount_avg
            FROM kdata WHERE date BETWEEN ? AND ? GROUP BY symbol
        ),
        today_bars AS (
            SELECT * FROM kdata WHERE date = (SELECT MAX(date) FROM kdata)
        ),
        prev_day AS (
            SELECT k.symbol, k.close AS prev_close
            FROM kdata k
            WHERE k.date = (SELECT MAX(date) FROM kdata WHERE date < (SELECT MAX(date) FROM kdata))
        )
        SELECT
            b.代码       AS 代码,
            b.名称       AS 名称,
            b.细分行业   AS 行业,
            b.地区       AS 地区,
            ROUND(t.close, 2)        AS 收盘,
            ROUND((t.close - p.prev_close) / NULLIF(p.prev_close, 0) * 100, 2) AS 涨幅_pct,
            ROUND(b.净利润率, 2)       AS ROE_pct,
            ROUND(b.市净率, 2)       AS PB,
            ROUND(b.毛利率, 1)       AS 毛利率_pct,
            ROUND(b.收入同比, 1)     AS 营收同比_pct,
            ROUND(b.利润同比, 1)     AS 净利同比_pct,
            ROUND(b.总股本_亿, 2)    AS 总股本_亿,
            ROUND(b.总股本_亿 * t.close, 0) AS 总市值_亿,
            ROUND(rb.amount_avg/1e8, 2) AS 日均成交额_亿
        FROM stock_basic b
        JOIN today_bars t ON b.代码 = t.symbol
        JOIN range_bars rb ON b.代码 = rb.symbol
        JOIN prev_day p ON b.代码 = p.symbol
        WHERE 1=1 {extra}
        ORDER BY {sort_map[sort_by]}
        LIMIT 200
    """
    try:
        df = con.execute(sql, params).df()
    except Exception as e:
        st.error(f"查询失败: {e}")
        st.stop()
    st.caption(f"匹配 {len(df)} 只")
    st.dataframe(df, width='stretch', height=600)
    if not df.empty:
        st.download_button("下载 CSV", df.to_csv(index=False).encode("utf-8-sig"),
                           "filter_result.csv", "text/csv", key='flt_dl')


# ============================================================
# 页面 9: 排行榜 (2026-06-19 整合自老 app.py)
# 当日截面, 涨幅/成交额/ROE/PB倒数
# ============================================================
elif page == "🏆 排行榜":
    st.header("🏆 排行榜 (当日截面)")
    page_help("🏆 排行榜")

    c1, c2 = st.columns(2)
    with c1:
        metric = st.radio("指标", ["涨幅%", "成交额(亿)", "净利润率%", "PB倒数(低→高)"],
                          horizontal=True, key='rank_metric')
    with c2:
        sel_industries_r = st.multiselect("行业 (留空=全部)", get_industries(), key='rank_ind')

    con = get_con()
    where = "1=1"
    params = []
    if sel_industries_r:
        ph = ",".join(["?"] * len(sel_industries_r))
        where += f" AND b.细分行业 IN ({ph})"
        params.extend(sel_industries_r)

    if metric == "涨幅%":
        order_col = "(t.close - p.prev_close) / NULLIF(p.prev_close, 0) * 100"
    elif metric == "成交额(亿)":
        order_col = "t.amount / 1e8"
    elif metric == "净利润率%":
        order_col = "b.净利润率"
    else:  # PB倒数 (低 PB → 高 1/PB)
        order_col = "1.0 / NULLIF(b.市净率, 0)"

    sql = f"""
        WITH today_bars AS (
            SELECT * FROM kdata WHERE date = (SELECT MAX(date) FROM kdata)
        ),
        prev_day AS (
            SELECT k.symbol, k.close AS prev_close
            FROM kdata k
            WHERE k.date = (SELECT MAX(date) FROM kdata WHERE date < (SELECT MAX(date) FROM kdata))
        )
        SELECT
            b.代码, b.名称, b.细分行业 AS 行业, b.地区,
            ROUND(t.close, 2)  AS 收盘,
            ROUND((t.close - p.prev_close) / NULLIF(p.prev_close, 0) * 100, 2) AS 涨幅_pct,
            ROUND(t.amount/1e8, 2) AS 成交额_亿,
            ROUND(b.净利润率, 2) AS ROE_pct,
            ROUND(b.市净率, 2) AS PB,
            ROUND(b.毛利率, 1) AS 毛利率_pct,
            ROUND(b.收入同比, 1) AS 营收同比_pct,
            ROUND(b.利润同比, 1) AS 净利同比_pct
        FROM stock_basic b
        JOIN today_bars t ON b.代码 = t.symbol
        JOIN prev_day p ON b.代码 = p.symbol
        WHERE {where} AND {order_col} IS NOT NULL
        ORDER BY {order_col} DESC
        LIMIT 30
    """
    df_top = con.execute(sql, params).df()
    sql_asc = sql.replace("DESC", "ASC", 1)
    df_bot = con.execute(sql_asc, params).df()

    t1, t2 = st.tabs(["📈 TOP 30", "📉 BOTTOM 30"])
    with t1:
        st.dataframe(df_top, width='stretch', height=600)
        if not df_top.empty:
            st.download_button("下载 TOP CSV", df_top.to_csv(index=False).encode("utf-8-sig"),
                               "rank_top.csv", "text/csv", key='rank_dl_top')
    with t2:
        st.dataframe(df_bot, width='stretch', height=600)


# ============================================================
# 页面 10: 行业概览 (2026-06-19 整合自老 app.py)
# 行业/地区 维度 × 财务质量 × 涨幅 × 散点图
# ============================================================
elif page == "🏭 行业概览":
    st.header("🏭 行业概览 (财务质量 + 涨幅 + 资金)")
    page_help("🏭 行业概览")

    con = get_con()
    dim = st.radio("分组维度", ["行业", "地区"], horizontal=True, key='ind_overview_dim')
    col = "b.细分行业" if dim == "行业" else "b.地区"

    sql = f"""
        WITH today_bars AS (
            SELECT * FROM kdata WHERE date = (SELECT MAX(date) FROM kdata)
        ),
        prev_day AS (
            SELECT k.symbol, k.close AS prev_close
            FROM kdata k
            WHERE k.date = (SELECT MAX(date) FROM kdata WHERE date < (SELECT MAX(date) FROM kdata))
        )
        SELECT {col} AS 分组,
               count(*)                                AS 股票数,
               ROUND(AVG((t.close - p.prev_close) / NULLIF(p.prev_close, 0) * 100), 2) AS 涨幅_pct,
               ROUND(SUM(t.amount)/1e8, 0)             AS 总成交额_亿,
               ROUND(AVG(b.市净率), 2)                 AS 行业PB,
               ROUND(AVG(b.净利润率), 2)                 AS ROE_pct,
               ROUND(AVG(b.毛利率), 1)                 AS 毛利率_pct,
               ROUND(AVG(b.收入同比), 2)               AS 收入同比_pct,
               ROUND(AVG(b.利润同比), 2)               AS 净利同比_pct
        FROM stock_basic b
        JOIN today_bars t ON b.代码 = t.symbol
        JOIN prev_day p ON b.代码 = p.symbol
        WHERE {col} IS NOT NULL AND {col} != ''
        GROUP BY {col}
        HAVING count(*) >= 5
        ORDER BY 涨幅_pct DESC
    """
    df = con.execute(sql).df()

    tab1, tab2, tab3 = st.tabs(["📊 涨幅榜", "💰 财务质量榜", "🔥 资金活跃榜"])
    with tab1:
        st.dataframe(df.head(30), width='stretch', height=500)
    with tab2:
        st.dataframe(df.sort_values("ROE_pct", ascending=False).head(30),
                     width='stretch', height=500)
    with tab3:
        st.dataframe(df.sort_values("总成交额_亿", ascending=False).head(30),
                     width='stretch', height=500)

    st.divider()
    st.subheader("🎯 行业涨幅 vs 财务质量 散点图")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["ROE_pct"], y=df["涨幅_pct"],
        mode="markers+text",
        text=df["分组"], textposition="top center",
        marker=dict(
            size=df["股票数"], sizemode="area",
            sizeref=2.*max(df["股票数"])/(40.**2), sizemin=4,
            color=df["行业PB"], colorscale="Viridis", showscale=True,
            colorbar=dict(title="行业PB")),
        hovertemplate="<b>%{text}</b><br>ROE: %{x:.1f}%<br>涨幅: %{y:.2f}%<br>PB: %{marker.color:.2f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    fig.update_layout(height=600, xaxis_title="行业平均 ROE (%)", yaxis_title="涨幅 (%)")
    st.plotly_chart(fig, width='stretch')


# ============================================================
# 页面: 🔥 热度轮动 (2026-06-25 新增)
# 观察昨天/今天成交额百分位排名的迁移, 找升温/降温/留存三类信号
# ============================================================
elif page == "🔥 热度轮动":
    st.header("🔥 热度轮动")
    page_help("🔥 热度轮动")
    st.caption("""
热度分 = 当日成交额在过去 N 个交易日窗口内的百分位排名 (0-100, 越高=成交额越异常放量)。
样本 = 全 A, 过滤 ST 与上市不足 60 天。

三栏对比:
- **升温榜** (heat_yest < 冷阈值, heat_today >= 热阈值): 昨天默默无闻，今天突然爆发
- **降温榜** (heat_yest >= 热阈值, heat_today < 冷阈值): 昨天还热门，今天资金撤离
- **留存榜** (两日都 >= 热阈值): 持续热门, 资金持续涌入
""")

    # 阈值控件
    c1, c2, c3 = st.columns(3)
    with c1:
        window_days = st.slider("对照窗口 (天)", 5, 60, 20, help="看今天的成交额在过去 N 天里的相对位置")
    with c2:
        hot_th = st.slider("热阈值", 50, 95, 80, help="热度 >= 此值认为热门")
    with c3:
        cold_th = st.slider("冷阈值", 0, 50, 50, help="热度 < 此值认为冷门")

    if hot_th <= cold_th:
        st.error("热阈值必须大于冷阈值")
        st.stop()

    heating, cooling, staying, industry_summary, today_str = compute_heat_rotation(window_days, hot_th, cold_th)

    st.divider()
    st.caption(f"📅 对照日: 今天 **{today_str}** vs 昨天 | 升温 {len(heating)} | 降温 {len(cooling)} | 留存 {len(staying)}")

    def _fmt(df_in, heat_col_today='heat_today', heat_col_yest='heat_yest'):
        out = df_in[['symbol', 'name', 'industry', heat_col_yest, heat_col_today,
                     'amt_today', 'ret_pct', 'close_today']].copy()
        out.columns = ['代码', '名称', '行业', '昨天热度', '今天热度',
                       '今日额(亿)', '今日涨跌%', '今日收盘']
        out['今日额(亿)'] = (out['今日额(亿)'] / 1e8).round(2)
        out['今日涨跌%'] = out['今日涨跌%'].round(2)
        out['今日收盘'] = out['今日收盘'].round(2)
        out['昨天热度'] = out['昨天热度'].round(1)
        out['今天热度'] = out['今天热度'].round(1)
        return out

    tab_h, tab_c, tab_s, tab_ind, tab_hist, tab_bt = st.tabs([
        f"🔥 升温榜 ({len(heating)})",
        f"❄️ 降温榜 ({len(cooling)})",
        f"♨️ 留存榜 ({len(staying)})",
        f"🏭 行业净流入 ({len(industry_summary)})",
        "📈 多日趋势",
        "🧪 回测验证",
    ])

    def _show_with_jump(df_in, key_prefix, label):
        """升温/降温/留存 tab 通用渲染: dataframe + 跳转 K 线"""
        st.subheader(label)
        if df_in.empty:
            st.info("无符合条件的票")
            return
        st.dataframe(_fmt(df_in), width='stretch', height=500)
        # 预计算 name dict 避免 O(N) 遍历
        name_map = dict(zip(df_in['symbol'], df_in['name']))
        target = st.selectbox("跳转 K 线", df_in['symbol'].tolist(),
                               format_func=lambda s, m=name_map: f"{s} {m.get(s, '')}",
                               key=f'heat_jump_{key_prefix}')
        if st.button("📊 查看该股 K 线", key=f'heat_btn_{key_prefix}'):
            st.query_params.update(page="📊 个股K线", symbol=target)
            st.rerun()

    with tab_h:
        _show_with_jump(heating, 'h', "🔥 升温榜: 昨天冷门 → 今天热门")
    with tab_c:
        _show_with_jump(cooling, 'c', "❄️ 降温榜: 昨天热门 → 今天冷门")
    with tab_s:
        _show_with_jump(staying, 's', "♨️ 留存榜: 两天都热门")

    with tab_ind:
        st.subheader("🏭 行业净流入榜")
        st.caption("资金加权版: net_amt = SUM(升温股今日成交额) - SUM(降温股今日成交额) | 票数加权版: net_flow = 升温股数 - 降温股数")

        # 排序模式选择 (用 index 而非字符串包含匹配, 改 radio 选项时不用同时改 if)
        sort_mode_idx = st.radio(
            "排序口径",
            ["💰 资金加权 (net_amt)", "🧮 票数加权 (net_flow)"],
            key='heat_ind_sort', horizontal=True,
        )
        sort_by_amt = sort_mode_idx.startswith("💰")
        sort_col_display = "净流入额(亿)" if sort_by_amt else "净流入(票数)"
        # 过滤行业票数太少的 (样本噪声)
        min_n = st.slider("最少样本数 (行业内股票总数)", 3, 30, 8, key='heat_min_n')
        ind_view = industry_summary[industry_summary['total_stocks'] >= min_n].copy()

        # 加 (亿) 单位换算
        ind_view['net_amt_yi'] = (ind_view['net_amt'] / 1e8).round(2)
        ind_view['heating_amt_yi'] = (ind_view['heating_amt'] / 1e8).round(2)
        ind_view['cooling_amt_yi'] = (ind_view['cooling_amt'] / 1e8).round(2)
        ind_view['heat_ratio_pct'] = (ind_view['heat_ratio'] * 100).round(1)

        # 按用户选择重排
        if sort_by_amt:
            ind_view = ind_view.sort_values('net_amt', ascending=False).reset_index(drop=True)
        else:
            ind_view = ind_view.sort_values('net_flow', ascending=False).reset_index(drop=True)

        ind_show = ind_view[['industry', 'total_stocks', 'heating_n', 'cooling_n',
                              'staying_n', 'net_flow', 'net_amt_yi',
                              'heating_amt_yi', 'cooling_amt_yi', 'heat_ratio_pct']]
        # 列名固定, 两个指标都展示 (票数加权看 net_flow, 资金加权看 net_amt_yi)
        ind_show.columns = ['行业', '总股票数', '升温股数', '降温股数', '留存股数',
                            '净流入(票数)', '净流入额(亿)',
                            '升温总额(亿)', '降温总额(亿)', '升温比例%']
        st.caption(f"📊 当前排序口径: **{sort_col_display}**")
        st.dataframe(ind_show, width='stretch', height=600)

        # 横条图
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**📈 资金最流入 (Top 10, 按{sort_col_display})**")
            chart_col = 'net_amt_yi' if sort_by_amt else 'net_flow'
            top10 = ind_view.nlargest(10, chart_col)
            st.bar_chart(top10.set_index('industry')[chart_col], height=300, horizontal=True)
        with c2:
            st.markdown(f"**📉 资金最流出 (Bottom 10, 按{sort_col_display})**")
            bot10 = ind_view.nsmallest(10, chart_col)
            st.bar_chart(bot10.set_index('industry')[chart_col], height=300, horizontal=True)

        st.divider()
        st.markdown("**🎯 行业资金流向散点图**")
        st.caption("X=净流入额(亿) | Y=留存股数(持续热门) | 气泡大小=行业总股票数 | 颜色=升温比例(越红越热)")

        import plotly.express as px
        scatter = ind_view[ind_view['total_stocks'] >= 5].copy()
        scatter['升温比例'] = scatter['heat_ratio_pct']
        scatter['净流入(亿)'] = scatter['net_amt_yi']

        if len(scatter) > 0:
            fig = px.scatter(
                scatter,
                x='净流入(亿)', y='staying_n',
                size='total_stocks', color='升温比例',
                hover_name='industry',
                hover_data={'net_amt_yi': ':.1f', 'heating_n': True, 'cooling_n': True,
                            'total_stocks': True, '升温比例': ':.1f'},
                color_continuous_scale='RdYlGn',
                labels={'staying_n': '留存股数', '升温比例': '升温比例(%)'},
                height=500,
            )
            fig.update_layout(showlegend=False)
            fig.add_hline(y=0, line_dash='dash', line_color='gray', opacity=0.3)
            fig.add_vline(x=0, line_dash='dash', line_color='gray', opacity=0.3)
            st.plotly_chart(fig, width='stretch')
            st.caption(f"📊 共 {len(scatter)} 个行业 (总股票数 >= 5)")
        else:
            st.info("样本不足, 调整滑块'最少样本数'查看")

        # 跳转某行业内股票到同业对比
        if not ind_view.empty:
            target_ind = st.selectbox("跳转同业对比", ind_view['industry'].tolist(), key='heat_ind_jump')
            if st.button("📊 查看该行业同业对比", key='heat_btn_ind'):
                st.session_state['peer_industry_preset'] = target_ind
                st.session_state._current_page = "⚖️ 同业对比"
                st.rerun()

    with tab_hist:
        st.subheader("📈 多日热度趋势")
        st.caption("""
横向看多日的热度迁移, 找"连续升温/降温/留存"的股票模式。
- 拖动滑块改窗口天数 (5/10/20/30)
- 每日分别给每只票打 1 个 signal, 显示按日期 × 信号的分布
- 数据仅基于当前内存中的 kdata (不依赖持久化)
""")
        hist_days = st.slider("查看最近 N 个交易日", 3, 15, 5, key='heat_hist_days')

        hist_df = compute_heat_history(days=hist_days, window_days=window_days,
                                       hot_th=hot_th, cold_th=cold_th)
        if hist_df.empty:
            st.warning("数据不足")
            st.stop()

        st.divider()
        # 按日期 × 信号 计数
        pivot_cnt = hist_df.groupby(['date', 'signal']).size().unstack(fill_value=0)
        # 保证列顺序
        for sig in ['heating', 'cooling', 'staying', 'normal']:
            if sig not in pivot_cnt.columns:
                pivot_cnt[sig] = 0
        pivot_cnt = pivot_cnt[['heating', 'cooling', 'staying', 'normal']]
        st.markdown("**每日信号分布 (按信号计数)**")
        st.dataframe(pivot_cnt, width='stretch')

        # 信号占比堆叠条
        st.markdown("**每日信号占比**")
        pivot_pct = pivot_cnt.div(pivot_cnt.sum(axis=1), axis=0) * 100
        st.bar_chart(pivot_pct, height=300)

        st.divider()
        # 找出连续 heating / cooling 的票
        st.markdown(f"**连续 ≥ 2 天 heating 的股票 (信号持续=真实趋势)**")
        hist_sorted = hist_df.sort_values(['symbol', 'date']).reset_index(drop=True)
        hist_sorted['prev_signal'] = hist_sorted.groupby('symbol')['signal'].shift(1)
        heat_run = hist_sorted[(hist_sorted['signal'] == 'heating') &
                                (hist_sorted['prev_signal'] == 'heating')]
        if heat_run.empty:
            st.info("无连续 heating 票 (历史窗内还未形成模式)")
        else:
            run_summary = heat_run.groupby(['symbol', 'name']).agg(
                days=('date', 'count'),
                avg_heat=('heat_pct', 'mean'),
                first_date=('date', 'min'),
                last_date=('date', 'max'),
                industry=('industry', 'first'),
            ).reset_index().sort_values(['days', 'avg_heat'], ascending=[False, False])
            st.dataframe(run_summary, width='stretch', height=400)

        st.markdown(f"**连续 ≥ 2 天 cooling 的股票 (信号持续=资金撤离确认)**")
        cool_run = hist_sorted[(hist_sorted['signal'] == 'cooling') &
                                (hist_sorted['prev_signal'] == 'cooling')]
        if cool_run.empty:
            st.info("无连续 cooling 票")
        else:
            run_summary = cool_run.groupby(['symbol', 'name']).agg(
                days=('date', 'count'),
                avg_heat=('heat_pct', 'mean'),
                first_date=('date', 'min'),
                last_date=('date', 'max'),
                industry=('industry', 'first'),
            ).reset_index().sort_values(['days', 'avg_heat'], ascending=[False, True])
            st.dataframe(run_summary, width='stretch', height=400)

        st.divider()
        st.markdown("### 🔍 个股信号历史查询")
        st.caption("输入 6 位股票代码查看该票过去 N 天每天的 heat_pct 与 signal")

        sym_input = st.text_input("股票代码 (6位数字)", "", max_chars=6, key='heat_sym_input')
        if sym_input:
            sym_input_clean = sym_input.strip()
            if not sym_input_clean.isdigit() or len(sym_input_clean) == 0:
                st.warning("⚠️ 请输入 6 位数字代码 (如 600519)")
            else:
                sym_input = sym_input_clean.zfill(6)
                # 拉长 30 天数据, 不依赖 hist_days
                sym_hist = compute_heat_history(days=30, window_days=window_days,
                                                 hot_th=hot_th, cold_th=cold_th)
                sym_data = sym_hist[sym_hist['symbol'] == sym_input].sort_values('date').reset_index(drop=True)
                if sym_data.empty:
                    st.warning(f"代码 {sym_input} 在最近 30 个交易日无数据 (可能停牌/ST/上市不足)")
                else:
                    name = sym_data['name'].iloc[0]
                    industry = sym_data['industry'].iloc[0]
                    st.markdown(f"**{sym_input} {name}** ({industry}) — 过去 {len(sym_data)} 个交易日")

                # 信号统计
                sig_stats = sym_data['signal'].value_counts().to_dict()
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("🔥 heating", sig_stats.get('heating', 0))
                c2.metric("❄️ cooling", sig_stats.get('cooling', 0))
                c3.metric("♨️ staying", sig_stats.get('staying', 0))
                c4.metric("⚪ normal", sig_stats.get('normal', 0))

                # 双图: heat_pct 时序 + ret_pct 时序
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("**热度时序 (heat_pct)**")
                    chart_data = sym_data.set_index('date')[['heat_pct', 'heat_prev']]
                    chart_data.columns = ['今日热度', '昨日热度']
                    st.line_chart(chart_data, height=300)
                with c2:
                    st.markdown("**日收益时序 (ret_pct %)**")
                    st.bar_chart(sym_data.set_index('date')['ret_pct'], height=300)

                # 数据明细表
                st.markdown("**每日明细**")
                detail = sym_data[['date', 'heat_prev', 'heat_pct', 'signal', 'ret_pct', 'amt']].copy()
                detail.columns = ['日期', '昨日热度', '今日热度', '信号', '涨跌%', '成交额']
                detail['成交额'] = (detail['成交额'] / 1e8).round(2)
                detail['涨跌%'] = detail['涨跌%'].round(2)
                detail['昨日热度'] = detail['昨日热度'].round(1)
                detail['今日热度'] = detail['今日热度'].round(1)
                st.dataframe(detail, width='stretch', height=300)

                # 信号变化高亮
                sig_changes = sym_data[sym_data['signal'].shift(1) != sym_data['signal']].dropna()
                if not sig_changes.empty:
                    st.markdown("**信号切换点 (signal 发生变化)**")
                    change_view = sig_changes[['date', 'heat_prev', 'heat_pct', 'signal', 'ret_pct']].copy()
                    change_view['heat_change'] = (change_view['heat_pct'] - change_view['heat_prev']).round(1)
                    change_view.columns = ['日期', '昨日热度', '今日热度', '新信号', '当日涨跌%', '热度变化']
                    change_view['当日涨跌%'] = change_view['当日涨跌%'].round(2)
                    st.dataframe(change_view, width='stretch')

                # 跳转到 K 线
                if st.button("📊 查看该股完整 K 线", key='heat_sym_btn'):
                    st.query_params.update(page="📊 个股K线", symbol=sym_input)
                    st.rerun()

    with tab_bt:
        st.subheader("🧪 回测验证: 历史信号表现")
        st.caption("""
每天访问本页时会自动同步今天的轮动数据到 ~/stock_data/heat_rotation_daily.parquet (幂等)。
回测: 历史被标记为 heating/cooling/staying 的票, 次日 (T+1) 平均涨跌幅。
- 数据需累积几天才有意义, 第一次访问后第 2 天才会有 next_ret
- 不同阈值会分别保存 (window_days + hot_th + cold_th 是 dedup key)
""")

        # 同步按钮 (显式, 给用户控制权)
        col_sync, col_status = st.columns([1, 3])
        with col_sync:
            if st.button("🔄 同步今天数据", key='heat_sync_btn'):
                st.cache_data.clear()
                new_n, sync_today = append_heat_rotation_today(window_days, hot_th, cold_th)
                if new_n == 0:
                    st.info(f"今日 {sync_today} 已存在 ({sync_today})")
                else:
                    st.success(f"✅ 新增 {new_n} 条 ({sync_today})")
                st.rerun()
        with col_status:
            if os.path.exists(HEAT_PATH):
                st.caption(f"📁 {HEAT_PATH} ({os.path.getsize(HEAT_PATH)//1024} KB)")
            else:
                st.caption("📁 尚未生成")

        bt_df = load_heat_rotation_backtest(window_days, hot_th, cold_th)
        if bt_df.empty:
            st.warning("无回测数据, 请先点上方 '🔄 同步今天数据', 第二天再来看")
        else:
            st.divider()
            st.markdown(f"**回测样本: {len(bt_df)} 条历史信号记录, "
                        f"覆盖 {bt_df['date'].nunique()} 个交易日**")
            valid = bt_df.dropna(subset=['next_ret'])
            if valid.empty:
                st.info("需要至少累积 2 天数据才能算 next_ret (今日信号的次日表现需等明天才能验证)")
            else:
                st.markdown("**次日表现 (T+1 ret_pct) 按信号分组**")
                stats = valid.groupby('signal').agg(
                    n=('next_ret', 'count'),
                    mean_ret=('next_ret', 'mean'),
                    median_ret=('next_ret', 'median'),
                    win_rate=('next_ret', lambda s: (s > 0).mean() * 100),
                    std=('next_ret', 'std'),
                ).reset_index()
                stats['mean_ret'] = stats['mean_ret'].round(2)
                stats['median_ret'] = stats['median_ret'].round(2)
                stats['win_rate'] = stats['win_rate'].round(1)
                stats['std'] = stats['std'].round(2)
                stats.columns = ['信号', '样本数', '平均涨幅%', '中位涨幅%', '胜率%', '波动%']
                st.dataframe(stats, width='stretch')

                # 详细分布 - 看 heating 的次日 ret 分布
                st.markdown("**heating 信号次日涨幅分布**")
                heating_next = valid[valid['signal'] == 'heating']['next_ret']
                if len(heating_next) > 0:
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("样本数", len(heating_next))
                    c2.metric("平均", f"{heating_next.mean():.2f}%")
                    c3.metric("胜率", f"{(heating_next > 0).mean() * 100:.1f}%")
                    c4.metric("最大", f"{heating_next.max():.2f}%")
                    st.bar_chart(heating_next, height=200)
                else:
                    st.info("尚无 heating 信号样本")


# ============================================================
# 页面 11: SQL 控制台 (2026-06-19 整合自老 app.py)
# stock_basic 是静态基本面 (53 列), 动态指标需从 kdata 算
# ============================================================
elif page == "💻 SQL 控制台":
    st.header("💻 SQL 控制台")
    page_help("💻 SQL 控制台")
    st.caption("""
可用视图/表:
- `kdata` (TABLE, 16M+ 行: symbol, date, open, high, low, close, volume, amount)
- `asi_yearly` / `asi_yearly_up` (VIEW: year, asi_sum, asi_yearly_rank, top50_days, ...)
- `stock_basic` (VIEW: 53 列静态基本面: 代码/名称/细分行业/地区/上市日期/市净率/市销率/市现率/净利润率/毛利率/营业利润率/净利润率/收入同比/利润同比/总股本_亿/B/A股_亿/...)
- `stock_slim` (TABLE: symbol/name/industry/region/listing_date, 物化加速用)
""")
    default_sql = """-- 茅台/宁德/比亚迪 财务对比 (静态基本面 + 当日行情)
SELECT
    b.代码, b.名称, b.细分行业 AS 行业, b.地区,
    ROUND(t.close, 2)        AS 收盘,
    ROUND(b.市净率, 2)       AS PB,
    ROUND(b.市销率, 2)       AS PS,
    ROUND(b.市现率, 2)       AS PCF,
    ROUND(b.净利润率, 2)       AS ROE,
    ROUND(b.毛利率, 1)       AS 毛利率,
    ROUND(b.营业利润率, 1)   AS 营业利润率,
    ROUND(b.净利润率, 1)     AS 净利率,
    ROUND(b.收入同比, 1)     AS 营收同比,
    ROUND(b.利润同比, 1)     AS 净利同比
FROM stock_basic b
JOIN (SELECT * FROM kdata WHERE date = (SELECT MAX(date) FROM kdata)) t
  ON b.代码 = t.symbol
WHERE b.代码 IN ('600519','300750','002594')
ORDER BY b.代码;"""
    sql = st.text_area("SQL", value=default_sql, height=300, key='sql_console')

    if st.button("执行 ▶", type="primary", key='sql_run'):
        con = get_con()
        try:
            t0 = time.time()
            df = con.execute(sql).df()
            ms = (time.time() - t0) * 1000
            st.success(f"✅ {len(df)} 行, {ms:.0f} ms")
            st.dataframe(df, width='stretch')
            if not df.empty:
                st.download_button("下载 CSV", df.to_csv(index=False).encode("utf-8-sig"),
                                   "result.csv", "text/csv", key='sql_dl')
        except Exception as e:
            st.error(f"❌ {e}")


st.caption("--- 数据源: ~/stock_data/*.parquet | Powered by Streamlit + DuckDB + Plotly")
