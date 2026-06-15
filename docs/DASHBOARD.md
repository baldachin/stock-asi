# A 股 Dashboard 算法说明 (~/stock/dashboard.py)

> 这份文档讲 **dashboard.py 里所有算法** 的设计原理、SQL 模板、参数选择、踩过的坑。给"想理解/修改/扩展 dashboard 的人"读, 不是给 AI agent 的导航(那是 CLAUDE.md 的事)。

---

## 1. 一句话总览

7 个页面 + 1 个 in-memory DuckDB 引擎 + 4 个 Parquet 文件 + 5/10/60 分钟三级缓存 = dashboard 全部。

启动: `~/stock/.venv/bin/streamlit run ~/stock/dashboard.py --server.address 0.0.0.0 --server.port 8501`

---

## 2. 数据流

```
┌────────────────────────┐
│ 4 Parquet 文件 (零锁)  │  ~/stock_data/
│  kdata.parquet  1.2GB  │  - 1990 至今, 1630 万行, 5114 只票
│  asi_yearly.parquet    │  - ASI v2 加权 (K=3.0), 1990-2026
│  asi_yearly_up.parquet │  - ASI v1 仅上涨日, 1990-2026
│  stock_basic.parquet   │  - 股票代码/名称/行业/地区/财务, 5515 行
└────────┬───────────────┘
         │ read_parquet
         ▼
┌────────────────────────┐
│ DuckDB in-memory       │  - 每次新开 (50ms), 用完即弃
│ 4 个 VIEW 映射         │  - kdata / asi_yearly / asi_yearly_up / stock_basic
└────────┬───────────────┘
         │ SQL (含窗口函数)
         ▼
┌────────────────────────┐
│ cached functions       │  - ttl 5/10/60 分钟
│ (7 个核心 + 5 个辅助)  │  - 复用 get_con() 拿新连接
└────────┬───────────────┘
         │ pd.DataFrame
         ▼
┌────────────────────────┐
│ Streamlit 渲染         │  - 7 个页面, 表格 + plotly 图
│                        │  - 跳转: query_params + session_state 双层
└────────────────────────┘
```

**关键**: 写时用 `os.replace()` 原子替换, 读时永远拿到完整文件 → 没有任何 .lock / .wal 之类的状态文件。

---

## 3. 7 个核心 cached 函数

### 3.1 `get_con()` + `safe_query()`

**作用**: 每次新开 DuckDB 内存连接, 注册 4 个 Parquet 为 VIEW。

**为什么不用顶层 con**: 旧版 `duckdb.connect(DB_PATH)` 有文件锁, 与 streamlit 长连接 + writer 撞锁。改 in-memory 后必须**用完即弃**, 否则重新打开会重新加载 1.2GB Parquet.

**SQL 注册模式**:
```python
def get_con():
    con = duckdb.connect(':memory:')  # 启动 ~50ms
    for view, path in [(kdata, ...), (asi, ...), ...]:
        if os.path.exists(path):
            con.execute(f"CREATE VIEW {view} AS SELECT * FROM read_parquet('{path}')")
    return con
```

**注意**: 缺文件不崩, 跳过该 VIEW. 这样某个 Parquet 没生成时, dashboard 仍能启动 (个别页报"表不存在").

### 3.2 `load_kdata_with_asi()` - 单只 K线 + 每日 ASI + 5 周期 RPS

**位置**: 83-155 行
**TTL**: 5 分钟

**3 个 mode** (ASI 公式差异):
- `v2` (推荐): `score = LN(N+1-rank) / LN(N+1) × 100 × (1 + TANH(ret_pct/3.0))`, 范围 0-200
- `up` (老版): 仅上涨日 (close > open) 参与排名
- `v0`: 无加权

**ASI 数学**:
```
单日得分 = 排名强度 × 涨跌幅权重
  排名强度 = LN(当日总票数 + 1 - 当日排名) / LN(当日总票数 + 1) × 100     -- 0-100
  涨跌幅权重 = 1 + TANH(当日涨跌幅% / 3.0)                                  -- 0-2
合起来 0-200
```

**RPS 数学**:
```
ret_N = close / N 天前 close - 1
RPS_N = 100 × PERCENT_RANK() OVER (PARTITION BY date ORDER BY ret_N)   -- 0-100
```

**3 个 CTE 设计**:
1. `market_ranks`: 全市场 RANK → 单只的每日 amount_rank + total_stocks
2. `kdata_range`: 单只 K 线数据
3. `rps_calc`: 5 周期 RPS, 一次性 LAG 5 次 + PERCENT_RANK 5 次

**LAG 窗口外扩**: RPS120 需要至少 120 个交易日历史. `ext_start = start - timedelta(days=125)`, 把查询窗口往前扩 125 天, 保证 LAG(120) 有值.

**坑**: LAG 5 次的 placeholder 顺序要对:
```python
# 6 个 placeholder: [start, end, symbol, start, end, ext_start, end]
# 对应 SQL 里 6 个 ? 位置
```

### 3.3 `load_asi_top()` - 读预计算 ASI Top N

**位置**: 179-202 行
**TTL**: 5 分钟

直接读 `asi_yearly.parquet` (由 `asi_calculator_parquet.py` 离线算好). 排序用预存的 `asi_yearly_rank` 列.

**为什么用预计算**:
- ASI 全市场每天算一次, 数据 16M+ 行 → 实时算 30+ 秒
- 离线算好, dashboard 查表 < 1 秒
- 折中: 数据有 1 天延迟 (asi_calculator cron 每日跑)

**上市满 N 年过滤**: 用 `b.上市日期 <= cutoff` join stock_basic, 排除新股.

### 3.4 `load_rps_top()` - RPS 排名 (即时计算)

**位置**: 204-292 行
**TTL**: 5 分钟

**为什么 RPS 不预计算**: 5 周期组合太多 (5/10/20/60/120), 用户可选不同截止日 → 实时算

**5 周期 LAG 技巧** (避免 5 个相关子查询):
```sql
LAG(k.close, 5)   OVER w AS close_first,
LAG(k.close, 10)  OVER w AS close_second,
LAG(k.close, 20)  OVER w AS close_third,
LAG(k.close, 60)  OVER w AS close_fourth,
LAG(k.close, 120) OVER w AS close_fifth
WINDOW w AS (PARTITION BY k.symbol ORDER BY k.date)
```

**窗口大小**: `int(max_p * 1.6) + 30` 天 (120 周期 × 1.6 + 30 = 222 天, 覆盖交易日 + buffer).

### 3.5 `load_industry_strength()` - 行业强度

**位置**: 352-419 行
**TTL**: 5 分钟

**2 个算法分支** (用户可切):
- **旧版 `weight_by_return=False`**: `avg(pct_rank) * 100`, 0-100, 越活跃越强
- **新版 `weight_by_return=True` (推荐)**: `weighted_strength = avg(pct_rank × 当日涨跌幅%)`
  - 上涨日加分, 下跌日扣分
  - 强度可以负数 (-100 ~ +100)

**up_only**: True 时只看 close>open 的日子, 跳过下跌日 (防恐慌出货噪音).

**HAVING stock_count >= 5**: 排除 < 5 只票的小行业 (单股/双股行业排名意义不大).

**坑**: `b.代码` 用 `read_parquet(?)` 临时加载, 不预注册到 `get_con()`, 减少顶层 con 内存.

### 3.6 `load_peer_compare()` - 同业对比 (归一化净值)

**位置**: 421-436 行
**TTL**: 5 分钟

**算法**: 拉 close → pivot (date × symbol) → 除以第 0 行 (起点=1.0) → melt 回长表.

画图时所有曲线都从 1.0 出发, 直接看相对涨跌.

### 3.7 `compute_dip_pool()` - 低吸观察池

**位置**: 1165-1290 行
**TTL**: 1 小时 (重, 不需频繁刷)

**5 切片**: 详见后文 §5. 详细 SQL 在 stock-dip-pool skill 里.

---

## 4. 7 个页面地图

| 页面 | 核心 SQL / 算法 | 缓存函数 | 关键交互 |
|---|---|---|---|
| 🏠 总览 | `SELECT date, SUM(amount)/1e8 ... LIMIT 30` | safe_query | 4 metric + 30 日成交额图 + 行业/地区分布 |
| 📊 个股K线 | `load_kdata_with_asi` | 5 分钟 | K线 + 量 + 额 + ASI + RPS 5 行子图; ASI mode 切 v1/v2/v0 |
| 🏆 ASI 排名 | `load_asi_top` (读预计算) | 5 分钟 | Top N 表格 + 行业分布饼图 + 跳转 |
| 📈 RPS 排名 | `load_rps_top` (即时) | 5 分钟 | 5 周期 RPS Top N + 5 周期交叉解读 |
| 🎯 低吸观察池 | `compute_dip_pool` | 1 小时 | 5 切片 dataframe + 跳转 K线 |
| 🏭 行业强度 | `load_industry_strength` | 5 分钟 | 旧版/新版算法可切 + 强度色阶 + 跳转同业对比 |
| ⚖️ 同业对比 | `load_peer_compare` | 5 分钟 | 归一化净值多股对比 |

---

## 5. 低吸观察池 (5 切片)

> 完整 SQL 在 `stock-dip-pool` skill 里, 这里只讲算法设计.

**核心思想**: 长期强势 (排除"以前牛但近年已掉队") + 当年高关注 (top100 天数) + 价格阶段性回调.

**"最近 2 年连续强势"过滤** (避免不同市场阶段的票干扰):
```sql
WHERE year IN (2024, 2025) HAVING SUM(above) = 2
```

**5 个切片**:

| 切片 | 条件 | 数量 (2026-06-08) |
|---|---|---|
| v2-1 | top100_days≥30 + draw_60_pct≤-15% | 19 |
| v2-2 | top100_days≥30 + RSI(14)≤30 | 6 |
| v2-5 | top100≥25 + draw_60≥12% + RSI≤40 + ret_20d<0 (4 条件交集) | 12 |
| v2-6 | top100≥25 + draw_250_pct≤-20% (1 年级别回调) | 14 |
| v2-7 | top100≥20 + draw_60≥10% + ret_20d≤-5% + RSI≤50 (放宽) | 15 |

**回撤计算** (60/20/250 日):
```sql
SELECT symbol, MAX(close) AS hi_60
FROM kdata WHERE date >= 'window_start' AND date <= 'last_date'
GROUP BY symbol
-- draw_60_pct = close / hi_60 - 1, * 100
```

**RSI 简化版** (0-100):
```sql
RSI(14) = 100 × SUM(正收益%) / SUM(|收益%|)
```
说明: 不是 Wilder 标准 RSI, 是"涨的日子总涨幅占总波幅的比例". 简化版, 够用.

---

## 6. 跳转机制

**2 种方式并存**:

### 6.1 `st.query_params` 跳页 + 传 symbol
主要跳转方式, 浏览器 URL 可见:
```python
st.query_params.update(page="📊 个股K线", symbol=sel)
st.rerun()
```
读取端: sidebar 顶部 449-454 行, 用完即 `del st.query_params['page']`.

### 6.2 `st.session_state` 传字符串
行业→同业对比传 industry 字符串, 个股→K线传 return_to 路径:
```python
st.session_state['peer_industry_preset'] = ind_jump
st.session_state._current_page = "⚖️ 同业对比"
st.rerun()
```

**为什么不全用 query_params**: 行业名是中文, URL 不友好; return_to 是嵌套路径, query_params 嵌套支持差.

---

## 7. 缓存策略

| 缓存对象 | TTL | 理由 |
|---|---|---|
| 单 symbol 查询 (K线/RPS/Peer) | 5 分钟 | 数据随时可能被 cron 增量更新 |
| 股票基础数据 (basic) | 10 分钟 | 财务数据很少变 |
| 单 symbol yearly (asi) | 10 分钟 | 同上 |
| ASI Top 预计算读取 | 5 分钟 | asi_calculator 每日 17:00 跑 |
| RPS 即时计算 | 5 分钟 | 用户每次切不同截止日都新算 |
| 行业强度 | 5 分钟 | 同上 |
| 低吸观察池 | 1 小时 | 5 切片 SQL 重, 不需频繁刷 |

**缓存失效**:
- 改 SQL/装饰器参数: 缓存不自动失效, 必须重启 streamlit
- 改阈值/年份: 同上
- 用户手动: 侧栏 "🔄 刷新缓存" 按钮 = `st.cache_data.clear() + st.rerun()`

---

## 8. 关键设计决策

### 8.1 零锁架构 (2026-06-05 改造)

**问题**: 旧版用 DuckDB 单文件, 与 streamlit 长连接天然冲突, 表现为:
- writer 跑时 dashboard 卡死
- dashboard 打开时 writer 撞锁
- 要 stop-and-respawn streamlit 守护

**解决**:
- 放弃 DuckDB 单文件, 改 4 个独立 Parquet + DuckDB in-memory
- writer 用 `os.replace()` 原子替换 (写 tmp → fsync → rename)
- dashboard reader 永远拿完整文件 (内核保证原子性)

**代价**: 每次查询新开 in-memory DuckDB (~50ms), 但比之前 dashboard 卡死好太多.

### 8.2 为什么 `asi_yearly` 和 `asi_yearly_up` 两套并存

- `asi_yearly` (v2 加权, K=3.0): 主流, 推荐. 有 `asi_best_rank` / `asi_avg_rank` 列
- `asi_yearly_up` (v1 仅上涨日): 老版, asi_best_rank/avg_rank 缺. 仍有人用, 因为"只看上涨日"逻辑直观

**不打算合并**: v1/v2 哲学不同 (v1 强调"只在强势日参与排名", v2 强调"排名 × 当日方向"), 用户可切. 防御性列选择: `select_cols = [c for c in desired if c in cols]`.

### 8.3 RPS 不预计算

RPS 周期组合多 (5/10/20/60/120), 用户可切截止日 → 实时算.

**性能**: 单 symbol 5 周期 ~200ms, dashboard 查一次 < 1 秒, 可接受.

### 8.4 行业强度 2 个算法并存

- 旧版 (纯成交活跃度): `avg(pct_rank)`, 范围 0-100
- 新版 (涨跌幅加权): `avg(pct_rank × ret_pct)`, 范围约 -100~+100

**新版优势**: 区分"高成交 + 上涨"和"高成交 + 下跌" (后者是恐慌出货, 不算强).

**用户可切**: 行业强度页 1060 行 checkbox.

### 8.5 跳转 2 套机制

见 §6. 主要是 UX 考虑, 不是技术限制.

---

## 9. 常见错误 & 修复

### 9.1 NameError: name 'con' is not defined

**症状**: 总览页报 `NameError: name 'con' is not defined`
**原因**: 旧版用 `duckdb.connect(DB_PATH)` 在顶层, 改 in-memory 后忘了清理引用
**修复**: 全部用 `safe_query()` 或在函数内 `con = get_con()`, 不在顶层持有 con

### 9.2 RPS 早期数据全 NULL

**症状**: K线页 RPS 子图前 120 天一条直线
**原因**: LAG(120) 需要至少 120 个交易日历史, start_date 选太早
**修复**: `load_kdata_with_asi` 里 `ext_start = start - timedelta(days=125)`, 自动外扩

### 9.3 菜单栏两次点击才切换

**症状**: 点 sidebar radio 切页, 第一次没反应, 第二次才切
**原因**: radio 用了 `key=` 配合 `index=` 算默认, 但 streamlit widget state 跟 session_state 不同步
**修复**: radio 用 `key='nav_radio'`, 删除 `page = st.session_state._current_page` 拉回逻辑

### 9.4 缓存不刷新

**症状**: 改了 SQL/阈值, dashboard 还是旧数据
**原因**: `@st.cache_data` 缓存 module-level, 函数定义不变缓存不失效
**修复**: 重启 streamlit 或点侧栏 "🔄 刷新缓存" 按钮

---

## 10. 调参速查

| 想改什么 | 改哪里 | 注意 |
|---|---|---|
| 加新页 | 444 行 PAGES 列表 + 末尾加 elif | sidebar radio 同步 |
| 加 cached 函数 | 44-66 行附近 | 必须有 ttl= |
| 改 ASI K 值 | 89 行 `K = 3.0` | 跟 asi_calculator_parquet.py 28 行一致 |
| 改 RPS 周期 | 204 行默认 `periods=[5,10,20,60,120]` | 大周期需扩查询窗口 |
| 改低吸池切片 | 1165 行起 `compute_dip_pool()` | **改完必须重启** |
| 改缓存 TTL | 装饰器 `@st.cache_data(ttl=X)` | 短=数据新, 长=性能好 |
| 改跳转 | `st.query_params.update(...)` 6 处 | 用完即 del |
| 改行业强度色阶 | 1083-1095 行 `strength_color()` | 5 档: 深绿/浅绿/黄/橙/红 |
| 改 K 线面板行数 | 633-639 行 panels 列表 | 调整 row_heights 比例 |

---

## 11. 验证方法

### 11.1 AppTest 端到端 (无浏览器, 推荐)

```python
from streamlit.testing.v1 import AppTest
at = AppTest.from_file("dashboard.py", default_timeout=60)
at.run()
at.sidebar.radio[0].set_value("🎯 低吸观察池").run()
assert len(at.exception) == 0
print([len(d.value) for d in at.dataframe])  # 应得 5 个数
```

### 11.2 改 SQL 后必跑

1. AppTest 测 exception=0
2. 看对应 dataframe 行数是否符合预期
3. 重启 streamlit, 浏览器实测

### 11.3 维护 Checklist (改任何东西前)

- [ ] 改动是否需要重启 streamlit? (改 SQL/TTL/装饰器 → 是)
- [ ] 改动是否破坏低吸池? (改 kdata 字段/列名 → 跑 backfill 流程)
- [ ] 改动是否需要更新本说明文档? (改算法/加页/调参 → 是)
- [ ] 改动是否需要更新 CLAUDE.md? (改数据流/迁移 → 是)
- [ ] 改动是否需要写新 skill? (新算法/新流程/新工具 → 是)

---

## 12. 相关文件

- ~/stock/dashboard.py (本文件描述的全部代码)
- ~/stock/CLAUDE.md (AI agent 用的项目导航)
- ~/stock/asi_calculator_parquet.py (ASI 算法权威实现, dashboard 依赖)
- ~/stock/update_kdata_parquet.py (writer, 增量更新 kdata)
- ~/stock/parquet_atomic.py (原子写 helper)
- ~/stock/backfill_may_2026.py (5 月数据回填, 一次性)
- ~/stock_data/*.parquet (4 个数据文件)

## 13. 变更历史

- **2026-06-09**: 新增 🎯 低吸观察池 页 (5 切片, "最近 2 年连续强势" 过滤)
- **2026-06-05**: DuckDB → Parquet 零锁架构改造
- **2026-06-04**: 5 月数据回填 (5/6-5/20, 从旧 stock.db 捞 56760 行)
- **更早**: ASI/RPS/行业强度/同业对比页陆续实现
