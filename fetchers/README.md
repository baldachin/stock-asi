fetchers/ - A 股数据 fetcher 集合 (vendor 自 simonlin1212/a-stock-data v3.5.1)
=======================================================================

## 背景

历史数据源是 baostock，每日 17:00 cron 拉取 5537 只股票。
遇到过几次事故：
- 7/13 软失败（login 失败整批 50 只静默丢行）
- 7/27 cron 调度被某次清理弄丢
- TCP 7709 通达信协议在本机网络层被全阻断

调研 4 个候选方案后选定：vendor a-stock-data (https://github.com/simonlin1212/a-stock-data)
的两个核心 fetcher 替代 baostock。

## 模块

### ths_kline.py — 同花顺 K 线 (主源替代 baostock)

来源: a-stock-data Layer 1.4 同花顺备胎 (端点 d.10jqka.com.cn/v6/line/hs_{code}/01/last.js)

特点:
- 字段: [date, open, high, low, close, volume, amount, 振幅, 涨幅, 涨跌额, 换手率]
  (**实测字段顺序**: [date, open, high, low, close, volume, amount, ...], **与 baostock 完全一致**)
  ⚠️ a-stock-data SKILL.md 写的字段顺序是 [date, open, close, high, low, vol, amount],
  实测有出入 (用 600519 7/24 data 反推: idx 2=high=1309.21, idx 3=low=1286.20, idx 4=close=1297.41)
- 频率: 01=日, 11=周, 21=月, 30=1min, 31=5min, 32=15min, 33=30min, 34=60min
- 性能: 200ms/只, 单只联通; 5537 只串行 ~ 18 分钟
- 覆盖: 主板+创业板+科创板+北交所 (实测 100% 覆盖)
- 限速: 0.05s/只 (实测单只 200ms, 不限速)

### tencent_quote.py — 腾讯实时报价 (补 PE/PB/市值)

来源: a-stock-data Layer 1.2 腾讯财经 API (端点 qt.gtimg.cn/q=sh600519)

特点:
- 字段: 19 个关键字段 (name/price/PE_TTM/PB/市值/换手率/涨跌停/量比)
- 性能: 0.3s/100 只批量 (350 只/秒)
- 涵盖: 沪深主板+创业板+科创板+北交所+指数+ETF
- 限速: 0 (腾讯不限速)
- 重要字段含义 (实测 2026-07-28):
  - vals[39] = PE_TTM, vals[46] = PB, vals[44] = 流通市值(亿), vals[45] = 总市值(亿)
  - vals[47] = 涨停价, vals[48] = 跌停价 (主板 ±10%, 创/科 ±20%, 北交所 ±30%)
- 路由: 6 位裸码 5/6/9 + SH_INDEX → sh; 92 → bj; 其他 → sz

## 验证

- /tmp/hermes-verify-fetchers-2026-07-28.py — 10/10 PASS (原型验证)
- /tmp/hermes-verify-fetchers-full-2026-07-28.py — 22/22 PASS (完整体验)

合计 32 个断言覆盖: 字段完整性/数值对账/批量性能/北交所路由/涨跌停计算/集成。

## 集成策略 (下一步, 待用户拍板)

按 user pref #8 (副作用显式), **暂未**自动集成到 update_kdata_parquet.py。
建议路径:
1. 写 ~/stock/fetchers/__init__.py 带 BaostockFallback 类 (ths → tencent → baostock 链)
2. update_kdata_parquet.py::fetch_all_incremental 改调用 fallback 链
3. 跑 1 天完整 backfill 验证数据完整性
4. 跑月度 4 字段财务数据 (用 tencent_quote 一周一次, 补 PE/PB)

## 已知边界

- 5min K 线: 同花顺 last.js 不返回完整 5min 数据 (待替换为其他源)
- ths_kline.fetch_many 5537 只串行 ~ 18 分钟 (vs baostock 22 分钟, 略快但不显著)
  - 优化方向: ThreadPoolExecutor 10 并发 (待同花顺风控面验证)
- tencent_quote 单次受限 ~ 几十只 (HEAD 检查未见限制, 100% 成功覆盖 100 只)
  - 5537 只一次 query URL 长度可能过长 (待测)

## 已知坑 (从 SKILL.md 修正)

- ⚠️ SKILL.md 同花顺字段顺序写错: 实际是 [date, open, high, low, close, vol, amount]
  不是 [date, open, close, high, low, vol, amount]
- a-stock-data 的 SKILL.md 没改正这处, 提了一个 PR patch 在 fetchers/pr_skillmd_field_order.patch

## 依赖

零第三方依赖 (stdlib only): urllib.request, json, time, typing

## 版本

2026-07-28 v3.5.1-vendor
