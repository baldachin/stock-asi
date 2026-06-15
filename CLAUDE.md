# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

A股数据分析和ASI（成交额强度指标）计算系统。数据从腾讯证券和Baostock获取，存储为Parquet格式，计算年度ASI排名。

## 数据文件 (~/stock_data/)

- `kdata.parquet` - K线数据主文件 (symbol, date, open, high, low, close, volume, amount)，约1.2GB，16M+ 行，1M 行/row group
- `asi_yearly.parquet` - 预计算的年度ASI得分 (全交易日口径)
- `asi_yearly_up.parquet` - 预计算的年度ASI得分 (仅上涨日口径)
- `stock_basic.parquet` - A股股票列表和基本信息 (~5500 只)

**所有数据文件都用 snappy 压缩, pyarrow write。Writer 用 `parquet_atomic.write_atomic()` 写到 .tmp → fsync → os.replace 原子替换。**

历史包袱:
- 旧 DuckDB 文件 `stock.db` (1.3GB) 暂时保留作后悔窗口, 2026-06-12 自动清理 (已设 cron)
- 旧 Parquet `~/stock/stock_basic.parquet` 路径已废弃, 改用 `~/stock_data/stock_basic.parquet`

## 核心脚本

### 数据更新
- **`update_kdata_safe.py`** - 增量更新主脚本 (已废弃，被 Parquet 版替代)。从腾讯证券API (`web.ifzq.gtimg.cn`) 抓取K线，20并发线程。只读取parquet元数据获取最新日期，不加载全量数据。
- **`merge_incremental.py`** - 将增量数据合并到主文件。使用pyarrow row group过滤避免全量加载。
- **`rebuild_kdata.py`** - 从Baostock全量重建。用于数据损坏时。每批处理300只股票。
- **`update_kdata_parquet.py`** - Parquet 版增量更新 (活跃)。流式重写 `kdata.parquet` (裁掉最后 30 天 + 加新数据)，原子 rename。零锁。

### ASI计算
- **`asi_calculator.py`** - 从 `kdata.parquet` 计算年度ASI得分 (已废弃，保留做参考)。公式：`ln(max_rank + 1 - rank) / ln(max_rank + 1) × 100`，按日计算后年度聚合。
- **`update_asi_notion.py`** - 计算当年ASI Top50并同步到Notion。归档旧记录后创建新记录。
- **`asi_calculator_parquet.py`** - Parquet 版 ASI 计算 (活跃)。`calculate_asi(year, up_only, weighted)`，用 DuckDB in-memory 读 Parquet，写 Parquet (原子)。

### 工具
- **`parquet_atomic.py`** - `write_atomic(table, path)` helper，写 .tmp + fsync + os.replace
- **`migrate_to_parquet.py`** - DuckDB → Parquet 一次性迁移脚本 (历史工具)

## 架构要点

- **Parquet row groups**: 每个row group约1M行，date作为普通string列。直接读取row group元数据获取日期范围，避免加载全量数据。
- **数据文件 (4 个 Parquet)**：`~/stock_data/kdata.parquet` (1.2GB, 16M+ 行) + `asi_yearly.parquet` (4MB) + `asi_yearly_up.parquet` (同结构) + `stock_basic.parquet` (1MB)。所有脚本 (writer, asi, dashboard) 都读这 4 个文件。
- **Dashboard 引擎**: 用 DuckDB in-memory 模式 + `read_parquet` 把 4 个文件注册为 VIEW, 然后用 SQL 查询。**完全没有 .db 文件, 没有锁问题**。
- **Writer 引擎**: 用 `pyarrow.ParquetWriter` 流式重写 (逐 row group 读老文件, 过滤, 写新文件, 原子 rename)。
- **并发抓取**: `update_kdata_parquet.py` 使用单线程顺序抓 (50 只/批)，每只 sleep 0.05s 避免请求过快。如果将来要并发, 用 ThreadPoolExecutor + fcntl flock 单例锁。
- **ASI评分**: 按成交额每日排名，得分0-100。年度聚合包括得分总和、top50/top100天数统计。v2 版本用 `tanh(ret_pct / K)` 加权 (K=3.0, 上涨加分下跌扣分)。
- **Notion同步**: API有速率限制 (429时重试)，列表查询用 data_source API，创建用直接 API。
- **数据源**: K线数据来自腾讯证券，成交量额来自通达信 (TDX) 导出的前复权数据。腾讯证券API不返回成交额。

## ✅ 零锁架构 (2026-06-05 改造)

**核心设计**: 所有数据都是 Parquet 文件, writer 用 `os.replace()` 原子替换。

**优势**:
- 写失败 → 旧文件 100% 完好 (`.tmp` 文件留着, 主文件不动)
- dashboard 读文件时, 持 fd 读旧数据, 不报错
- 任何数量的 dashboard + writer + reader 可以同时跑
- 备份只需 `cp kdata.parquet backup/` (rsync 友好)

**与之前 DuckDB 方案对比**:
- ❌ DuckDB 1.5.x 文件锁互斥 → dashboard 持锁 → writer 撞锁
- ✅ Parquet 文件无锁, 任意并发

**Writer 内存峰值**: 16M 行全量重写 ~ 1.2GB 内存峰值 (流式 ParquetWriter)。机器可用内存 < 2GB 时建议先停 dashboard (释放 ~600MB)。

**Writer 耗时**: 全量重写 kdata.parquet ~10-15 秒 (1.2GB snappy 写)。Baostock 抓取 22 分钟 (单线程顺序)。

**已知边界**:
- 裁掉最后 30 天: writer 跑前会丢最后 30 天的数据 (CROP_DAYS=30), 用新数据覆盖
- 如抓取失败, 最后 30 天会是旧的 (下一次 writer 修复)
- 不支持"任意日期"精确合并, 只能"裁 + 加"

**手动跑 writer 不再需要停 dashboard**:
```bash
~/stock/.venv/bin/python ~/stock/update_kdata_parquet.py   # 任何时候跑都行
```

## 已知数据问题

- 2026-05-06~20 共 11 个交易日历史曾整段缺失 (writer CROP_DAYS=30 裁剪后未被新数据覆盖)，已于 2026-06-08 从旧 stock.db 后悔窗口回填 — 见 `backfill_may_2026.py`。回填逻辑：从旧 db 导出 5/6-5/20，过滤停牌股 (open IS NULL) 246 行 (与 parquet 5/21+ 风格一致) → merge 写入 kdata.parquet (56,760 行, amount 字段全有) → 原子替换。备份：`~/stock_data/kdata.parquet.pre_may_backfill`。
- 腾讯证券 API 不返回成交额字段。所有 amount 数据来自 Baostock (query_history_k_data_plus adjustflag='2') 和历史 TDX 导入。
- 全表仍有少量遗留 NULL/0：约 11,608 行 `open IS NULL` (1990s-2000s 历史停牌股)、约 268 行 `amount=0/空` (1990-12 到 1995-06 的早期数据)，均不在本次修复范围。

## 常用命令

```bash
# 增量更新K线数据 (每日 cron 自动跑：周一~周五 17:00, 写 ~/stock_data/kdata.parquet)
~/stock/.venv/bin/python ~/stock/update_kdata_parquet.py

# 一次性回填 5月缺失窗口 (2026-05-06 ~ 2026-05-20, 从旧 stock.db 捞)
~/stock/.venv/bin/python ~/stock/backfill_may_2026.py

# 重新计算年度ASI得分 (两套口径, 写 Parquet)
~/stock/.venv/bin/python ~/stock/asi_calculator_parquet.py              # 全交易日 (asi_yearly.parquet)
~/stock/.venv/bin/python ~/stock/asi_calculator_parquet.py 2026 --up    # 仅上涨日 (asi_yearly_up.parquet)

# 启动Streamlit面板 (dashboard 用 DuckDB in-memory 读 4 个 Parquet)
~/stock/.venv/bin/streamlit run ~/stock/dashboard.py --server.port 8501

# 更新Notion Top50
~/stock/.venv/bin/python ~/stock/update_asi_notion.py

# 导出ASI Top50到CSV
~/stock/.venv/bin/python ~/stock/export_asi_top.py
```

## 依赖

- `baostock` - Baostock股票列表和历史数据
- `pandas`, `pyarrow` - 数据处理和Parquet存储
- `numpy` - 数值计算
- `duckdb` (in-memory only, 不再创建 .db 文件) - dashboard/asi_calculator 的 SQL 引擎
- `psutil` - 内存监控 (开发期使用)
- 无测试套件
