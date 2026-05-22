# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

A股数据分析和ASI（成交额强度指标）计算系统。数据从腾讯证券和Baostock获取，存储为Parquet格式，计算年度ASI排名。

## 数据文件

- `kdata.parquet` - K线数据主文件（symbol, date, open, high, low, close, volume, amount），约340MB
- `asi_yearly.parquet` - 预计算的年度ASI得分
- `kdata_incremental.parquet` - 待合并的增量更新数据
- `stock_basic.parquet` - A股股票列表和基本信息

## 核心脚本

### 数据更新
- **`update_kdata_safe.py`** - 增量更新主脚本。从腾讯证券API（`web.ifzq.gtimg.cn`）抓取K线，20并发线程。只读取parquet元数据获取最新日期，不加载全量数据。
- **`merge_incremental.py`** - 将增量数据合并到主文件。使用pyarrow row group过滤避免全量加载。
- **`rebuild_kdata.py`** - 从Baostock全量重建。用于数据损坏时。每批处理300只股票。

### ASI计算
- **`asi_calculator.py`** - 从`kdata.parquet`计算年度ASI得分。公式：`ln(max_rank + 1 - rank) / ln(max_rank + 1) × 100`，按日计算后年度聚合。
- **`update_asi_notion.py`** - 计算当年ASI Top50并同步到Notion。归档旧记录后创建新记录。

## 架构要点

- **Parquet row groups**：每个row group约60万行，date作为索引。直接读取row group元数据获取日期范围，避免加载数据。
- **并发抓取**：`update_kdata_safe.py`使用`ThreadPoolExecutor`，20个worker，每个worker最多150个请求。
- **ASI评分**：按成交额每日排名，得分0-100。年度聚合包括得分总和、top50/top100天数统计。
- **Notion同步**：API有速率限制（429时重试），列表查询用data_source API，创建用直接API。

## 常用命令

```bash
# 增量更新K线数据（每日运行）
python update_kdata_safe.py

# 合并增量数据到主文件
python merge_incremental.py

# 全量重建K线数据（数据损坏时）
python rebuild_kdata.py

# 重新计算年度ASI得分
python asi_calculator.py

# 更新Notion Top50
python update_asi_notion.py

# 导出ASI Top50到CSV
python export_asi_top50.py
```

## 依赖

- `baostock` - Baostock股票列表和历史数据
- `pandas`, `pyarrow` - 数据处理和Parquet存储
- `numpy` - 数值计算
- 无测试套件
