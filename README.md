# A股数据分析和 ASI 排名系统

A股日线数据管理和成交额强度指标（ASI）年度排名计算。

## 数据

- `stock_basic.parquet` — A股股票列表和基本信息
- `asi_yearly.parquet` — 年度ASI得分

## 核心脚本

### 数据更新
- `update_kdata_safe.py` — 增量更新K线（腾讯证券API，20并发）
- `merge_incremental.py` — 合并增量到主文件
- `rebuild_kdata.py` — 从Baostock全量重建

### ASI计算
- `asi_calculator.py` — 计算年度ASI得分
- `update_asi_notion.py` — 同步Top50到Notion
- `export_asi_top50.py` — 导出CSV

## 使用

```bash
pip install -r requirements.txt
python update_kdata_safe.py   # 增量更新
python merge_incremental.py   # 合并
python asi_calculator.py      # 计算ASI
```

## 依赖

baostock, pandas, pyarrow, numpy
