# stock-asi 本地部署说明 (Windows)

## 环境

- **Python**: 3.13.12 (miniconda3)
- **虚拟环境**: `F:\Develops\stock_asi_venv\`
- **数据目录**: `F:\Develops\stock_data\`
- **Dashboard**: http://localhost:8501

## 安装步骤 (已完成)

1. 创建虚拟环境:
   ```bash
   "D:\Users\BraveYun\miniconda3\python.exe" -m venv F:\Develops\stock_asi_venv
   ```

2. 安装依赖:
   ```bash
   F:\Develops\stock_asi_venv\Scripts\python.exe -m pip install baostock pandas pyarrow numpy duckdb streamlit plotly psutil akshare
   ```

3. 修补 baostock 兼容 pandas 3.x (`resultset.py` 中 `df.append` → `pd.concat`)
4. 修补 parquet_atomic.py 兼容 Windows (`fsync` 在 `O_RDONLY` 句柄不可靠时跳过)
5. 修补 update_kdata_parquet.py 兼容 Windows (`fcntl` 改为可选,Windows 用 `msvcrt.locking`)

## 路径替换

所有 `.py` 文件中的 `/home/hanshuang8902/` 硬编码路径已批量替换为 `F:/Develops/`。
工具脚本: `_patch_paths.py`。

## 数据文件状态

| 文件 | 大小 | 状态 |
|---|---|---|
| stock_basic.parquet | 1.0 MB | ✓ 已就绪 (5515 只股票) |
| asi_yearly.parquet | 4.0 MB | ✓ 已就绪 (76815 行, 列已重命名: top50_sum→top50_days) |
| asi_yearly_up.parquet | 4.0 MB | ✓ 已就绪 |
| kdata.parquet | ~700 MB | ⏳ 后台下载中 (2024-2026, 5533 只, ~4-5h) |

## 启动 Dashboard

```bash
F:\Develops\stock_asi_venv\Scripts\streamlit.exe run F:\Develops\stock-asi\dashboard.py --server.port 8501
```

## Dashboard 功能可用性

| 页面 | 状态 | 备注 |
|---|---|---|
| 总览 | ✅ 可用 | 读 stock_basic |
| ASI 排名 | ✅ 可用 | 读 asi_yearly, 已有完整历史数据 |
| K线 | ⏳ 待 kdata | 下载完成后可用 |
| RPS 排名 | ⏳ 待 kdata | 下载完成后可用 |
| 行业强度 | ⏳ 待 kdata | 下载完成后可用 |
| 低吸观察池 | ⏳ 待 kdata | 下载完成后可用 |
| 同业对比 | ⏳ 待 kdata | 下载完成后可用 |

## 重新计算 ASI (kdata 下载完成后)

```bash
F:\Develops\stock_asi_venv\Scripts\python.exe F:\Develops\stock-asi\asi_calculator_parquet.py 2026
F:\Develops\stock_asi_venv\Scripts\python.exe F:\Develops\stock-asi\asi_calculator_parquet.py 2026 --up
```

## 增量更新 (cron 场景)

```bash
F:\Develops\stock_asi_venv\Scripts\python.exe F:\Develops\stock-asi\update_kdata_parquet.py
```

## Windows 兼容性补丁

1. **baostock `resultset.py`** (`F:\Develops\stock_asi_venv\Lib\site-packages\baostock\data\resultset.py`):
   - `df.append(temp_df)` → `pd.concat([df, temp_df])` (pandas 2.0+ 移除)
2. **parquet_atomic.py**:
   - `os.fsync(fd)` 在 Windows O_RDONLY 句柄上有时 EBADF, 跳过 fsync (MoveFileEx 仍原子)
3. **update_kdata_parquet.py**:
   - `fcntl.flock` → 跨平台: POSIX 用 fcntl, Windows 用 msvcrt.locking
   - 默认起始日期 1990-01-01 → 2017-01-01 (缩短首次下载时间)

## 路径配置

所有路径现在硬编码为 `F:/Develops/stock_data/` 和 `F:/Develops/stock-asi/`。
修改路径: 编辑相关脚本顶部常量即可。

## 数据下载监控

进度文件: `F:\Develops\stock-asi\_download_progress.txt`
日志文件: `F:\Develops\stock-asi\_download.log`