# K线增量更新脚本注意事项 (2026-06-03)

## Bug fix in update_kdata_duckdb.py
原 `fetch_one(bs_conn, code, ...)` 函数把 `bs.login()` 的返回值当作连接对象，调用 `bs_conn.query_history_k_data_plus(...)`。但 Baostock 的 `query_*` 函数是**模块级函数**，不是 login 结果对象的方法。`bs.login()` 返回 `ResultData`，没有 `query_history_k_data_plus` 属性，导致 `AttributeError` 被 `except Exception` 静默吞掉，每个 batch 都返回 0 行。

### 症状
- `total_rows` 一直是 0
- `login success!` / `logout success!` 正常打印
- 没有错误信息（被 except 吞了）
- DuckDB 数据看起来"正常"（max date 还在几天前，但实际是上次成功运行留下的）

### 修复
把 `bs_conn.query_history_k_data_plus(...)` 改为 `bs.query_history_k_data_plus(...)`（去掉 `bs_conn.` 前缀）。`bs_conn` 参数保留以兼容 `process_batch` 调用。

## 性能
111 个 batch × ~27s/batch = 约 50 分钟。脚本是单线程顺序处理每个 batch（每个 batch 内顺序处理 50 只股票），每只股票开/关一个 DuckDB 连接。如需加速可改为 ThreadPoolExecutor + 多 Baostock login session 并发。

## 数据完整性
- 2026-06-03（周三）Baostock 未返回数据，可能是收盘后 API 还没更新，明日 cron 跑时再抓
- 历史 amount=0 的问题（CLAUDE.md 中提到的 2026-05-12~19 范围）已被本次 update 通过 ON CONFLICT 覆盖，amount=0 检查返回 0
