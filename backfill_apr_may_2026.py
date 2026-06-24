#!/usr/bin/env python3
"""
手动回填 4/20~6/2 缺失数据 (从 pre_may_backfill 备份)

背景 (2026-06-23 诊断):
- writer bug: CROP_DAYS=30 > DAYS_BACK=15, 每次 cron 永久丢 15 天
- 累积到 6/19 已丢 4/20~5/6 和 5/8~6/3 两段数据
- 6/3 当前有数据 (5162 行), 保留不覆盖
- pre_may_backfill 备份是 6/8 17:37 (writer 回填 5/6~5/8 之前), 含完整 4/20~6/3

策略: pyarrow 流式处理, 内存峰值 < 100MB
- 流式读 pre_may_backfill 的 4/20~6/2, 直接写新文件 (append)
- 流式读 current kdata.parquet, 逐 row group 过滤 (< 4/20 或 > 6/2) 写新文件

用法:
    ~/stock/.venv/bin/python ~/stock/backfill_apr_may_2026.py
"""
import sys
import os
import time
import pyarrow.parquet as pq

# ========== 配置 ==========
BACKUP_PATH = '/home/hanshuang8902/stock_data/kdata.parquet.pre_may_backfill'
CURRENT_PATH = '/home/hanshuang8902/stock_data/kdata.parquet'
TMP_PATH = '/home/hanshuang8902/stock_data/kdata.backfill_tmp.parquet'
MISSING_FROM = '2026-04-20'
MISSING_TO   = '2026-06-02'


def filter_and_append_current_before(writer, current_pf, before_date):
    """从 current 流式读, 过滤 < before_date, append 到 writer"""
    import pyarrow as pa
    import pyarrow.compute as pc
    from datetime import date as _date
    before_ts = _date.fromisoformat(before_date)
    kept = 0
    for rg_idx in range(current_pf.num_row_groups):
        rg = current_pf.read_row_group(rg_idx)
        date_col = rg.column('date')
        mask = pc.less(date_col, before_ts)
        rg_filtered = rg.filter(mask)
        if len(rg_filtered) > 0:
            writer.write_table(rg_filtered)
            kept += len(rg_filtered)
        if (rg_idx + 1) % 5 == 0:
            print(f'    current(before) row_group {rg_idx+1}/{current_pf.num_row_groups} (kept {kept:,})', flush=True)
    return kept


def filter_and_append_current_after(writer, current_pf, after_date):
    """从 current 流式读, 过滤 > after_date, append 到 writer"""
    import pyarrow as pa
    import pyarrow.compute as pc
    from datetime import date as _date
    after_ts = _date.fromisoformat(after_date)
    kept = 0
    for rg_idx in range(current_pf.num_row_groups):
        rg = current_pf.read_row_group(rg_idx)
        date_col = rg.column('date')
        mask = pc.greater(date_col, after_ts)
        rg_filtered = rg.filter(mask)
        if len(rg_filtered) > 0:
            writer.write_table(rg_filtered)
            kept += len(rg_filtered)
        if (rg_idx + 1) % 5 == 0:
            print(f'    current(after) row_group {rg_idx+1}/{current_pf.num_row_groups} (kept {kept:,})', flush=True)
    return kept


def get_max_id_from_parquet_writer(writer):
    """从 writer 状态拿 max id (通过读取未关闭的 file footer).
    pyarrow 的 ParquetWriter 没有直接接口, 用 duckdb 读 tmp 文件."""
    import duckdb
    if not os.path.exists(TMP_PATH):
        return 0
    con = duckdb.connect(':memory:')
    # 取 max(id), 但 TMP_PATH 可能正在被写, 用 read_parquet 应该是无锁读已写入部分
    # 简化: 直接从 original CURRENT_PATH 读 max(id) (因为前 step 还没写新 id 到 backup, backup id 我们自己分配)
    r = con.execute(f"SELECT COALESCE(MAX(id), 0) FROM read_parquet('{CURRENT_PATH}')").fetchone()
    return r[0]


def filter_and_append_backup(writer, backup_pf, from_date, to_date, id_start):
    """从 backup 流式读, 过滤 from~to 日期, 补 id 列, append 到 writer.
    返回 (kept_rows, last_id)"""
    import pyarrow as pa
    import pyarrow.compute as pc
    from datetime import date as _date
    from_ts = _date.fromisoformat(from_date)
    to_ts = _date.fromisoformat(to_date)
    kept = 0
    next_id = id_start
    for rg_idx in range(backup_pf.num_row_groups):
        rg = backup_pf.read_row_group(rg_idx, columns=['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        date_col = rg.column('date')
        mask = pc.and_(pc.greater_equal(date_col, from_ts), pc.less_equal(date_col, to_ts))
        rg_filtered = rg.filter(mask)
        if len(rg_filtered) > 0:
            # 补 id 列并按目标 schema 重排列顺序 (current 是 id, symbol, date, open, ...)
            n = len(rg_filtered)
            id_arr = pa.array(range(next_id, next_id + n), type=pa.int64())
            # backup 的列顺序可能是 date, symbol, open, ... (无 id), 用 add_column 后再 select 重排
            rg_with_id = rg_filtered.add_column(0, 'id', id_arr)
            # 重排列顺序到 [id, symbol, date, open, high, low, close, volume, amount]
            rg_reordered = rg_with_id.select(['id', 'symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
            writer.write_table(rg_reordered)
            next_id += n
            kept += n
        if (rg_idx + 1) % 5 == 0:
            print(f'    backup row_group {rg_idx+1}/{backup_pf.num_row_groups} (kept {kept:,})', flush=True)
    return kept, next_id





def main():
    if not os.path.exists(BACKUP_PATH):
        raise FileNotFoundError(f'备份文件不存在: {BACKUP_PATH}')

    # 用 current kdata 的 schema (含 id 列, 这是最终目标文件的 schema)
    schema = pq.read_schema(CURRENT_PATH)
    print(f'[backfill] schema (from current): {schema}', flush=True)

    backup_pf = pq.ParquetFile(BACKUP_PATH)
    current_pf = pq.ParquetFile(CURRENT_PATH)
    writer = pq.ParquetWriter(TMP_PATH, schema, compression='snappy')

    # 顺序: 老数据 (< 4/20) → backfill 数据 (4/20~6/2, 续接 id) → 新数据 (> 6/2)
    # 注意: filter_and_append_current 是 < before_date OR > after_date 双向过滤
    #       我们要拆成两次调用: 第一次只 < 4/20, 第二次只 > 6/2

    # Step 1: 写 current 的 < 4/20 部分 (老数据)
    print(f'\n[backfill] Step 1: current < 4/20 (老数据)', flush=True)
    t0 = time.time()
    rows1 = filter_and_append_current_before(writer, current_pf, MISSING_FROM)
    print(f'  保留 {rows1:,} 行 ({time.time()-t0:.1f}s)', flush=True)

    # 取 max id 给 backup 用
    max_id = get_max_id_from_parquet_writer(writer)
    print(f'  当前 max_id: {max_id:,}')

    # Step 2: 写 backup 的 4/20~6/2 部分 (id 续接)
    print(f'\n[backfill] Step 2: backup 4/20~6/2 (回填)', flush=True)
    t0 = time.time()
    rows2, last_id = filter_and_append_backup(writer, backup_pf, MISSING_FROM, MISSING_TO, max_id + 1)
    print(f'  保留 {rows2:,} 行 (id {max_id+1}~{last_id}) ({time.time()-t0:.1f}s)', flush=True)

    # Step 3: 写 current 的 > 6/2 部分 (新数据)
    print(f'\n[backfill] Step 3: current > 6/2 (新数据)', flush=True)
    t0 = time.time()
    rows3 = filter_and_append_current_after(writer, current_pf, MISSING_TO)
    print(f'  保留 {rows3:,} 行 ({time.time()-t0:.1f}s)', flush=True)

    writer.close()
    print(f'\n[backfill] writer closed (总 {rows1+rows2+rows3:,} 行)', flush=True)

    # fsync + 原子替换
    sz = os.path.getsize(TMP_PATH) / 1024 / 1024
    print(f'\n[backfill] 原子替换 {CURRENT_PATH} ({sz:.1f} MB)', flush=True)
    fd = os.open(TMP_PATH, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(TMP_PATH, CURRENT_PATH)
    print(f'  ✓ 替换完成', flush=True)

    # 验证
    print(f'\n[backfill] 验证 (4/15~6/8):', flush=True)
    pf = pq.ParquetFile(CURRENT_PATH)
    import duckdb
    con = duckdb.connect(':memory:')
    df_check = con.execute(f"""
        SELECT date, COUNT(*) AS rows, COUNT(DISTINCT symbol) AS stocks
        FROM read_parquet('{CURRENT_PATH}')
        WHERE date BETWEEN '2026-04-15' AND '2026-06-08'
        GROUP BY date ORDER BY date
    """).df()
    print(df_check.to_string(index=False))

    df_total = con.execute(f"""
        SELECT MIN(date) AS min_d, MAX(date) AS max_d,
               COUNT(*) AS rows, COUNT(DISTINCT date) AS days,
               COUNT(DISTINCT symbol) AS stocks
        FROM read_parquet('{CURRENT_PATH}')
    """).df()
    print(f'\n[backfill] 全量统计:', flush=True)
    print(df_total.to_string(index=False))

    print(f'\n[backfill] DONE.', flush=True)


if __name__ == '__main__':
    main()