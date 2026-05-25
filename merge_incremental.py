#!/usr/bin/env python3
"""
合并 kdata_incremental.parquet 到 kdata.parquet
策略:
  1. 读取增量数据（timestamp date）
  2. 收集需覆盖的 (date, symbol) 对
  3. 逐 RG 读取，移除增量中已存在的记录
  4. 追加增量，输出新文件
  5. 备份替换
"""
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np
import gc, os

KDATA = 'kdata.parquet'
INC   = 'kdata_incremental.parquet'
OUT   = 'kdata_new.parquet'

# ---- 1. 读增量 ----
print("读取增量...")
inc = pq.read_table(INC).to_pandas()
inc['date'] = pd.to_datetime(inc['date'])
print(f"  增量 {len(inc)} 行, 日期 {inc['date'].min().date()} ~ {inc['date'].max().date()}, {inc['symbol'].nunique()} 只股票")

# 增量日期集合
inc_dates = set(inc['date'].dt.strftime('%Y-%m-%d'))
# (date_str, symbol) -> 增量行 dict
inc_map = {}
for _, row in inc.iterrows():
    key = (row['date'].strftime('%Y-%m-%d'), str(row['symbol']).zfill(6))
    inc_map[key] = row
print(f"  增量 key 数: {len(inc_map)}")

# ---- 2. 扫描主文件 RG 元数据 ----
print("扫描主文件 RG...")
mainpf = pq.ParquetFile(KDATA)
rg_info = []
for i in range(mainpf.metadata.num_row_groups):
    rg = mainpf.metadata.row_group(i)
    date_col = None
    for j in range(rg.num_columns):
        c = rg.column(j)
        if c.path_in_schema == 'date':
            date_col = c
            break
    if date_col is None:
        min_d = max_d = None
    else:
        min_d = date_col.statistics.min
        max_d = date_col.statistics.max
        # timestamp 可能需要转 string
        if hasattr(min_d, 'strftime'):
            min_d = min_d.strftime('%Y-%m-%d')
        if hasattr(max_d, 'strftime'):
            max_d = max_d.strftime('%Y-%m-%d')
    rg_info.append((i, min_d, max_d, rg.num_rows))

# ---- 3. 决定哪些 RG 需要处理 ----
overlap_rgs = []
inc_dates_sorted = sorted(inc_dates)
min_inc = inc_dates_sorted[0]
max_inc = inc_dates_sorted[-1]
for rg_idx, min_d, max_d, num_rows in rg_info:
    if min_d is None:
        continue
    # 快速过滤：RG date 范围与增量完全无交集则跳过
    if max_d < min_inc or min_d > max_inc:
        continue
    overlap_rgs.append(rg_idx)

print(f"  总 RG: {len(rg_info)}, 需处理: {len(overlap_rgs)}, 保留: {len(rg_info)-len(overlap_rgs)}")

# ---- 4. 逐 RG 处理 ----
print("写新文件...")
writer = None
rows_written = 0
total_removed = 0

# 先写不重叠的 RG
for rg_idx, min_d, max_d, num_rows in rg_info:
    if rg_idx in overlap_rgs:
        continue
    table = mainpf.read_row_group(rg_idx)
    if writer is None:
        writer = pq.ParquetWriter(OUT, mainpf.schema_arrow, compression='snappy')
    writer.write_table(table.cast(mainpf.schema_arrow))
    rows_written += table.num_rows

# 处理重叠的 RG：逐行合并 amount
inc_keys_str = {f"{d}|{s}" for d, s in inc_map.keys()}
# 转为 (key -> amount) 映射用于合并
inc_amount_map = {f"{d}|{str(s).zfill(6)}": v['amount']
                  for (d, s), v in inc_map.items()}

for rg_idx in overlap_rgs:
    table = mainpf.read_row_group(rg_idx)
    df = table.to_pandas()
    df['date_str'] = df['date'].dt.strftime('%Y-%m-%d')
    # 向量化过滤：构建 (date_str, symbol) key 列
    df['key'] = df['date'].dt.strftime('%Y-%m-%d') + '|' + df['symbol'].str.zfill(6)
    mask = ~df['key'].isin(inc_keys_str)
    df_filtered = df[mask].drop(columns=['key', 'date_str'])
    # 增量中有交集的旧记录：提取出来后可删除 df
    old_overlap = df[~mask][['date', 'symbol', 'amount']].copy()
    removed = len(df) - len(df_filtered)
    total_removed += removed
    del table, df
    gc.collect()

    # 用增量的 amount 更新，但保留旧记录的非零 amount
    old_overlap['key'] = old_overlap['date'].dt.strftime('%Y-%m-%d') + '|' + old_overlap['symbol'].str.zfill(6)
    inc_amt = old_overlap['key'].map(inc_amount_map).fillna(0)
    old_overlap['amount'] = np.where(inc_amt != 0, inc_amt, old_overlap['amount'])
    old_overlap = old_overlap.drop(columns=['key'])
    del inc_amt
    gc.collect()

    # 合并：过滤后的 + amount 保留的旧重叠记录
    df_merged = pd.concat([df_filtered, old_overlap], ignore_index=True)
    del df_filtered, old_overlap
    gc.collect()

    table_filtered = pa.Table.from_pandas(df_merged, preserve_index=False).cast(mainpf.schema_arrow)
    del df_merged
    gc.collect()

    if writer is None:
        writer = pq.ParquetWriter(OUT, mainpf.schema_arrow, compression='snappy')
    writer.write_table(table_filtered)
    rows_written += table_filtered.num_rows
    del table_filtered
    gc.collect()
    print(f"  RG {rg_idx}: 移除 {removed} 条，写入 {rows_written} 条累计")

# ---- 5. 追加增量 ----
print(f"追加增量 {len(inc)} 条...")
# 按主文件 schema 的列顺序重排
col_order = [f.name for f in mainpf.schema_arrow]
inc_table = pa.Table.from_pandas(inc, preserve_index=False)
inc_table = inc_table.select(col_order).cast(mainpf.schema_arrow)
writer.write_table(inc_table)
rows_written += inc_table.num_rows
writer.close()
del writer, inc_table
gc.collect()

# ---- 6. 验证 ----
print("验证...")
final_pf = pq.ParquetFile(OUT)
print(f"  最终: {final_pf.metadata.num_rows:,} 行, {final_pf.metadata.num_row_groups} RG")
# 取最后几行
last = final_pf.read_row_group(final_pf.metadata.num_row_groups - 1).to_pandas()
print(f"  最后日期: {last['date'].max()}")
# 检查增量覆盖的日期
check_dates = ['2026-05-21', '2026-05-22', '2026-05-25']
for d in check_dates:
    cnt = sum(1 for k in inc_map if k[0] == d)
    print(f"  {d} 增量 key 数: {cnt}")

# ---- 7. 备份替换 ----
bak = KDATA + '.bak3'
if os.path.exists(bak):
    os.remove(bak)
os.rename(KDATA, bak)
os.rename(OUT, KDATA)
print(f"\n完成！备份: {bak}")
print(f"最终行数: {final_pf.metadata.num_rows:,}")
