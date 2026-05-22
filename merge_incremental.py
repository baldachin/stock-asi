#!/usr/bin/env python3
"""合并 kdata_incremental.parquet 到 kdata.parquet
   主文件有 date index，增量文件有 date column
   增量日期: 2026-05-12 ~ 2026-05-19，共 31098 行
   主文件最新: 2026-04-24
   策略: 用 pyarrow 过滤主文件中增量覆盖的日期，再用 concat 合并
"""
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import gc, os

KDATA = 'kdata.parquet'
INC   = 'kdata_incremental.parquet'
OUT   = 'kdata_new.parquet'

# 读增量
print("读取增量...")
inc = pq.read_table(INC)
inc_dates = inc.column('date').to_pylist()
inc_dates_set = set(inc_dates)
print(f"  增量 {inc.num_rows} 行, 覆盖 {len(inc_dates_set)} 个交易日")

# 增量中每个 date 对应的 symbol set
inc_df = inc.to_pandas()
inc_df['date'] = pd.to_datetime(inc_df['date'])
inc_symbol_per_date = inc_df.groupby('date')['symbol'].apply(set).to_dict()

# 过滤后的行数（不加载全量，只算 metadata）
print("扫描主文件 row groups...")
mainpf = pq.ParquetFile(KDATA)
rg_metadata = [(i, r.num_rows, r.columns) for i, r in enumerate(mainpf.metadata.row_group_metadata)]

# 收集需要保留的 row groups
keep_rgs = []
remove_dates = set()
rows_removed = 0

for rg_idx, rg_rows, cols in rg_metadata:
    # 取该 row group 的 date min/max
    date_col = None
    for c in cols:
        if c.column_name == 'date':
            date_col = c
            break
    if date_col is None:
        keep_rgs.append(rg_idx)
        continue
    min_date = date_col.statistics.min
    max_date = date_col.statistics.max
    # 如果 rg 的日期范围与增量有交集
    overlap = False
    for d in inc_dates_set:
        if min_date <= d <= max_date:
            overlap = True
            break
    if overlap:
        remove_dates.add((rg_idx, min_date, max_date, rg_rows))
    else:
        keep_rgs.append(rg_idx)

print(f"  保留 {len(keep_rgs)} 个 row groups")
print(f"  需处理 {len(remove_dates)} 个 row groups（与增量日期重叠）")

# 分批读主文件，跳过被增量覆盖的数据
print("写新文件...")
writer = None
rows_written = 0

for rg_idx, min_date, max_date, rg_rows in sorted(remove_dates):
    # 读需要拆分的 rg
    table = mainpf.read_row_group(rg_idx)
    df = table.to_pandas()
    df['date'] = pd.to_datetime(df['date'])
    # 过滤掉增量中存在的 (date, symbol) 对
    symbols_to_remove = set()
    for d in inc_dates_set:
        if min_date <= d <= max_date:
            if d in inc_symbol_per_date:
                for s in inc_symbol_per_date[d]:
                    symbols_to_remove.add((d, s))
    mask = df.apply(lambda r: (r['date'], r['symbol']) not in symbols_to_remove, axis=1)
    df_filtered = df[mask]
    rows_removed += len(df) - len(df_filtered)
    table_filtered = pa.Table.from_pandas(df_filtered)
    del table, df, df_filtered
    gc.collect()
    
    if writer is None:
        writer = pq.ParquetWriter(OUT, table_filtered.schema, compression='snappy')
    writer.write_table(table_filtered)
    rows_written += table_filtered.num_rows
    print(f"  rg {rg_idx}: 过滤 {len(df) - len(df_filtered)} 行, 保留 {len(df_filtered)} 行")
    del table_filtered
    gc.collect()

# 写保留的 rgs
for rg_idx in keep_rgs:
    table = mainpf.read_row_group(rg_idx)
    if writer is None:
        writer = pq.ParquetWriter(OUT, table.schema, compression='snappy')
    writer.write_table(table)
    rows_written += table.num_rows
    del table
    gc.collect()

writer.close()

# 追加增量
print("追加增量数据...")
inc_df = inc.to_pandas()
inc_df['date'] = pd.to_datetime(inc_df['date'])
# 转为 pa table
inc_table = pa.Table.from_pandas(inc_df)
with pq.ParquetWriter(OUT, inc_table.schema, compression='snappy') as writer:
    writer.write_table(inc_table)

# 验证
final = pq.read_table(OUT)
print(f"\n最终: {final.num_rows:,} 行")
print(f"日期: {final.column('date')[0].as_py().date()} ~ {final.column('date')[final.num_rows-1].as_py().date()}")

# 备份旧文件，替换
os.rename(KDATA, KDATA + '.bak2')
os.rename(OUT, KDATA)
print("完成！")
