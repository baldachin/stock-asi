#!/usr/bin/env python3
"""合并 kdata_incremental.parquet 到 kdata.parquet
   主文件 date 列是 string，增量文件 date 列是 timestamp
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
# 增量日期转为 string 以便与主文件一致
inc_dates_set = set(str(d.date()) if hasattr(d, 'date') else str(d) for d in inc_dates)
print(f"  增量 {inc.num_rows} 行, 覆盖 {len(inc_dates_set)} 个交易日")

# 增量中每个 date 对应的 symbol set
inc_df = inc.to_pandas()
inc_df['date'] = pd.to_datetime(inc_df['date'])
inc_df['date_str'] = inc_df['date'].dt.strftime('%Y-%m-%d')
inc_symbol_per_date = inc_df.groupby('date_str')['symbol'].apply(set).to_dict()

# 扫描主文件 row groups
print("扫描主文件 row groups...")
mainpf = pq.ParquetFile(KDATA)
rg_metadata = []
for i in range(mainpf.metadata.num_row_groups):
    rg = mainpf.metadata.row_group(i)
    date_col = None
    for j in range(rg.num_columns):
        c = rg.column(j)
        if c.path_in_schema == 'date':
            date_col = c
            break
    if date_col is None:
        rg_metadata.append((i, rg.num_rows, None, None))
    else:
        rg_metadata.append((i, rg.num_rows, date_col.statistics.min, date_col.statistics.max))

# 收集需要保留的 row groups 和需要拆分的
keep_rgs = []
split_rgs = []  # (rg_idx, min_date, max_date, rg_rows)

for rg_idx, rg_rows, min_date, max_date in rg_metadata:
    if min_date is None:
        keep_rgs.append(rg_idx)
        continue
    # 如果 rg 的日期范围与增量有交集
    overlap = False
    for d in inc_dates_set:
        if min_date <= d <= max_date:
            overlap = True
            break
    if overlap:
        split_rgs.append((rg_idx, min_date, max_date, rg_rows))
    else:
        keep_rgs.append(rg_idx)

print(f"  保留 {len(keep_rgs)} 个 row groups")
print(f"  需拆分 {len(split_rgs)} 个 row groups")

# 获取主文件 schema（统一使用）
main_schema = mainpf.schema_arrow
# 确保增量 date 也转为 string 以匹配 schema
inc_df['date'] = inc_df['date'].dt.strftime('%Y-%m-%d')
inc_df_dropped = inc_df.drop(columns=['date_str'])
inc_table = pa.Table.from_pandas(inc_df_dropped, preserve_index=False)

# 统一 schema：所有表的 schema 必须一致
all_schemas_equal = (
    main_schema.equals(inc_table.schema) or
    all(main_schema.equals(inc_table.schema) for _ in [1])
)
# 实际上只保证 main_schema 用于写文件，增量 table schema 可能不同，做 cast
inc_table = inc_table.cast(main_schema)

# 分批读写
print("写新文件...")
writer = None
rows_written = 0

# 处理需拆分的 rgs
for rg_idx, min_date, max_date, rg_rows in sorted(split_rgs):
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
    mask = df.apply(lambda r: (r['date'].strftime('%Y-%m-%d'), r['symbol']) not in symbols_to_remove, axis=1)
    df_filtered = df[mask]
    df_filtered['date'] = df_filtered['date'].dt.strftime('%Y-%m-%d')
    table_filtered = pa.Table.from_pandas(df_filtered, preserve_index=False).cast(main_schema)
    del table, df, df_filtered
    gc.collect()
    
    if writer is None:
        writer = pq.ParquetWriter(OUT, main_schema, compression='snappy')
    writer.write_table(table_filtered)
    rows_written += table_filtered.num_rows
    del table_filtered
    gc.collect()
    print(f"  rg {rg_idx}: 写入完成")

# 写保留的 rgs
for rg_idx in keep_rgs:
    table = mainpf.read_row_group(rg_idx)
    if writer is None:
        writer = pq.ParquetWriter(OUT, main_schema, compression='snappy')
    writer.write_table(table)
    rows_written += table.num_rows
    del table
    gc.collect()

# 追加增量
print("追加增量数据...")
writer.write_table(inc_table)
rows_written += inc_table.num_rows
writer.close()
del writer, inc_table
gc.collect()

# 验证
final = pq.read_table(OUT)
print(f"\n最终: {final.num_rows:,} 行")
first_date = final.column('date')[0].as_py()
last_date = final.column('date')[final.num_rows-1].as_py()
print(f"日期: {first_date} ~ {last_date}")

# 备份旧文件，替换
os.rename(KDATA, KDATA + '.bak2')
os.rename(OUT, KDATA)
print("完成！")
