import pyarrow.parquet as pq, pandas as pd, time
COLS = ['open','high','low','close','volume','amount']

pf = pq.ParquetFile('/home/hanshuang8902/stock/kdata.parquet')
tdx_pf = pq.ParquetFile('/home/hanshuang8902/stock/tdx_parsed.parquet')

df = pf.read_row_group(0).to_pandas().reset_index()
df['symbol'] = df['symbol'].astype(str).str.zfill(6)
df['date'] = df['date'].astype(str).str[:10]
print(f'kdata: {len(df)} 行')

tdx_rg = tdx_pf.read_row_group(0, columns=['symbol','date']+COLS).to_pandas()
tdx_rg['symbol'] = tdx_rg['symbol'].astype(str).str.zfill(6)
tdx_rg['date'] = tdx_rg['date'].astype(str).str[:10]
print(f'tdx: {len(tdx_rg)} 行')

rg_syms = set(df['symbol'].unique())
common = rg_syms & set(tdx_rg['symbol'].unique())
print(f'重叠: {len(common)}')

# 精确 merge 方案
t0 = time.time()
tdx_sub = tdx_rg[tdx_rg['symbol'].isin(common)][['symbol','date']+COLS].copy()
tdx_sub.columns = ['symbol','date'] + [f'new_{c}' for c in COLS]

merged = df.merge(tdx_sub, on=['symbol','date'], how='inner')
print(f'merged: {len(merged)} 行, {time.time()-t0:.1f}s')

for col in COLS:
    merged[col] = merged[f'new_{col}']

df_updated = df.set_index(['symbol','date'])
merged_idx = merged.set_index(['symbol','date'])
df_updated.update(merged_idx)
df_out = df_updated.reset_index()
print(f'update done: {time.time()-t0:.1f}s')
