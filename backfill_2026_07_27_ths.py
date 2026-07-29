"""Backfill 2026-07-27 (周一) 5539 只 K 线 via ths_kline.

注意 ths_kline 当天 7/28 盘中只能返回 9 只 (前几只), 7/27 完整数据是已知.
策略: 抓 7/27 1 天, 跳过 7/28 (盘中, 等 17:00 cron 完事).
"""
import sys
import time
from pathlib import Path
import pandas as pd
import duckdb

sys.path.insert(0, '/home/hanshuang8902/stock')
import update_kdata_parquet as writer_mod
from fetchers import ths_kline

START = time.time()
print(f"[backfill-7-27-ths] start at {time.strftime('%H:%M:%S')}", flush=True)

# 1. 拿 codes
codes = writer_mod.get_all_codes()
print(f"  codes: {len(codes)} 只")

# 2. fetch 7/27 (1 天)
print(f"\n[Step1] 抓 7/27 1 天 × {len(codes)} 只 ...")


def fetch_via_ths(code, start_date, end_date):
    rows = ths_kline.fetch_one(code, 'D', start_date, end_date)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d').dt.date
    df['symbol'] = code
    df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount']]
    for col in ['open', 'high', 'low', 'close', 'amount']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['volume'] = df['volume'].astype('int64')
    return df


all_dfs = []
failed = []
t0 = time.time()

for i, code in enumerate(codes):
    df = fetch_via_ths(code, '20260727', '20260727')
    if not df.empty:
        all_dfs.append(df)
    else:
        failed.append(code)
    if (i + 1) % 200 == 0:
        elapsed = time.time() - t0
        rate = (i + 1) / elapsed
        print(f"  [{i+1}/{len(codes)}] {elapsed:.0f}s | {rate:.0f}只/秒 | {len(all_dfs)} ok, {len(failed)} fail", flush=True)

df_new = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
elapsed = time.time() - t0
print(f"\n  抓取完成: {len(df_new):,} 行, {len(failed)} 失败, {elapsed:.1f}秒 ({len(df_new)/elapsed:.0f} 行/秒)")

# 3. 写盘
if not df_new.empty:
    print(f"\n[Step2] merge_and_write 7/27 ...")
    t0 = time.time()
    writer_mod.merge_and_write(df_new)
    print(f"  merge_and_write 耗时: {time.time()-t0:.1f}秒")

# 4. verify
print(f"\n[Step3] 验证写入")
con = duckdb.connect(':memory:')
df_check = con.execute("""
    SELECT date, COUNT(DISTINCT symbol) AS stocks,
           ROUND(SUM(volume)/1e8, 1) AS vol_yi,
           ROUND(SUM(amount)/1e8, 1) AS amt_yi
    FROM read_parquet('/home/hanshuang8902/stock_data/kdata.parquet')
    WHERE date >= '2026-07-24' AND volume > 0
    GROUP BY date ORDER BY date DESC
""").fetchdf()
print(df_check.to_string())

total = time.time() - START
print(f"\n[backfill-7-27-ths] DONE in {total:.1f}s")
