#!/usr/bin/env python3
"""
补 2026 年 amount 数据（腾讯证券不返回成交额，从 Baostock 补全）
策略: 每批共享一个 Baostock 连接，减少 login/logout 开销
"""
import pandas as pd
import duckdb
import baostock as bs
import time
from datetime import datetime

DB = 'F:/Develops/stock_data/stock.db'
BATCH_SIZE = 50

def get_2026_symbols_with_zero():
    """找出 2026 年 amount=0 的股票"""
    conn = duckdb.connect(DB, read_only=True)
    symbols = conn.execute("""
        SELECT DISTINCT symbol FROM kdata
        WHERE year(date) = 2026 AND (amount = 0 OR amount IS NULL)
    """).fetchall()
    conn.close()
    return [s[0] for s in symbols]

def fetch_batch(batch):
    """一个 Baostock 连接拉完一批股票 2026 年的 amount"""
    results = []
    try:
        lg = bs.login()
        for code in batch:
            bs_code = f"sh.{code}" if code.startswith(('6', '9')) else f"sz.{code}"
            try:
                rs = bs.query_history_k_data_plus(
                    bs_code, 'date,amount', '2026-01-01', '2026-12-31', 'd', adjustflag='2'
                )
                if rs.error_code != '0':
                    continue
                df = rs.get_data()
                if df.empty:
                    continue
                df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
                df = df[df['amount'].notna() & (df['amount'] > 0)]
                if df.empty:
                    continue
                df['symbol'] = code
                df['date'] = pd.to_datetime(df['date']).dt.date
                results.append(df[['date', 'symbol', 'amount']])
            except Exception:
                continue
        bs.logout()
    except Exception:
        pass
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

def upsert_amount(df):
    """ON CONFLICT 更新 amount"""
    if df.empty:
        return 0
    conn = duckdb.connect(DB)
    conn.execute("""
        INSERT INTO kdata (symbol, date, open, high, low, close, volume, amount)
        SELECT symbol, date, 0.0, 0.0, 0.0, 0.0, 0, amount
        FROM df
        ON CONFLICT (symbol, date) DO UPDATE SET amount = excluded.amount
    """)
    conn.close()
    return len(df)

def main():
    t0 = datetime.now()
    symbols = get_2026_symbols_with_zero()
    print(f"2026 年 amount=0 的股票: {len(symbols)} 只")

    batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    total = 0
    updated = 0

    for i, batch in enumerate(batches):
        df = fetch_batch(batch)
        if not df.empty:
            n = upsert_amount(df)
            total += n
        elapsed = (datetime.now() - t0).total_seconds()
        print(f"\r  批次 {i+1}/{len(batches)} | {elapsed:.0f}s | 更新 {total:,} 条", end='', flush=True)

    # 验证
    conn = duckdb.connect(DB, read_only=True)
    zero_left = conn.execute("SELECT COUNT(*) FROM kdata WHERE year(date)=2026 AND (amount=0 OR amount IS NULL)").fetchone()[0]
    conn.close()
    print(f"\n完成! 更新 {total:,} 条, 剩余 amount=0: {zero_left:,}, 耗时 {datetime.now()-t0}")

if __name__ == '__main__':
    main()
