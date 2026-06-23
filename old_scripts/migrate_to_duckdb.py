#!/usr/bin/env python3
"""
迁移 Parquet 数据到 DuckDB
"""
import duckdb
import pandas as pd
import pyarrow.parquet as pq

DB = '~/stock/stock.db'

conn = duckdb.connect(DB)

# ---- 1. 迁移 kdata ----
print("=== 迁移 kdata ===")
pf = pq.ParquetFile('kdata.parquet')
total = pf.metadata.num_row_groups
for i in range(total):
    df = pf.read_row_group(i).to_pandas()
    # 转换日期类型
    df['date'] = pd.to_datetime(df['date']).dt.date
    # symbol 补零
    df['symbol'] = df['symbol'].astype(str).str.zfill(6)
    # 写入 DuckDB（存在则替换）
    conn.execute("""
        INSERT INTO kdata (symbol, date, open, high, low, close, volume, amount)
        SELECT symbol, date, open, high, low, close, volume, amount
        FROM df
        ON CONFLICT (symbol, date) DO UPDATE SET
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            amount = excluded.amount
    """)
    print(f"  RG {i+1}/{total} 完成，当前行数: {conn.execute('SELECT COUNT(*) FROM kdata').fetchone()[0]:,}")

# ---- 2. 迁移 asi_yearly ----
print("\n=== 迁移 asi_yearly ===")
asi = pd.read_parquet('asi_yearly.parquet')
print(f"  {len(asi):,} 条")
conn.execute("""
    INSERT INTO asi_yearly (symbol, year, asi_sum, asi_mean, asi_std, asi_trading_days,
        asi_best_rank, asi_avg_rank, top50_days, top100_days, asi_score_ratio, asi_yearly_rank)
    SELECT symbol, year, asi_sum, asi_mean, asi_std, asi_trading_days,
           asi_best_rank, asi_avg_rank, top50_sum, top100_sum, asi_score_ratio, asi_yearly_rank
    FROM asi
    ON CONFLICT (symbol, year) DO UPDATE SET
        asi_sum = excluded.asi_sum, asi_mean = excluded.asi_mean, asi_std = excluded.asi_std,
        asi_trading_days = excluded.asi_trading_days, asi_best_rank = excluded.asi_best_rank,
        asi_avg_rank = excluded.asi_avg_rank, top50_days = excluded.top50_days,
        top100_days = excluded.top100_days, asi_score_ratio = excluded.asi_score_ratio,
        asi_yearly_rank = excluded.asi_yearly_rank
""")
print(f"  asi_yearly: {conn.execute('SELECT COUNT(*) FROM asi_yearly').fetchone()[0]:,} 条")

# ---- 3. 迁移 stock_basic ----
print("\n=== 迁移 stock_basic ===")
sb = pd.read_parquet('stock_basic.parquet')
# 转换日期
sb['上市日期'] = pd.to_datetime(sb['上市日期']).dt.date
print(f"  {len(sb):,} 条")
conn.execute("""
    INSERT INTO stock_basic
    SELECT * FROM sb
    ON CONFLICT (代码) DO UPDATE SET
        名称 = excluded.名称, 细分行业 = excluded.细分行业, 地区 = excluded.地区,
        上市日期 = excluded.上市日期, 总股本_亿 = excluded.总股本_亿,
        B股_A股_亿 = excluded.B股_A股_亿, H股_亿 = excluded.H股_亿,
        总资产_亿 = excluded.总资产_亿, 净资产_亿 = excluded.净资产_亿,
        少数股权_亿 = excluded.少数股权_亿, 资产负债率 = excluded.资产负债率,
        流动资产_亿 = excluded.流动资产_亿, 固定资产_亿 = excluded.固定资产_亿,
        无形资产_亿 = excluded.无形资产_亿, 流动负债_亿 = excluded.流动负债_亿,
        货币资金_亿 = excluded.货币资金_亿, 存货_亿 = excluded.存货_亿,
        应收账款_亿 = excluded.应收账款_亿, 合同负债_亿 = excluded.合同负债_亿,
        资本公积金_亿 = excluded.资本公积金_亿, 营业收入_亿 = excluded.营业收入_亿,
        营业成本_亿 = excluded.营业成本_亿, 营业利润_亿 = excluded.营业利润_亿,
        投资收益_亿 = excluded.投资收益_亿, 利润总额_亿 = excluded.利润总额_亿,
        税后利润_亿 = excluded.税后利润_亿, 净利润_亿 = excluded.净利润_亿,
        扣非净利润_亿 = excluded.扣非净利润_亿, 未分利润_亿 = excluded.未分利润_亿,
        经营现金流_亿 = excluded.经营现金流_亿, 总现金流_亿 = excluded.总现金流_亿,
        股东人数 = excluded.股东人数, 人均持股 = excluded.人均持股,
        人均市值 = excluded.人均市值, 利润同比 = excluded.利润同比,
        收入同比 = excluded.收入同比, 市净率 = excluded.市净率,
        市现率 = excluded.市现率, 市销率 = excluded.市销率,
        股息率 = excluded.股息率, 每股收益 = excluded.每股收益,
        每股净资 = excluded.每股净资, 每股公积 = excluded.每股公积,
        每股未分配 = excluded.每股未分配, 每股现金流 = excluded.每股现金流,
        权益比 = excluded.权益比, 净益率 = excluded.净益率,
        毛利率 = excluded.毛利率, 营业利润率 = excluded.营业利润率,
        净利润率 = excluded.净利润率, 研发费用_亿 = excluded.研发费用_亿,
        员工人数 = excluded.员工人数
""")
print(f"  stock_basic: {conn.execute('SELECT COUNT(*) FROM stock_basic').fetchone()[0]:,} 条")

# ---- 验证 ----
print("\n=== 验证 ===")
for tbl, want in [('kdata', 16262403), ('asi_yearly', 76815), ('stock_basic', 5515)]:
    actual = conn.execute(f'SELECT COUNT(*) FROM {tbl}').fetchone()[0]
    status = '✓' if actual > 0 else '✗'
    print(f"  {status} {tbl}: {actual:,} 行")

conn.close()
print("\n迁移完成")
