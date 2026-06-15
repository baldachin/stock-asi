#!/usr/bin/env python3
"""
创建 stock.db 表结构
"""
import duckdb

DB = '/home/hanshuang8902/stock_data/stock.db'

conn = duckdb.connect(DB)
conn.execute("CREATE SEQUENCE IF NOT EXISTS kdata_id_seq START 1")

# K线数据表
conn.execute("""
CREATE TABLE IF NOT EXISTS kdata (
    id          BIGINT DEFAULT nextval('kdata_id_seq'),
    symbol      VARCHAR NOT NULL,
    date        DATE NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      BIGINT,
    amount      DOUBLE,
    PRIMARY KEY (symbol, date)
)
""")

# ASI年度得分表
conn.execute("""
CREATE TABLE IF NOT EXISTS asi_yearly (
    symbol          VARCHAR,
    year            INTEGER,
    asi_sum         DOUBLE,
    asi_mean        DOUBLE,
    asi_std         DOUBLE,
    asi_trading_days BIGINT,
    asi_best_rank   DOUBLE,
    asi_avg_rank    DOUBLE,
    top50_days      BIGINT,
    top100_days     BIGINT,
    asi_score_ratio DOUBLE,
    asi_yearly_rank BIGINT,
    PRIMARY KEY (symbol, year)
)
""")

# 股票基本信息表
conn.execute("""
CREATE TABLE IF NOT EXISTS stock_basic (
    代码          VARCHAR PRIMARY KEY,
    名称          VARCHAR,
    细分行业      VARCHAR,
    地区          VARCHAR,
    上市日期      DATE,
    总股本_亿     DOUBLE,
    B股_A股_亿    DOUBLE,
    H股_亿        DOUBLE,
    总资产_亿     DOUBLE,
    净资产_亿     DOUBLE,
    少数股权_亿   DOUBLE,
    资产负债率    DOUBLE,
    流动资产_亿   DOUBLE,
    固定资产_亿   DOUBLE,
    无形资产_亿   DOUBLE,
    流动负债_亿   DOUBLE,
    货币资金_亿   DOUBLE,
    存货_亿       DOUBLE,
    应收账款_亿   DOUBLE,
    合同负债_亿   DOUBLE,
    资本公积金_亿 DOUBLE,
    营业收入_亿   DOUBLE,
    营业成本_亿   DOUBLE,
    营业利润_亿   DOUBLE,
    投资收益_亿   DOUBLE,
    利润总额_亿   DOUBLE,
    税后利润_亿   DOUBLE,
    净利润_亿     DOUBLE,
    扣非净利润_亿 DOUBLE,
    未分利润_亿   DOUBLE,
    经营现金流_亿 DOUBLE,
    总现金流_亿   DOUBLE,
    股东人数      DOUBLE,
    人均持股      DOUBLE,
    人均市值      DOUBLE,
    利润同比      DOUBLE,
    收入同比      DOUBLE,
    市净率        DOUBLE,
    市现率        DOUBLE,
    市销率        DOUBLE,
    股息率        DOUBLE,
    每股收益      DOUBLE,
    每股净资      DOUBLE,
    每股公积      DOUBLE,
    每股未分配    DOUBLE,
    每股现金流    DOUBLE,
    权益比        DOUBLE,
    净益率        DOUBLE,
    毛利率        DOUBLE,
    营业利润率    DOUBLE,
    净利润率      DOUBLE,
    研发费用_亿   DOUBLE,
    员工人数      DOUBLE
)
""")

# 创建索引
conn.execute("CREATE INDEX IF NOT EXISTS idx_kdata_symbol ON kdata(symbol)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_kdata_date ON kdata(date)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_asi_symbol_year ON asi_yearly(symbol, year)")

print("表结构创建完成")
conn.close()
