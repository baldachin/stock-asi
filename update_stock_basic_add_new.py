#!/usr/bin/env python3
"""
增量更新 stock_basic.parquet — 补 6/5 之后 baostock 新增的 A股
不动已有 5,515 只 (它们有完整 53 列基本面)
只添加 6/5 之后新上市的 ~330 只, 用 baostock 给的 3 列 (代码/名称/上市日期), 其余 50 列 NaN

输出: ~/stock_data/stock_basic.parquet
备份: ~/stock_data/stock_basic.parquet.pre_2026_07_15_add
"""
import sys, os, shutil
sys.path.insert(0, '/home/hanshuang8902/stock')

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import baostock as bs
from datetime import date

BASIC_PATH = '/home/hanshuang8902/stock_data/stock_basic.parquet'
BACKUP_PATH = BASIC_PATH + '.pre_2026_07_15_add'

def main():
    print(f'=== 更新 {BASIC_PATH} (增量补 6/5 之后新上市) ===\n')

    # 1. 备份
    if not os.path.exists(BACKUP_PATH):
        shutil.copy2(BASIC_PATH, BACKUP_PATH)
        print(f'  ✓ 备份: {BACKUP_PATH}')
    else:
        print(f'  备份已存在: {BACKUP_PATH}, 跳过')

    # 2. 读现有 basic, 提取代码列
    con = duckdb.connect(':memory:')
    con.execute(f"CREATE VIEW basic AS SELECT * FROM read_parquet('{BASIC_PATH}')")
    basic_codes = set(con.execute("SELECT 代码 FROM basic").df()['代码'].tolist())
    # 过滤脏数据
    basic_codes = {c for c in basic_codes if len(c) == 6 and c[0] in '036'}
    print(f'  现有 basic: {len(basic_codes)} 只 (filter 后)')

    # 3. 实时从 baostock 拉
    print('  从 baostock 拉最新 A股列表...')
    bs.login()
    rs = bs.query_stock_basic()
    data = rs.get_data()
    bs.logout()
    a = data[data['type'] == '1'].copy()
    a['code_raw'] = a['code'].str.replace('sh.', '').str.replace('sz.', '')
    bs_codes = set(a[a['code_raw'].str.match(r'^[036]\d{5}$', na=False)]['code_raw'].tolist())
    print(f'  baostock 返回: {len(bs_codes)} 只')

    # 4. 找新增
    new_codes = sorted(bs_codes - basic_codes)
    print(f'  新增 (baostock 有但 basic 没有): {len(new_codes)} 只')
    if not new_codes:
        print('  无需更新, 退出')
        return 0

    # 5. 拉新增的 IPO 日期 (用 baostock query_stock_basic 一次拿, 不每只 query_history)
    # 注: query_stock_basic 已返回 ipoDate 字段, 不用单只拉
    a_new = a[a['code_raw'].isin(new_codes)].copy()
    print(f'  抓取完成, baostock query_stock_basic 已含 ipoDate, 实际拿到 {len(a_new)} 行')
    new_rows = []
    for _, r in a_new.iterrows():
        ipo = r.get('ipoDate', None)
        # baostock ipoDate 可能是 datetime/date/str/NaT
        if ipo is None or (hasattr(ipo, '__class__') and ipo.__class__.__name__ == 'NaTType'):
            ipo = None
        elif hasattr(ipo, 'date'):
            ipo = ipo.date() if hasattr(ipo, 'date') and callable(ipo.date) else ipo
        elif isinstance(ipo, str):
            try:
                ipo = date.fromisoformat(ipo)
            except ValueError:
                ipo = None
        new_rows.append({
            '代码': r['code_raw'],
            '名称': r['code_name'] if 'code_name' in r else r['code_raw'],
            '上市日期': ipo,
        })

    # 6. 读原 basic schema, 构造新行 (列对齐, 缺的填 None)
    orig_schema = pq.read_schema(BASIC_PATH)
    orig_cols = [f.name for f in orig_schema]
    print(f'  原 schema: {len(orig_cols)} 列')

    # 新行 dict → 全 schema 列
    new_full_rows = []
    for r in new_rows:
        row = {col: None for col in orig_cols}
        row['代码'] = r['代码']
        row['名称'] = r['名称']
        ipo = r['上市日期']
        if ipo is not None:
            # 已是 date / datetime / Timestamp 都不需要 fromisoformat
            if isinstance(ipo, str):
                try:
                    ipo = date.fromisoformat(ipo)
                except ValueError:
                    ipo = None
            row['上市日期'] = ipo
        new_full_rows.append(row)

    # 7. 写新文件: 复制所有老 row group + 追加新行
    import pandas as pd
    df_new = pd.DataFrame(new_full_rows)
    print(f'  新增 DataFrame: {len(df_new)} 行')

    # 类型对齐
    for col in orig_cols:
        target_type = orig_schema.field(col).type
        if pa.types.is_date32(target_type) and col in df_new.columns:
            df_new[col] = pd.to_datetime(df_new[col], errors='coerce')

    new_path = BASIC_PATH + '.new'
    writer = pq.ParquetWriter(new_path, orig_schema, compression='snappy')
    pf = pq.ParquetFile(BASIC_PATH)
    kept = 0
    for i in range(pf.num_row_groups):
        rg = pf.read_row_group(i)
        writer.write_table(rg)
        kept += rg.num_rows
    print(f'  复制老数据: {kept:,} 行')

    # 追加新行
    table_new = pa.Table.from_pandas(df_new, preserve_index=False, safe=False)
    if table_new.schema != orig_schema:
        table_new = table_new.cast(orig_schema, safe=False)
    writer.write_table(table_new)
    writer.close()
    print(f'  写入新数据: {len(df_new)} 行')

    # fsync + atomic rename
    fd = os.open(new_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(new_path, BASIC_PATH)
    print(f'  ✓ 原子 rename 完成')

    # 8. 验证
    print('\n=== 验证 ===')
    con2 = duckdb.connect(':memory:')
    r = con2.execute(f"SELECT COUNT(*) FROM read_parquet('{BASIC_PATH}')").fetchone()
    # 实际写入了 5,515 (原) + 334 (新) = 5,849; basic_codes 是 filter 后的 5,203 (丢了 ~312 脏/非 6 位代码)
    expected = 5515 + len(new_codes)
    print(f'  新 basic 总行数: {r[0]} (期望 {expected})')
    assert r[0] == expected, f'行数不对: {r[0]}'

    # 抽查 3 只新代码
    for c in new_codes[:3]:
        r2 = con2.execute(f"SELECT 代码, 名称, 上市日期 FROM read_parquet('{BASIC_PATH}') WHERE 代码 = '{c}'").df()
        print(f'  {c}: {r2.iloc[0].to_dict()}')

    print('\n完成!')
    return 0

if __name__ == '__main__':
    sys.exit(main())
