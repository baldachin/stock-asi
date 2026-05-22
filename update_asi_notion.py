#!/usr/bin/env python3
"""
ASI Top50 每日 15:30 自动更新脚本
- 计算最新年份的 ASI Top50
- 清空旧记录，创建新记录
- 设置 更新日期 为今天
"""

import sys, os, json, urllib.request, urllib.error, time
from datetime import datetime, date

# ========== 配置 ==========
KDATA_PATH = '/home/hanshuang8902/stock/kdata.parquet'
STOCK_BASIC_PATH = '/home/hanshuang8902/stock/stock_basic.parquet'
NOTION_API_KEY = os.environ.get('NOTION_API_KEY', '')
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json"
}
DB_ID = "3fd97742-040a-46c2-bd82-f4c78cca39a0"   # for creating pages
DS_ID = "0857125a-01ac-48b0-b650-6aa230f05fdc"  # for querying
TOP_N = 50
MIN_LISTING_YEAR_OFFSET = 1  # 上市满一年: listed before (latest_year - 1)

# ========== Notion 工具 ==========

def notion_req(url, data=None, method='POST'):
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=NOTION_HEADERS, method=method)
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = e.headers.get('Retry-After', '5')
                print(f"  [Rate Limit] Waiting {retry_after}s...")
                time.sleep(int(retry_after) + 1)
            else:
                body_err = e.read().decode() if e.fp else ''
                raise urllib.error.HTTPError(url, e.code, f"{e.reason}: {body_err}", e.headers, e.fp)
    raise Exception("Notion request failed after 5 retries")

def get_all_page_ids():
    """Get all page IDs from the database via data_source query"""
    cursor = None
    page_ids = []
    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        result = notion_req(f"https://api.notion.com/v1/data_sources/{DS_ID}/query", payload)
        pages = result.get('results', [])
        if not pages:
            break
        page_ids.extend(p['id'] for p in pages)
        cursor = result.get('next_cursor')
        if not cursor:
            break
    return page_ids

def archive_all_pages():
    """Archive all existing pages in the database"""
    page_ids = get_all_page_ids()
    archived = 0
    for page_id in page_ids:
        try:
            notion_req(
                f"https://api.notion.com/v1/pages/{page_id}",
                {"archived": True},
                'PATCH'
            )
            archived += 1
        except Exception as e:
            print(f"  Archive error for {page_id}: {e}")
    print(f"  Archived {archived} pages")
    return archived

def compute_asi_top50(year):
    """Compute ASI Top50 for given year"""
    import pandas as pd
    import numpy as np

    # Only read rows where date year == target year (predicate pushdown avoids loading all 16.7M rows)
    start_date_str = f"{year}-01-01"
    df = pd.read_parquet(KDATA_PATH, columns=['symbol', 'date', 'amount'], filters=[("date", ">=", start_date_str)])
    df = df.reset_index()  # date was stored as index, convert to column
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    # Re-filter in case partition row groups span multiple years (RG stats overlap)
    df = df[df['year'] == year]
    df = df[df['amount'] > 0]

    # Listed before filter (load basic for eligible set + name mapping)
    listed_before_year = year - MIN_LISTING_YEAR_OFFSET
    basic = pd.read_parquet(STOCK_BASIC_PATH)
    basic['上市日期_dt'] = pd.to_datetime(basic['上市日期'])
    eligible = set(basic[basic['上市日期_dt'].dt.year < listed_before_year]['代码'].astype(str).tolist())
    df = df[df['symbol'].isin(eligible)]

    if len(df) == 0:
        print("  NO_DATA")
        return None

    daily_counts = df.groupby('date')['symbol'].transform('count')
    df['amount_rank'] = df.groupby('date')['amount'].rank(method='min', ascending=False)
    numerator = np.log(daily_counts + 1 - df['amount_rank'])
    denominator = np.log(daily_counts + 1)
    df['asi_score'] = np.where(denominator != 0, (numerator / denominator) * 100, 0.0)
    df['top50'] = (df['amount_rank'] <= 50).astype(int)
    df['top100'] = (df['amount_rank'] <= 100).astype(int)

    agg = df.groupby('symbol').agg(
        asi_sum=('asi_score', 'sum'),
        asi_mean=('asi_score', 'mean'),
        asi_trading_days=('asi_score', 'count'),
        best_rank=('amount_rank', 'min'),
        avg_rank=('amount_rank', 'mean'),
        top50_days=('top50', 'sum'),
        top100_days=('top100', 'sum'),
    ).reset_index()

    agg = agg.sort_values('asi_sum', ascending=False).head(TOP_N).reset_index(drop=True)
    agg['rank'] = agg.index + 1

    # Get stock names
    name_map = dict(zip(basic['代码'].astype(str), basic['名称']))
    agg['name'] = agg['symbol'].map(name_map).fillna(agg['symbol'])

    print(f"  Computed {len(agg)} rows for {year}, top1={agg.iloc[0]['symbol']}({agg.iloc[0]['name']})")
    return agg

def create_pages(df):
    """Create new pages in Notion"""
    today = date.today().isoformat()
    created = 0
    for _, row in df.iterrows():
        stock_name = row['name']
        code = str(row['symbol'])
        try:
            code_num = int(code)
        except:
            code_num = None
        props = {
            "Name": {"title": [{"text": {"content": stock_name}}]},
            "代码": {"number": code_num},
            "asi_sum": {"number": round(float(row['asi_sum']), 4)},
            "asi_mean": {"number": round(float(row['asi_mean']), 4)},
            "最佳排名": {"number": int(row['best_rank'])},
            "平均排名": {"number": round(float(row['avg_rank']), 1)},
            "top50天": {"number": int(row['top50_days'])},
            "top100天": {"number": int(row['top100_days'])},
            "交易天数": {"number": int(row['asi_trading_days'])},
            "排名": {"number": int(row['rank'])},
            "更新日期": {"date": {"start": today}},
        }
        payload = {
            "parent": {"database_id": DB_ID},
            "properties": props
        }
        for attempt in range(5):
            try:
                result = notion_req("https://api.notion.com/v1/pages", payload)
                if result.get('id'):
                    created += 1
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    retry_after = e.headers.get('Retry-After', '5')
                    print(f"  [Rate Limit] Waiting {retry_after}s...")
                    time.sleep(int(retry_after) + 1)
                else:
                    raise
        if created % 10 == 0:
            print(f"  Created {created} pages...")
    print(f"  Total created: {created}")
    return created

# ========== 主流程 ==========

def main():
    print(f"\n{'='*50}")
    print(f"[{datetime.now()}] ASI Top50 Update Started")
    print(f"{'='*50}")

    # Step 1: determine latest year (read metadata only, no data loaded)
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(KDATA_PATH)
    last_rg = pf.metadata.num_row_groups - 1
    dates_rg = pf.read_row_group(last_rg, columns=['date']).column('date').to_pylist()
    latest_date = max(dates_rg)
    if isinstance(latest_date, str):
        latest_date = datetime.strptime(latest_date, '%Y-%m-%d').date()
    latest_year = latest_date.year
    # count trading days: scan all row groups date ranges from metadata
    trading_days_sofar = 0
    for i in range(pf.metadata.num_row_groups):
        tbl = pf.read_row_group(i, columns=['date'])
        dates = tbl.column('date').to_pylist()
        trading_days_sofar += len(set(dates))
    print(f"Latest data: {latest_date} ({trading_days_sofar} trading days in {latest_year})")

    # Step 2: compute ASI Top50 (pass latest_year so it reads only needed data)
    print(f"\nComputing ASI Top50 for {latest_year}...")
    df = compute_asi_top50(latest_year)
    if df is None or len(df) == 0:
        print("ERROR: No ASI data computed")
        return

    # Step 3: archive old pages
    print(f"\nArchiving old pages...")
    archive_all_pages()

    # Step 4: create new pages
    print(f"\nCreating new pages...")
    create_pages(df)

    print(f"\n[{datetime.now()}] Update Complete!")
    print(f"Year: {latest_year}, Rows: {len(df)}")
    print(f"Top5: {', '.join(df.head(5)['name'].tolist())}")
    print(f"Update date set to: {date.today()}")

if __name__ == '__main__':
    main()
