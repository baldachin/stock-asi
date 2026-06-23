#!/usr/bin/env python3
"""
ASI Top50 每日 15:30 自动更新脚本 (Parquet 版, 替代 DuckDB 版)

- 用 DuckDB in-memory 读 Parquet 计算最新年份 ASI Top50
- 清空旧 Notion 记录，创建新记录
- 与原 DuckDB 版公式一致: LN(max_rank + 1 - rank) / LN(max_rank + 1) × 100
  (无价格加权, 全交易日口径)
"""

import sys, os, json, urllib.request, urllib.error, time
from datetime import datetime, date
import duckdb

# ---------- 配置 ----------
KDATA_PATH     = '~/stock_data/kdata.parquet'
STOCK_BASIC    = '~/stock_data/stock_basic.parquet'
NOTION_API_KEY = os.environ.get('NOTION_API_KEY', '')
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json"
}
DB_ID   = "3fd97742-040a-46c2-bd82-f4c78cca39a0"
DS_ID   = "0857125a-01ac-48b0-b650-6aa230f05fdc"
TOP_N   = 50
MIN_YEAR_OFFSET = 1  # 上市满一年
# ----------------------------

def notion_req(url, data=None, method='POST'):
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=NOTION_HEADERS, method=method)
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(int(e.headers.get('Retry-After', '5')) + 1)
            else:
                raise
    raise Exception("Notion request failed after 5 retries")

def get_all_page_ids():
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
    page_ids = get_all_page_ids()
    for page_id in page_ids:
        try:
            notion_req(f"https://api.notion.com/v1/pages/{page_id}", {"archived": True}, 'PATCH')
        except Exception as e:
            print(f"  Archive error: {e}")
    print(f"  Archived {len(page_ids)} pages")

def compute_asi_top50(year):
    """从 Parquet 计算当年 ASI Top50 (与原 DuckDB 版公式一致)"""
    con = duckdb.connect(':memory:')
    con.execute(f"CREATE VIEW kdata AS SELECT * FROM read_parquet('{KDATA_PATH}')")

    # 检查交易天数
    trading_days = con.execute(f"""
        SELECT COUNT(DISTINCT date) FROM kdata WHERE year(date) = {year}
    """).fetchone()[0]
    print(f"  {year}年已交易 {trading_days} 天")

    # SQL 窗口函数计算 Top50 (与原版公式一致: 无价格加权)
    rows = con.execute(f"""
        WITH daily AS (
            SELECT
                symbol,
                date,
                RANK() OVER (PARTITION BY date ORDER BY amount DESC) AS amount_rank,
                COUNT(*) OVER (PARTITION BY date) AS max_rank
            FROM kdata
            WHERE year(date) = {year} AND amount > 0
        ),
        scored AS (
            SELECT
                symbol,
                amount_rank,
                max_rank,
                LN(max_rank + 1 - amount_rank) / LN(max_rank + 1) * 100 AS asi_score,
                (amount_rank <= 50)  AS in_top50,
                (amount_rank <= 100) AS in_top100
            FROM daily
            WHERE amount_rank <= max_rank
        ),
        agg AS (
            SELECT
                symbol,
                COUNT(*)                                    AS trading_days,
                SUM(asi_score)                             AS asi_sum,
                AVG(asi_score)                             AS asi_mean,
                MIN(amount_rank)                           AS best_rank,
                AVG(amount_rank)                           AS avg_rank,
                SUM(CASE WHEN in_top50  THEN 1 ELSE 0 END) AS top50_days,
                SUM(CASE WHEN in_top100 THEN 1 ELSE 0 END) AS top100_days
            FROM scored
            GROUP BY symbol
        )
        SELECT
            symbol,
            asi_sum,
            asi_mean,
            best_rank,
            avg_rank,
            top50_days,
            top100_days,
            trading_days
        FROM agg
        ORDER BY asi_sum DESC
        LIMIT {TOP_N}
    """).fetchall()

    # 获取股票名称 (从 stock_basic.parquet)
    sb_rows = con.execute(f"""
        SELECT 代码, 名称 FROM read_parquet('{STOCK_BASIC}')
    """).fetchall()
    name_map = {str(code): name for code, name in sb_rows}

    con.close()

    result = []
    for rank, row in enumerate(rows, 1):
        symbol = str(row[0])
        result.append({
            'symbol': symbol,
            'name': name_map.get(symbol, symbol),
            'asi_sum': row[1],
            'asi_mean': row[2],
            'best_rank': int(row[3]),
            'avg_rank': round(row[4], 1),
            'top50_days': int(row[5]),
            'top100_days': int(row[6]),
            'trading_days': int(row[7]),
            'rank': rank,
        })
    if result:
        print(f"  计算完成: top1={result[0]['symbol']}({result[0]['name']})")
    return result

def create_pages(rows):
    today = date.today().isoformat()
    created = 0
    for row in rows:
        code_num = None
        try:
            code_num = int(row['symbol'])
        except:
            pass
        props = {
            "Name": {"title": [{"text": {"content": row['name']}}]},
            "代码": {"number": code_num},
            "asi_sum": {"number": round(float(row['asi_sum']), 4)},
            "asi_mean": {"number": round(float(row['asi_mean']), 4)},
            "最佳排名": {"number": row['best_rank']},
            "平均排名": {"number": row['avg_rank']},
            "top50天": {"number": row['top50_days']},
            "top100天": {"number": row['top100_days']},
            "交易天数": {"number": row['trading_days']},
            "排名": {"number": row['rank']},
            "更新日期": {"date": {"start": today}},
        }
        for attempt in range(5):
            try:
                result = notion_req("https://api.notion.com/v1/pages", {
                    "parent": {"database_id": DB_ID},
                    "properties": props
                })
                if result.get('id'):
                    created += 1
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(int(e.headers.get('Retry-After', '5')) + 1)
                else:
                    raise
        if created % 10 == 0:
            print(f"  Created {created} pages...")
    print(f"  Total created: {created}")
    return created

def main():
    print(f"\n{'='*50}")
    print(f"[{datetime.now()}] ASI Top50 更新 (Parquet版)")
    print(f"{'='*50}")

    # 用 in-memory DuckDB 读 Parquet 获取最新年份
    con = duckdb.connect(':memory:')
    con.execute(f"CREATE VIEW kdata AS SELECT * FROM read_parquet('{KDATA_PATH}')")
    latest_year = con.execute("SELECT MAX(year(date))::INT FROM kdata").fetchone()[0]
    con.close()
    print(f"最新年份: {latest_year}")

    # 计算
    print(f"\n计算 {latest_year} 年 ASI Top50...")
    rows = compute_asi_top50(latest_year)

    if not rows:
        print("  无数据，跳过 Notion 同步")
        return

    # 清空旧记录
    print(f"\n清空旧 Notion 记录...")
    archive_all_pages()

    # 创建新记录
    print(f"\n创建新记录...")
    create_pages(rows)

    print(f"\n[{datetime.now()}] 完成!")
    print(f"Top5: {', '.join(r['name'] for r in rows[:5])}")

if __name__ == '__main__':
    main()