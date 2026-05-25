#!/usr/bin/env python3
"""
K线数据增量更新 - 腾讯证券接口版
数据源: 腾讯证券 (web.ifzq.gtimg.cn)
策略:
  1. 动态从 kdata.parquet 最后一页读取本地最新日期（不加载全量数据）
  2. 从 baostock 获取全量 A股列表（type=1）
  3. 通过腾讯证券接口批量拉取增量 K线（10 并发，极速）
  4. 写入 kdata_incremental.parquet
  5. 每日 cron: 工作日 16:00 执行
"""

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import baostock as bs
import urllib.request
import json
import time
import gc
import os
import sys
import random
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- 配置 ----------
PARQUET_ORIG  = 'kdata.parquet'
PARQUET_INC   = 'kdata_incremental.parquet'
MAX_WORKERS   = 20           # 并发线程数
REQUESTS_PER_WORKER = 150    # 每个 worker 最大请求数（避免单连接过载）
MAX_RETRIES   = 3
TIMEOUT       = 15            # 请求超时（秒）
DAYS_BACK     = 15            # 每次多抓几天防止遗漏
# ----------------------------

def get_exchange(code):
    c = str(code).zfill(6)
    if c.startswith('6') or c.startswith('9'): return 'sh'
    if c.startswith('0') or c.startswith('3'): return 'sz'
    return None

def get_last_date():
    """只读 parquet 最后一页，快速获取本地最新日期"""
    from datetime import datetime
    pf = pq.ParquetFile(PARQUET_ORIG)
    num_rg = pf.metadata.num_row_groups - 1
    # date 存储在 string 列中，取其最大值后转为 date
    table = pf.read_row_group(num_rg).to_pandas()
    last = table['date'].max()
    return datetime.strptime(last, '%Y-%m-%d').date()

def get_all_codes():
    """从 baostock 获取全量 A股代码"""
    lg = bs.login()
    rs = bs.query_stock_basic()
    data = rs.get_data()
    bs.logout()
    a = data[data['type'] == '1'].copy()
    a['code_raw'] = a['code'].str.replace('sh.', '').str.replace('sz.', '')
    codes = a[a['code_raw'].str.match(r'^[036]\d{5}$', na=False)]['code_raw'].tolist()
    print(f"  A股总数: {len(codes)}")
    return codes

def fetch_tencent_kline(code, start_date, end_date, max_rows=100):
    """
    从腾讯证券拉取日K线
    返回: list of [date, open, close, high, low, volume(万)] 或空 list
    """
    prefix = get_exchange(code)
    if not prefix:
        return []
    url = (f"https://web.ifzq.gtimg.cn/appstock/app/kline/kline"
           f"?_var=kline_day&param={prefix}{code},day,{start_date},{end_date},{max_rows}")
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://finance.qq.com/'
            })
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                text = resp.read().decode()
            json_str = text[text.index('=') + 1:]
            obj = json.loads(json_str)
            key = f"{prefix}{code}"
            if obj.get('code') != 0 or key not in obj.get('data', {}):
                return []
            day_list = obj['data'][key].get('day', [])
            return day_list
        except Exception:
            if attempt < MAX_RETRIES - 1:
                time.sleep(0.5 * (attempt + 1))
    return []

def process_code(code, start_date, end_date):
    """抓取单只股票的 K 线并转换为 DataFrame 行"""
    rows = fetch_tencent_kline(code, start_date, end_date)
    if not rows:
        return pd.DataFrame()
    records = []
    for row in rows:
        try:
            # row: [date, open, close, high, low, volume(万)]
            # 腾讯 volume 单位是万股，转为股
            vol_wan = float(row[5])
            records.append({
                'date': pd.to_datetime(row[0]),
                'symbol': code,
                'open': float(row[1]),
                'close': float(row[2]),
                'high': float(row[3]),
                'low': float(row[4]),
                'volume': int(vol_wan * 10000),
                'amount': 0,  # 腾讯接口无成交额字段，填0
            })
        except (ValueError, IndexError):
            continue
    return pd.DataFrame(records)

def fetch_all_incremental(codes, start_date, end_date, progress_cb=None):
    """并发抓取所有股票的增量数据"""
    all_dfs = []
    total = len(codes)
    done = 0
    fetched_rows = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_code = {
            executor.submit(process_code, code, start_date, end_date): code
            for code in codes
        }
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            try:
                df = future.result()
                if len(df) > 0:
                    all_dfs.append(df)
                    fetched_rows += len(df)
            except Exception:
                pass
            done += 1
            if progress_cb and done % 200 == 0:
                progress_cb(done, total, fetched_rows)

    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

def write_incremental(new_df, inc_path):
    """追加写入增量文件"""
    new_df['date'] = pd.to_datetime(new_df['date'])
    new_df['symbol'] = new_df['symbol'].astype(str).str.zfill(6)
    new_df = new_df[['date','symbol','open','high','low','close','volume','amount']]
    new_df = new_df.sort_values(['date','symbol']).reset_index(drop=True)
    new_df = new_df.drop_duplicates(subset=['date','symbol'], keep='last')

    if os.path.exists(inc_path):
        old = pd.read_parquet(inc_path)
        old['date'] = pd.to_datetime(old['date'])
        new_df = pd.concat([old, new_df], ignore_index=False)
        new_df = new_df.drop_duplicates(subset=['date','symbol'], keep='last')
        new_df = new_df.sort_values(['date','symbol']).reset_index(drop=True)

    new_df.to_parquet(inc_path, engine='pyarrow', compression='snappy')
    return len(new_df)

def main():
    t0 = datetime.now()
    today_str = date.today().strftime('%Y-%m-%d')
    print(f"\n{'='*50}")
    print(f"[{t0.strftime('%H:%M:%S')}] K线增量更新 (腾讯接口)")
    print(f"增量目标: {today_str}")
    print(f"{'='*50}")

    # Step 1: 本地最新日期
    print(f"\n[Step1] 读取本地快照...")
    last_ts = get_last_date()
    last_date_str = str(last_ts)
    # 多抓 DAYS_BACK 天，防止非交易日导致漏抓
    from datetime import timedelta
    fetch_start = (last_ts - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%d')
    print(f"  本地最新: {last_date_str}, 抓取起点: {fetch_start}")

    if last_date_str >= today_str:
        print("  数据已最新，退出")
        return

    # Step 2: 全量股票列表
    print(f"\n[Step2] 获取股票列表...")
    codes = get_all_codes()

    # Step 3: 并发抓取
    def progress(done, total, rows):
        elapsed = (datetime.now() - t0).total_seconds()
        print(f"\r  进度: {done}/{total} ({elapsed:.0f}s, {rows}行)", end='', flush=True)

    print(f"\n[Step3] 抓取 K线 ({fetch_start} → {today_str})...")
    new_df = fetch_all_incremental(codes, fetch_start, today_str, progress_cb=progress)
    print()  # 换行

    if len(new_df) == 0:
        print("  无新数据")
        return

    # Step 4: 只保留 last_date 之后的新数据
    new_df = new_df[new_df['date'] > pd.Timestamp(last_ts)].reset_index(drop=True)
    print(f"  {last_date_str}之后新数据: {len(new_df)} 行, {new_df['symbol'].nunique()} 只")
    print(f"  日期范围: {new_df['date'].min().date()} ~ {new_df['date'].max().date()}")

    # Step 5: 写入增量文件
    print(f"\n[Step4] 写入增量文件...")
    inc_path = PARQUET_INC
    total_rows = write_incremental(new_df, inc_path)
    size_mb = os.path.getsize(inc_path) / 1024**2
    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n完成! 耗时 {elapsed:.1f}s, 总行 {total_rows}")
    print(f"增量文件: {inc_path} ({size_mb:.1f} MB)")

if __name__ == '__main__':
    main()
