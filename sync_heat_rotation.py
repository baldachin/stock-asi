#!/usr/bin/env python3
"""
热度轮动数据同步 (cron 任务)

触发: A股交易日 17:30 (在 kdata 更新 17:00 之后 30 分钟, 确保今天数据已落盘)
任务: 算今天的 heat rotation, 追加到 ~/stock_data/heat_rotation_daily.parquet
幂等: 重复执行无副作用 (dedup by date+symbol+window+hot+cold)

用法:
    ~/stock/.venv/bin/python ~/stock/sync_heat_rotation.py
    ~/stock/.venv/bin/python ~/stock/sync_heat_rotation.py --window 20 --hot 80 --cold 50
"""
import sys
import os
import time
import argparse
from datetime import datetime

# 让 heat_rotation_lib 可被 import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from heat_rotation_lib import append_heat_rotation_today


def run(window_days, hot_th, cold_th):
    t0 = time.time()
    print(f"[{datetime.now()}] sync_heat_rotation start "
          f"(window={window_days}, hot={hot_th}, cold={cold_th})")
    try:
        new_n, today = append_heat_rotation_today(window_days, hot_th, cold_th)
        elapsed = time.time() - t0
        if new_n == 0:
            print(f"[{datetime.now()}] 今日 {today} 已存在, 无新数据 ({elapsed:.1f}s)")
        else:
            print(f"[{datetime.now()}] ✅ 新增 {new_n} 条 ({today}, {elapsed:.1f}s)")
        return 0
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[{datetime.now()}] ❌ 失败 ({elapsed:.1f}s): {e}", file=sys.stderr)
        return 1


def main():
    parser = argparse.ArgumentParser(description="同步今日 heat rotation 数据到 parquet")
    parser.add_argument("--window", type=int, default=20, help="热度窗口天数 (默认 20)")
    parser.add_argument("--hot", type=float, default=80.0, help="热阈值 (默认 80)")
    parser.add_argument("--cold", type=float, default=50.0, help="冷阈值 (默认 50)")
    parser.add_argument("--retries", type=int, default=2, help="失败重试次数 (默认 2)")
    args = parser.parse_args()

    for attempt in range(args.retries + 1):
        rc = run(args.window, args.hot, args.cold)
        if rc == 0:
            return rc
        if attempt < args.retries:
            wait = 5 * (attempt + 1)
            print(f"[{datetime.now()}] 重试 {attempt + 1}/{args.retries}, 等待 {wait}s")
            time.sleep(wait)
    return rc


if __name__ == "__main__":
    sys.exit(main())