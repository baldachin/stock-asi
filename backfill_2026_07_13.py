"""
Backfill 2026-07-13 (周一) 单日 K线数据
- 场景: 整段空 (parquet 7/10 之后 0 行, 中间 7/11 周六/7/12 周日 周末无数据)
- 数据源: baostock query_history_k_data_plus, 全 ~5135 只票
- 写入: 复用 update_kdata_parquet.merge_and_write (CROP_DAYS=0 + dedup keep=last)
- 耗时预估: ~22 分钟 (跟 daily writer 同量级, 5135 只 × 1 天)

用法:
    ~/stock/.venv/bin/python ~/stock/backfill_2026_07_13.py

前置 (2026-07-14):
    1. update_kdata_parquet.py 已修 (FATAL → 早 return on df_new.max < last_date)
    2. dashboard 仍会显示 7/10 数据 (这次补 7/13 周一)

注意:
    - 7/11/7/12 是周末, baostock 没数据, 跳过
    - 7/14 (今天) 还在盘中, baostock 拿不到当日完整 K线, 等收盘 (15:00) 后单独补
    - backfill 前最好禁 cron, 避免 17:00 cron 撞锁 (但实际有锁保护, 撞锁会 exit 2)
    - 完成后用 build_window_parquet.py 重建 kdata_5y.parquet (dashboard 优先读)
"""
import os
import sys
import time
from datetime import datetime

import baostock as bs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, '/home/hanshuang8902/stock')
from update_kdata_parquet import (
    fetch_all_incremental, get_all_codes, get_last_date_from_parquet,
    merge_and_write, PARQUET_PATH, BATCH_SIZE,
)


# ===== 配置 =====
BACKFILL_FROM = '2026-07-13'
BACKFILL_TO   = '2026-07-13'  # 单日 (周一), 7/14 等收盘后单独补
BACKUP_PATH   = PARQUET_PATH + '.pre_backfill_2026_07_13.bak'


def main():
    t0 = datetime.now()
    print(f'\n{"="*50}')
    print(f'[{t0.strftime("%H:%M:%S")}] Backfill 2026-07-13 (周一)')
    print(f'{"="*50}')

    # 1. 备份
    print(f'\n[Step1] 备份当前 parquet → {BACKUP_PATH}')
    import shutil
    if not os.path.exists(BACKUP_PATH):
        shutil.copy2(PARQUET_PATH, BACKUP_PATH)
        print(f'  备份完成: {os.path.getsize(BACKUP_PATH) / 1e6:.1f}MB')
    else:
        print(f'  备份已存在, 复用: {BACKUP_PATH}')

    # 2. 确认目标窗口在 parquet 里是空的
    print(f'\n[Step2] 确认目标窗口在 parquet 里是空的...')
    pf = pq.ParquetFile(PARQUET_PATH)
    target_count = 0
    for i in range(pf.num_row_groups):
        col = pf.read_row_group(i, columns=['date']).column('date')
        for v in col.to_pylist():
            if pd.Timestamp(v).date() == pd.Timestamp(BACKFILL_FROM).date():
                target_count += 1
    print(f'  parquet 里 {BACKFILL_FROM} 现有 {target_count} 行')
    if target_count > 0:
        print(f'  [WARN] 目标日期已有数据! 改用"两步流式去重"模式 (skill stock-parquet-backfill)')
        sys.exit(1)

    # 3. 股票列表 + 抓取
    print(f'\n[Step3] 获取股票列表...')
    codes = get_all_codes()
    if not codes:
        print('  [FATAL] 股票列表获取失败')
        sys.exit(2)
    print(f'  共 {len(codes)} 只股票')

    print(f'\n[Step4] 抓取 {BACKFILL_FROM} ~ {BACKFILL_TO} ...')
    df_new = fetch_all_incremental(codes, BACKFILL_FROM, BACKFILL_TO, t0)
    if df_new.empty:
        print(f'  [FATAL] 抓取为空, baostock 上游可能无数据, 不写盘')
        sys.exit(3)
    print(f'\n  抓取到 {len(df_new):,} 行')
    print(f'  日期范围: {df_new["date"].min()} ~ {df_new["date"].max()}')
    print(f'  股票数: {df_new["symbol"].nunique()}')

    # 4. 合并写盘 (复用 update_kdata_parquet.merge_and_write)
    # 注意: CROP_DAYS=0, merge_and_write 的早 return 检查 df_new.max_date >= last_date
    #       这次 df_new.max_date=7/11, last_date=7/10, 满足, 写盘
    print(f'\n[Step5] 合并写盘...')
    merge_and_write(df_new)

    # 5. 验证
    print(f'\n[Step6] 验证...')
    pf = pq.ParquetFile(PARQUET_PATH)
    new_total = pf.metadata.num_rows
    new_last = get_last_date_from_parquet()
    print(f'  新总行数: {new_total:,}')
    print(f'  新最后日期: {new_last}')

    # 检查目标日期行数
    new_target_count = 0
    for i in range(pf.num_row_groups):
        col = pf.read_row_group(i, columns=['date']).column('date')
        for v in col.to_pylist():
            if pd.Timestamp(v).date() == pd.Timestamp(BACKFILL_FROM).date():
                new_target_count += 1
    print(f'  目标日期 {BACKFILL_FROM} 行数: {new_target_count} (期望 ≥ 5000)')

    if new_target_count < 5000:
        print(f'  [WARN] 目标日期行数偏少, baostock 可能残缺 (单股没数据)')

    elapsed = (datetime.now() - t0).total_seconds()
    print(f'\n完成! 耗时 {elapsed:.0f}s ({elapsed/60:.1f} 分钟)')
    print(f'下一步: 跑 build_window_parquet.py 重建 kdata_5y.parquet')
    print(f'        然后 streamlit 点 "🔄 刷新缓存" 按钮')


if __name__ == '__main__':
    main()
