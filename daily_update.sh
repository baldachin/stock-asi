#!/bin/bash
# 股票数据每日更新串行任务 (kdata writer → heat rotation sync)
# A股交易日 17:00 跑 (收盘后1小时, 17:00 ~ 17:30 writer, 17:30 ~ 17:35 heat sync)
#
# 相比之前的两条独立 cron:
#   - 修 sync_heat 在 17:30 跑时, kdata writer 还没跑完 (实际 writer 可能跑到 17:42)
#   - 刚性保证 kdata 完成后再 sync, 避免 "今日已存在无新数据" 的假阴性
#
# 行为:
#   1. setsid 守护化 (调用方 kill 不影响 writer)
#   2. 跑 update_kdata_parquet.py (写 ~/stock_data/kdata.parquet) — 失败重试 3 次
#   3. writer 成功后跑 build_window_parquet.py (dashboard 用 5y 窗口版)
#   4. 上面都成功后跑 sync_heat_rotation.py (更新 ~/stock_data/heat_rotation_daily.parquet)
#   5. heat sync 失败也 exit 0, 因为数据流已在 writer 阶段更新

set -e
cd /home/hanshuang8902/stock
source ~/.bashrc 2>/dev/null || true

# === 守护化 ===
if [ -z "$STOCK_CRON_DAEMON" ]; then
    export STOCK_CRON_DAEMON=1
    LOG="/tmp/daily_update_$(date +\%Y\%m\%d_\%H\%M).log"
    setsid bash "$0" "$@" > /dev/null 2>&1 < /dev/null &
    disown
    echo "守护进程已启动, 日志: $LOG"
    echo "  可用 'tail -f $LOG' 查看进度"
    exit 0
fi

# === 真正的 cron 逻辑 (守护进程内) ===
LOG="/tmp/daily_update_$(date +\%Y\%m\%d_\%H\%M).log"
echo "[$(date)] ===== 开始每日更新串行任务 =====" > "$LOG"

# 步骤 1: 跑 kdata writer, 失败重试 3 次
KDATA_OK=0
for attempt in 1 2 3; do
    echo "[$(date)] [kdata] 第 $attempt/3 次尝试" >> "$LOG"
    if /home/hanshuang8902/stock/.venv/bin/python /home/hanshuang8902/stock/update_kdata_parquet.py >> "$LOG" 2>&1; then
        echo "[$(date)] [kdata] ✓ writer 成功" >> "$LOG"
        KDATA_OK=1
        break
    else
        echo "[$(date)] [kdata] ✗ writer 失败 (第 $attempt 次)" >> "$LOG"
        if [ $attempt -lt 3 ]; then
            sleep 60
        fi
    fi
done

if [ $KDATA_OK -eq 0 ]; then
    echo "[$(date)] [kdata] ✗ 3 次都失败, 跳过 heat sync" >> "$LOG"
    exit 0
fi

# 步骤 2: 重建窗口版 parquet (dashboard 加速用)
echo "[$(date)] [window] 重建 5y 窗口版 parquet..." >> "$LOG"
if /home/hanshuang8902/stock/.venv/bin/python /home/hanshuang8902/stock/build_window_parquet.py --years 5 >> "$LOG" 2>&1; then
    echo "[$(date)] [window] ✓ 窗口版重建完成" >> "$LOG"
else
    echo "[$(date)] [window] ⚠ 重建失败 (不影响主流程, dashboard 会降级用全量)" >> "$LOG"
fi

# 步骤 3: 同步 heat rotation 数据 (依赖 kdata 已更新)
echo "[$(date)] [heat] 开始同步 heat rotation..." >> "$LOG"
if /home/hanshuang8902/stock/.venv/bin/python /home/hanshuang8902/stock/sync_heat_rotation.py >> "$LOG" 2>&1; then
    echo "[$(date)] [heat] ✓ heat sync 成功" >> "$LOG"
else
    echo "[$(date)] [heat] ⚠ heat sync 失败 (kdata 已更新, 下次重试)" >> "$LOG"
fi

echo "[$(date)] ===== 每日更新完成 =====" >> "$LOG"
exit 0