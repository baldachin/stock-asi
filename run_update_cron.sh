#!/bin/bash
# 股票数据增量更新 cron 包装脚本 (Parquet 方案, 2026-06-05 改造)
# A股交易日 17:00 跑 (收盘后1小时)
#
# 行为 (相比 DuckDB 方案极大简化):
#   1. setsid 守护化 (调用方 kill 不影响 writer)
#   2. 跑 update_kdata_parquet.py (写 ~/stock_data/kdata.parquet)
#   3. writer 失败重试 3 次
#   4. 不再 stop-and-respawn streamlit (Parquet 无锁, dashboard 可以同时跑)
#
# 关键设计: 任何时候都能跑, dashboard/writer 互不干扰

set -e
cd /home/hanshuang8902/stock
source ~/.bashrc 2>/dev/null || true

# === 守护化 ===
if [ -z "$STOCK_CRON_DAEMON" ]; then
    export STOCK_CRON_DAEMON=1
    LOG="/tmp/update_kdata_$(date +\%Y\%m\%d_\%H\%M).log"
    setsid bash "$0" "$@" > /dev/null 2>&1 < /dev/null &
    disown
    echo "守护进程已启动, 日志: $LOG"
    echo "  可用 'tail -f $LOG' 查看进度"
    exit 0
fi

# === 真正的 cron 逻辑 (守护进程内) ===
LOG="/tmp/update_kdata_$(date +\%Y\%m\%d_\%H\%M).log"
echo "[$(date)] ===== 开始增量更新 (Parquet 方案) =====" > "$LOG"

# 跑 writer, 失败重试 3 次
for attempt in 1 2 3; do
    echo "[$(date)] 第 $attempt/3 次尝试" >> "$LOG"
    if /home/hanshuang8902/stock/.venv/bin/python /home/hanshuang8902/stock/update_kdata_parquet.py >> "$LOG" 2>&1; then
        echo "[$(date)] ✓ writer 成功" >> "$LOG"
        exit 0
    else
        echo "[$(date)] ✗ writer 失败 (第 $attempt 次)" >> "$LOG"
        if [ $attempt -lt 3 ]; then
            sleep 60
        fi
    fi
done

echo "[$(date)] ✗ 3 次都失败, 通知" >> "$LOG"
# 失败也 exit 0 让 cron 知道跑过了
exit 0
