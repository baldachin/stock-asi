#!/bin/bash
# streamlit 8502 监控 — 端口挂了自动重启
# 每 5 分钟跑一次, 端口没了就启 streamlit
# 配套: 启 streamlit 前查 duckdb 看 parquet 状态, 健康才启

PORT=8502
LOG=/tmp/streamlit.log
STOCK=/home/hanshuang8902/stock
PY=$STOCK/.venv/bin/streamlit

# 1. 端口在监听, 健康, 退出 0
if ss -tlnp 2>/dev/null | grep -q ":$PORT "; then
    exit 0
fi

# 2. 端口挂了, 但 streamlit 进程残留 (罕见) — 先 kill
pkill -9 -f "streamlit run dashboard.py" 2>/dev/null
sleep 2

# 3. 检查真盘是否健康 (max_date 5 天内), 不健康不起 streamlit (用户会看到 stale data)
if ! ~/stock/.venv/bin/python -c "
import sys
sys.path.insert(0, '/home/hanshuang8902/stock')
from datetime import date, timedelta
import duckdb
r = duckdb.connect(':memory:').execute(\"SELECT MAX(date) FROM read_parquet('/home/hanshuang8902/stock_data/kdata.parquet')\").fetchone()
mx = r[0]
days_old = (date.today() - mx).days if hasattr(mx, 'year') else 999
sys.exit(0 if days_old <= 5 else 1)
" 2>/dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] streamlit 监控: parquet 数据太旧 (max > 5 天), 跳过重启" >> $LOG
    exit 1
fi

# 4. 启动 streamlit (setsid 脱离 cron session, 不会被 SIGTERM)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] streamlit 监控: 端口 $PORT 挂了, 自动重启" >> $LOG
cd $STOCK && setsid $PY run dashboard.py --server.port $PORT --server.address 0.0.0.0 --browser.gatherUsageStats false >> $LOG 2>&1 < /dev/null &

exit 0
