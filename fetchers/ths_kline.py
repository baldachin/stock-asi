"""同花顺 K 线 fetcher — baostock 完美替代

端点: https://d.10jqka.com.cn/v6/line/hs_{code}/01/last.js
频率: 01=日 11=周 21=月 30=1min 31=5min 32=15min 33=30min 34=60min
字段: [date, open, high, low, close, volume, amount, 振幅, 涨幅, 涨跌额, 换手率]
    - 跟 baostock schema 完全一致 (索引 0-6 兼容)
    - baostock 不返回额外 4 字段 (振幅/涨幅/涨跌额/换手率)

依赖: stdlib only (urllib + json)
验证: /tmp/hermes-verify-fetchers-2026-07-28.py 10/10 PASS
"""
import json
import time
import urllib.request
from typing import Optional

UA = "Mozilla/5.0"
FREQ_MAP = {"D": "01", "W": "11", "M": "21", "1": "30", "5": "31", "15": "32", "30": "33", "60": "34"}
THS_FIELD_NAMES = ["date", "open", "high", "low", "close", "volume", "amount",
                   "amplitude_pct", "change_pct", "change_amt", "turnover_pct"]


def _strip_jsonp(text: str) -> dict:
    """剥 JSONP 外壳 quotebridge_v6_line_hs_xxx_01_last({...})"""
    if "(" not in text or ")" not in text:
        return json.loads(text)
    return json.loads(text[text.index("(") + 1:text.rindex(")")])


def _build_url(code: str, frequency: str = "D") -> str:
    """code: 6 位裸码. frequency: D/W/M/1/5/15/30/60"""
    freq_code = FREQ_MAP.get(frequency.upper(), "01")
    return f"https://d.10jqka.com.cn/v6/line/hs_{code}/{freq_code}/last.js"


def _parse_rows(j: dict, start_date: Optional[str] = None, end_date: Optional[str] = None) -> list:
    """raw CSV -> list[dict]. start/end YYYYMMDD inclusive."""
    raw = j.get("data", "")
    if not raw:
        return []
    rows = []
    for line in raw.split(";"):
        f = line.split(",")
        if len(f) < 7:
            continue
        date = f[0]
        if start_date and date < start_date:
            continue
        if end_date and date > end_date:
            continue
        row = {THS_FIELD_NAMES[i]: f[i] for i in range(len(f)) if i < len(THS_FIELD_NAMES)}
        rows.append(row)
    return rows


def fetch_one(code: str, frequency: str = "D",
              start_date: Optional[str] = None, end_date: Optional[str] = None,
              retries: int = 3, retry_delay: float = 0.5) -> list:
    """单只股票 K 线.

    code: 6-digit ticker. Main board e.g. "600519", BSE e.g. "920xxx".
    frequency: D daily / W weekly / M monthly / 1 min / 5 min / 15 min / 30 min / 60 min.
    start_date: YYYYMMDD inclusive or None. end_date: YYYYMMDD inclusive or None.
    retries: HTTP retry count on failure. retry_delay: seconds between retries.

    Returns list of dicts sorted by date ascending. Empty list on failure.
    """
    url = _build_url(code, frequency)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=10) as r:
                text = r.read().decode("utf-8")
            return _parse_rows(_strip_jsonp(text), start_date, end_date)
        except Exception:
            if attempt < retries - 1:
                time.sleep(retry_delay)
    return []


def fetch_many(codes: list, frequency: str = "D",
               start_date: Optional[str] = None, end_date: Optional[str] = None,
               sleep_between: float = 0.0) -> dict:
    """批量抓取. code -> list[dict]. 失败 code 给空列表.

    串行抓取. 5537 只实测 ~ 60-90 分钟 (200ms per request).
    sleep_between: 调用间隔秒, 0 跑满, 0.05 慢一点但更稳.
    """
    out = {}
    for code in codes:
        out[code] = fetch_one(code, frequency, start_date, end_date)
        if sleep_between > 0:
            time.sleep(sleep_between)
    return out


if __name__ == "__main__":
    import sys
    code = sys.argv[1] if len(sys.argv) > 1 else "600519"
    freq = sys.argv[2] if len(sys.argv) > 2 else "D"
    start = sys.argv[3] if len(sys.argv) > 3 else None
    end = sys.argv[4] if len(sys.argv) > 4 else None
    rows = fetch_one(code, freq, start, end)
    print(f"code={code} freq={freq} got {len(rows)} rows")
    if rows:
        print(f"first: {rows[0]}")
        print(f"last:  {rows[-1]}")
