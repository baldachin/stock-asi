"""fetchers - vendor fetchers from simonlin1212/a-stock-data (commit 7/26 v3.5.1).

Pure stdlib (urllib + json), no third-party deps.

Modules:
  ths_kline        - 同花顺 K线 (含 amount, 200ms/rq, baostock 替代)
  tencent_quote    - 腾讯实时报价 (PE/PB/市值/换手率/涨跌停, 537 只/秒)

Design:
  - fetch_one + fetch_many standard interface
  - Fail returns empty dict/list, no exception (upstream decides warning)
  - Serial + sleep_between rate limit (ths 0.05s, tencent 0 - not rate-limited)
  - baostock still kept as fallback

Typical usage:
  from fetchers import ths_kline, tencent_quote

  k = ths_kline.fetch_one('600519', 'D', '20260701', '20260728')
  q = tencent_quote.fetch_one('600519')
  qs = tencent_quote.fetch_many(['600519', '000001', '688017'])

Verification:
  /tmp/hermes-verify-fetchers-2026-07-28.py — 10/10 PASS
  /tmp/hermes-verify-fetchers-full-2026-07-28.py — full module PASS
"""
from . import ths_kline, tencent_quote

__all__ = ["ths_kline", "tencent_quote"]
__version__ = "2026-07-28"
