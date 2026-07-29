"""腾讯财经实时报价 fetcher — 补 PE/PB/市值/换手率/涨跌停

端点: https://qt.gtimg.cn/q=sh600519,sz000001,bj920002
编码: GBK
字段: ~/.claude/skills/a-stock-data 内嵌 88 字段 (vendor from simonlin1212/a-stock-data)
    - 关键: 39=PE_TTM, 46=PB, 44=流通市值(亿), 45=总市值(亿)
    - 47=涨停价, 48=跌停价, 38=换手率%

依赖: stdlib only
"""
import time
import urllib.request

UA = "Mozilla/5.0"
SH_INDEX = {"000001", "000300", "000905", "000016", "000688", "000852", "000010"}


def _prefix(code: str) -> str:
    """给 code 加交易所前缀. 显式前缀透传 (避免 000001 上证/平安 歧义)."""
    low = code.lower()
    if low.startswith(("sh", "sz", "bj")):
        return low
    if code.startswith("92"):
        return f"bj{code}"
    if code in SH_INDEX or code.startswith(("5", "6", "9")):
        return f"sh{code}"
    if code.startswith(("4", "8")):
        return f"bj{code}"
    return f"sz{code}"


def _parse_line(line: str, key_of: dict) -> dict:
    """解析一行 ~ 分隔字段. (vals 索引 39=PE_TTM, 46=PB, 44=流通市值, 45=总市值)."""
    if "=" not in line or '"' not in line:
        return {}
    key = line.split("=")[0].split("_")[-1]
    vals = line.split('"')[1].split("~")
    if len(vals) < 53:
        return {}
    code = key_of.get(key, key[2:])  # 用入参原样做键
    return {
        "code": code,
        "name": vals[1],
        "price": float(vals[3]) if vals[3] else 0.0,
        "last_close": float(vals[4]) if vals[4] else 0.0,
        "open": float(vals[5]) if vals[5] else 0.0,
        "change_amt": float(vals[31]) if vals[31] else 0.0,
        "change_pct": float(vals[32]) if vals[32] else 0.0,
        "high": float(vals[33]) if vals[33] else 0.0,
        "low": float(vals[34]) if vals[34] else 0.0,
        "amount_wan": float(vals[37]) if vals[37] else 0.0,
        "turnover_pct": float(vals[38]) if vals[38] else 0.0,
        "pe_ttm": float(vals[39]) if vals[39] else 0.0,
        "amplitude_pct": float(vals[43]) if vals[43] else 0.0,
        "float_mcap_yi": float(vals[44]) if vals[44] else 0.0,  # 流通市值 (亿)
        "mcap_yi": float(vals[45]) if vals[45] else 0.0,        # 总市值 (亿)
        "pb": float(vals[46]) if vals[46] else 0.0,
        "limit_up": float(vals[47]) if vals[47] else 0.0,
        "limit_down": float(vals[48]) if vals[48] else 0.0,
        "vol_ratio": float(vals[49]) if vals[49] else 0.0,
        "pe_static": float(vals[52]) if vals[52] else 0.0,
    }


def fetch_one(code: str, retries: int = 3, retry_delay: float = 0.5) -> dict:
    """单只股票实时报价.

    code: 6-digit ticker or explicit prefix (sh/sz/bj).
    Returns dict with PE/PB/mcap/turnover/limit_up/limit_down. Empty dict on failure.
    """
    codes = [code]
    result = fetch_many(codes, retries=retries, retry_delay=retry_delay)
    return result.get(code, {})


def fetch_many(codes: list, retries: int = 3, retry_delay: float = 0.5) -> dict:
    """批量实时报价. code -> dict. 失败 code 给空 dict.

    腾讯不限速: 实测 5537 只一次 query ~ 6 秒 (vs 同花顺 200ms×5537=18分钟).
    """
    if not codes:
        return {}
    prefixed = [_prefix(c) for c in codes]
    key_of = {p: c for p, c in zip(prefixed, codes)}
    url = "https://qt.gtimg.cn/q=" + ",".join(prefixed)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=15) as r:
                data = r.read().decode("gbk")
            result = {}
            for line in data.strip().split(";"):
                row = _parse_line(line, key_of)
                if row:
                    result[row["code"]] = row
            # 失败 code 补空 dict
            for c in codes:
                result.setdefault(c, {})
            return result
        except Exception:
            if attempt < retries - 1:
                time.sleep(retry_delay)
    return {c: {} for c in codes}


if __name__ == "__main__":
    import sys
    args = sys.argv[1:] or ["600519", "000001", "688017", "920002"]
    out = fetch_many(args)
    for c, q in out.items():
        if q:
            print(f"  {q['name']}({c}): price={q['price']} PE_TTM={q['pe_ttm']} PB={q['pb']} "
                  f"市值={q['mcap_yi']}亿 换手={q['turnover_pct']}% 涨停={q['limit_up']} 跌停={q['limit_down']}")
        else:
            print(f"  {c}: ❌ 失败")
