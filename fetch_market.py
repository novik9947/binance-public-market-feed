#!/usr/bin/env python3
"""Read-only Binance Spot OHLCV collector. Public market data ONLY.

Runs on GitHub Actions; publishes summary.json plus market/<symbol>.json.
No account keys, Binance account endpoints, or trade execution.
"""
from __future__ import annotations
import concurrent.futures
import json
import math
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "LINKUSDT",
         "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "TRXUSDT", "UNIUSDT", "LTCUSDT",
         "NEARUSDT", "SUIUSDT", "AAVEUSDT", "APTUSDT", "DOTUSDT", "TONUSDT"]
API_BASE = "https://data-api.binance.vision"
DEST = pathlib.Path("market")
TIMEOUT = 18


def request(path: str, params: dict) -> object:
    url = API_BASE + path + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "BinancePublicMarketFeed/1.0", "Accept": "application/json"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as res:
                return json.load(res)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            if isinstance(exc, urllib.error.HTTPError) and exc.code in (400, 403, 404, 418, 429):
                raise RuntimeError(f"Binance HTTP {exc.code}") from None
            if attempt == 2:
                raise RuntimeError(f"Binance request failed: {type(exc).__name__}") from None
            time.sleep(attempt + 1)
    raise RuntimeError("Binance request failed")


def ema(values: list[float], n: int) -> float | None:
    if len(values) < n:
        return None
    v = sum(values[:n])/n
    for x in values[n:]:
        v += (x-v) * (2/(n+1))
    return v


def rsi(values: list[float], n: int = 14) -> float | None:
    if len(values) < n+1:
        return None
    changes = [values[i]-values[i-1] for i in range(1, len(values))]
    gain = sum(max(v, 0) for v in changes[:n])/n
    loss = sum(max(-v, 0) for v in changes[:n])/n
    for v in changes[n:]:
        gain = (gain*(n-1)+max(v, 0))/n
        loss = (loss*(n-1)+max(-v, 0))/n
    if loss == 0:
        return 50.0 if gain == 0 else 100.0
    return 100 - 100/(1+gain/loss)


def atr(bars: list[dict], n: int = 14) -> float | None:
    if len(bars) < n+1:
        return None
    ranges = []
    for a, b in zip(bars[:-1], bars[1:]):
        ranges.append(max(b["h"]-b["l"], abs(b["h"]-a["c"]), abs(b["l"]-a["c"])))
    v = sum(ranges[:n])/n
    for t in ranges[n:]:
        v = (v*(n-1)+t)/n
    return v


def clean_number(x: float | None, digits=8) -> float | None:
    return round(x, digits) if x is not None and math.isfinite(x) else None


def parse_closed(raw: object, now_ms: int) -> list[dict]:
    if not isinstance(raw, list):
        raise ValueError("Unexpected Binance response")
    bars=[]
    for row in raw:
        if not isinstance(row, list) or len(row) < 8:
            raise ValueError("Malformed candle")
        b = {"t":int(row[0]), "end":int(row[6]), "o":float(row[1]), "h":float(row[2]),
             "l":float(row[3]), "c":float(row[4]), "base_vol":float(row[5]), "quote_vol":float(row[7])}
        if b["end"] >= now_ms - 1000:
            continue  # NEVER include the still forming bar
        if not all(math.isfinite(v) for v in (b["o"], b["h"], b["l"], b["c"], b["base_vol"], b["quote_vol"])):
            raise ValueError("Non-finite candle")
        if min(b["o"],b["h"],b["l"],b["c"]) <= 0 or b["h"] < max(b["o"],b["c"],b["l"]) or b["l"] > min(b["o"],b["c"]):
            raise ValueError("Invalid price range")
        bars.append(b)
    if len(bars)<820:
        raise ValueError(f"Not enough closed 1h bars: {len(bars)}")
    for old, new in zip(bars[:-1], bars[1:]):
        if new["t"] - old["t"] != 3_600_000:
            raise ValueError("Missing hourly candles")
    return bars


def aggregate_four(bars: list[dict]) -> list[dict]:
    buckets={}
    for b in bars:
        start=b["t"] // 14_400_000 * 14_400_000
        buckets.setdefault(start, []).append(b)
    out=[]
    for start, group in sorted(buckets.items()):
        if len(group) != 4 or group[0]["t"] != start or group[-1]["t"] != start+10_800_000:
            continue
        out.append({"t":start, "end":group[-1]["end"], "o":group[0]["o"],
                    "h":max(b["h"] for b in group), "l":min(b["l"] for b in group),
                    "c":group[-1]["c"], "base_vol":sum(b["base_vol"] for b in group),
                    "quote_vol":sum(b["quote_vol"] for b in group)})
    return out


def indicator_set(bars: list[dict]) -> dict:
    closes=[x["c"] for x in bars]
    a,b,c=ema(closes,20),ema(closes,50),ema(closes,200)
    trend="up" if a is not None and b is not None and closes[-1]>a>b else (
          "down" if a is not None and b is not None and closes[-1]<a<b else "mixed")
    return {"trend":trend, "ema20":clean_number(a), "ema50":clean_number(b),
            "ema200":clean_number(c),"rsi14":clean_number(rsi(closes),2),
            "atr14":clean_number(atr(bars)),
            "last_closed_utc":datetime.fromtimestamp(bars[-1]["end"]/1000,timezone.utc).isoformat()}


def collect(symbol: str, now_ms: int) -> dict:
    raw=request("/api/v3/klines", {"symbol":symbol,"interval":"1h","limit":1000})
    b1=parse_closed(raw,now_ms)
    b4=aggregate_four(b1)
    if len(b4) < 210:
        raise ValueError(f"Too few aligned 4h bars: {len(b4)}")
    if now_ms - b1[-1]["end"] > 2*3_600_000:
        raise ValueError("Latest closed hour is more than two hours old")
    last=b1[-1]
    prev24=b1[-25]
    old4=b4[-21:-1]
    last4=b4[-1]
    avg4=sum(x["quote_vol"] for x in old4)/len(old4)
    data={
        "symbol":symbol,"market":"Binance Spot", "source":API_BASE+"/api/v3/klines",
        "as_of_utc":datetime.fromtimestamp(last["end"]/1000,timezone.utc).isoformat(),
        "last_closed_price":last["c"],
        "change24_pct":clean_number((last["c"]/prev24["c"]-1)*100,3),
        "quote_volume24h_usdt":clean_number(sum(x["quote_vol"] for x in b1[-24:]),2),
        "hour":indicator_set(b1), "four_hour":indicator_set(b4),
        "support_prev20_4h":clean_number(min(x["l"] for x in old4)),
        "resistance_prev20_4h":clean_number(max(x["h"] for x in old4)),
        "last_closed_4h_volume_ratio":clean_number(last4["quote_vol"]/avg4 if avg4>0 else None,3),
        "candles_1h":b1[-72:], "candles_4h":b4[-48:],
        "candle_policy":"closed-only; 4h built from 4 aligned closed 1h bars"
    }
    return data


def main() -> None:
    DEST.mkdir(exist_ok=True)
    now_ms=int(time.time()*1000)
    updated_at=datetime.fromtimestamp(now_ms/1000,timezone.utc).isoformat()
    results={}; errors={}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        future_map={pool.submit(collect,s,now_ms):s for s in PAIRS}
        for future in concurrent.futures.as_completed(future_map):
            sym=future_map[future]
            try:
                results[sym]=future.result()
                print("OK",sym,results[sym]["as_of_utc"],flush=True)
            except Exception as exc:
                errors[sym]=f"{type(exc).__name__}: {str(exc)[:120]}"
                print("ERROR",sym,errors[sym],flush=True)
                bad=DEST / (sym+".json")
                if bad.exists(): bad.unlink()
    if len(results)<4 or "BTCUSDT" not in results:
        raise SystemExit("Refusing to publish incomplete feed: need BTC and >=4 valid pairs")
    summary={"generated_at_utc":updated_at,"market":"Binance Spot", "source":API_BASE,
             "pairs_requested":len(PAIRS),"pairs_ok":len(results),"pairs_errors":errors,
             "timeframes":["1h","4h"],"closed_only":True,
             "safety":"Public delayed OHLCV only; not live executable bid/ask, margin borrowing or liquidation data.",
             "pairs":[]}
    for sym in PAIRS:
        if sym not in results: continue
        item=results[sym]
        (DEST/(sym+".json")).write_text(json.dumps(item,separators=(',',':'),allow_nan=False),encoding="utf-8")
        summary["pairs"].append({k:v for k,v in item.items() if not k.startswith("candles_")})
    pathlib.Path("summary.json").write_text(json.dumps(summary,indent=2,ensure_ascii=False,allow_nan=False)+"\n",encoding="utf-8")
    print("PUBLISHED",len(results),"of",len(PAIRS),flush=True)


def selftest() -> None:
    base=1_799_000_000_000 // 3_600_000 * 3_600_000
    candles=[[base+i*3_600_000,"100","102","99","101","123",base+(i+1)*3_600_000-1,"999"] for i in range(1000)]
    now=base+1000*3_600_000+15_000
    bars=parse_closed(candles,now)
    assert len(bars)==1000
    assert bars[-1]["end"] < now
    assert len(aggregate_four(bars)) >= 249
    assert round(rsi([float(x) for x in range(100)]),1)==100.0
    assert ema([1.0]*201,200)==1.0
    assert indicator_set(bars)["trend"]=="mixed"
    assert len(parse_closed(candles,base+999*3_600_000+15_000))==999
    print("Offline self-test passed")


if __name__=="__main__":
    if "--self-test" in sys.argv:selftest()
    else:main()
