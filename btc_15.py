#!/usr/bin/env python3
"""
btc_15m_live.py - live, self-tuning call on the current 15-minute BTC interval.

Every 2 seconds it reads the BTC price and says whether it thinks the current
quarter-hour interval (:00, :15, :30, :45) will END above or below the price
it STARTED at, with a probability. It also serves a live chart at
http://localhost:8000 (opened automatically), with a "Retrain now" button
that runs the self-tuning check on demand, and a confidence rating based
on how readings like the current one actually turned out in the past.

How it decides
  It compares the price now to the interval's starting price, then asks how
  likely the remaining time is to erase that gap, given how much BTC has been
  moving recently:

      P(up) = Phi( k * ln(now / start) / (volatility * sqrt(time left)) )

How it improves itself
  * It keeps a local database (btc_15m_<source>.db, next to where you run it)
    of 1-minute price history, every call it makes, and how each interval ended.
  * Once a day it builds candidate versions. A plain one picks the volatility
    window (30 min to 8 h) and the confidence adjustments k (early / mid / late
    in the interval) that would have given the most honest percentages. A
    path-aware one also mixes in the last 5 minutes' volatility and may add
    signals from how the price has moved. Direction signals: recent momentum,
    how steadily it has held its side, how often it crossed the start price,
    how much of its best lead it has given back, the trend over the last five
    intervals, and how many of them in a row finished on this side. Swing
    signals, which raise or lower confidence: a burst of movement in the last
    5 minutes, how choppy the interval has been minute to minute, how wide its
    range is, and how much the last five intervals swung. A signal is only
    used if it shows up clearly in the history, and is dropped when it fades.
  * The candidate is then tested on the most recent days, which it was NOT
    tuned on. It replaces the current version only if it scores better there.
    Otherwise the current version stays.
  * The page and the terminal show the score next to the original formula's,
    plus a live record of the calls made while the script was running.
  Tuning mostly changes how trustworthy the percentages are. With path signals
  in use, the call can also differ from the side the price is on right now,
  but only when the price is close to the start.

What else it records (for testing new signals later; none of it affects calls yet)
  Every 10 seconds, in the same database: best bid/ask and their sizes, how
  much was bought vs sold since the last snapshot, the BTC price on Kraken and
  Bitstamp, and (with --market) the market's bid/ask for "Up". In the last two
  minutes of each interval it also keeps every price reading, not one per 10 s.

Built to run for days
  Any error is retried with a growing pause, the reading rate slows itself
  down if the exchange says it is too fast, and outages are logged as gaps.
  Keep the computer awake (on a Mac: caffeinate -i python btc_15m_live.py).

Install
  pip install requests

Usage
  python btc_15m_live.py                       # Coinbase BTC-USD, every 2s
  python btc_15m_live.py --source binance      # Binance BTCUSDT (not in the US)
  python btc_15m_live.py --source binanceus
  python btc_15m_live.py --every 1             # read every second
  python btc_15m_live.py --log                 # one line per reading (default updates in place)
  python btc_15m_live.py --start-price 98596.1 # override the start price of the
                                               # interval in progress
  python btc_15m_live.py --history-days 180    # days of history to tune on (default 30); can be
                                               # raised later and the older data is downloaded
  python btc_15m_live.py --retrain-hours 12    # how often to re-tune (default 24)
  python btc_15m_live.py --market polymarket   # also record that market's odds for "Up"
  python btc_15m_live.py --market kalshi
  python btc_15m_live.py --no-record           # skip the extra 10-second recording
  python btc_15m_live.py --port 8080           # serve the chart on another port
  python btc_15m_live.py --no-web              # terminal only, no chart page

The first run downloads the history, which takes about a minute. Later runs
only fetch what is new. Stop with Ctrl+C. Delete the .db file to start over.
This is a statistical tool, not financial advice.
"""
import argparse
import bisect
import json
import math
import sqlite3
import threading
import time
import webbrowser
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

BAR = 900                          # seconds in a 15-minute interval
WINDOWS = (30, 60, 120, 240, 480)  # volatility windows (minutes) the tuner may choose from
BASE = {"id": 0, "window": 120, "k": [1.0, 1.0, 1.0]}   # the original, untuned formula
HOLDOUT = 0.2                      # most recent share of history kept back for testing
MARGIN = 0.0005                    # log-loss improvement a candidate needs to be adopted
MAX_TRAIN_DAYS = 90


def utc(ts, fmt="%H:%M"):
    return datetime.fromtimestamp(ts, timezone.utc).strftime(fmt)


# ----------------------------------------------------------------- data

class Feed:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers["User-Agent"] = "btc-15m-live/1.0"

    def get(self, url, **params):
        r = self.s.get(url, params=params, timeout=6)
        r.raise_for_status()
        return r.json()


class Coinbase(Feed):
    base = "https://api.exchange.coinbase.com/products/BTC-USD"

    def price(self):
        return float(self.get(self.base + "/ticker")["price"])

    def book(self):
        """Best bid and ask: (bid, bid_size, ask, ask_size)."""
        g = self.get(self.base + "/book", level=1)
        b, a = g["bids"][0], g["asks"][0]
        return float(b[0]), float(b[1]), float(a[0]), float(a[1])

    def flow(self, last_id):
        """BTC bought and sold at market since trade `last_id`: (bought, sold, trades, newest_id)."""
        rows = self.get(self.base + "/trades", limit=1000)
        new = [r for r in rows if last_id is not None and r["trade_id"] > last_id]
        # "side" is the resting order's side, so side == "sell" means the taker bought.
        return (sum(float(r["size"]) for r in new if r["side"] == "sell"),
                sum(float(r["size"]) for r in new if r["side"] == "buy"),
                len(new), max((r["trade_id"] for r in rows), default=last_id))

    def minutes(self, start, end):
        """1-minute candles as {open_time: (open, close)} for [start, end)."""
        out, t = {}, start
        while t < end:
            e = min(t + 299 * 60, end)     # API allows 300 candles per request
            rows = self.get(
                self.base + "/candles", granularity=60,
                start=datetime.fromtimestamp(t, timezone.utc).isoformat(),
                end=datetime.fromtimestamp(e, timezone.utc).isoformat(),
            )
            for row in rows:               # [time, low, high, open, close, volume]
                out[int(row[0])] = (float(row[3]), float(row[4]))
            t = e
            if t < end:
                time.sleep(0.15)
        return out


class Binance(Feed):
    def __init__(self, host):
        super().__init__()
        self.base = f"https://{host}/api/v3"

    def price(self):
        return float(self.get(self.base + "/ticker/price", symbol="BTCUSDT")["price"])

    def book(self):
        g = self.get(self.base + "/ticker/bookTicker", symbol="BTCUSDT")
        return float(g["bidPrice"]), float(g["bidQty"]), float(g["askPrice"]), float(g["askQty"])

    def flow(self, last_id):
        rows = self.get(self.base + "/aggTrades", symbol="BTCUSDT", limit=1000)
        new = [r for r in rows if last_id is not None and r["a"] > last_id]
        return (sum(float(r["q"]) for r in new if not r["m"]),      # m: the buyer was the resting order
                sum(float(r["q"]) for r in new if r["m"]),
                len(new), max((r["a"] for r in rows), default=last_id))

    def minutes(self, start, end):
        out, t = {}, start * 1000
        while t < end * 1000:
            rows = self.get(self.base + "/klines", symbol="BTCUSDT", interval="1m",
                            startTime=t, endTime=end * 1000 - 1, limit=1000)
            if not rows:
                break
            for r in rows:
                out[r[0] // 1000] = (float(r[1]), float(r[4]))
            t = rows[-1][0] + 60000
        return out


def make_feed(source):
    if source == "coinbase":
        return Coinbase()
    return Binance("api.binance.us" if source == "binanceus" else "api.binance.com")


def other_price(session, name):
    """BTC-USD last price on another exchange, to see how far price feeds disagree."""
    if name == "kraken":
        r = session.get("https://api.kraken.com/0/public/Ticker", params={"pair": "XBTUSD"}, timeout=6).json()
        return float(next(iter(r["result"].values()))["c"][0])
    r = session.get("https://www.bitstamp.net/api/v2/ticker/btcusd/", timeout=6)
    r.raise_for_status()
    return float(r.json()["last"])


def market_odds(session, market, start, cache):
    """(bid, ask) for "Up" in the 15-minute market starting at `start`, as probabilities 0-1."""
    if market == "polymarket":
        if start not in cache:
            ev = session.get("https://gamma-api.polymarket.com/events",
                             params={"slug": f"btc-updown-15m-{start}"}, timeout=6).json()
            m = ev[0]["markets"][0]
            unpack = lambda v: json.loads(v) if isinstance(v, str) else v
            outcomes = [o.lower() for o in unpack(m["outcomes"])]
            cache.clear()
            cache[start] = unpack(m["clobTokenIds"])[outcomes.index("up")]
        book = session.get("https://clob.polymarket.com/book", params={"token_id": cache[start]}, timeout=6).json()
        bids = [float(x["price"]) for x in book.get("bids", [])]
        asks = [float(x["price"]) for x in book.get("asks", [])]
        return (max(bids) if bids else None), (min(asks) if asks else None)
    r = session.get("https://api.elections.kalshi.com/trade-api/v2/markets",
                    params={"series_ticker": "KXBTC15M", "status": "open"}, timeout=6)
    r.raise_for_status()
    for m in r.json()["markets"]:
        closes = datetime.fromisoformat(m["close_time"].replace("Z", "+00:00")).timestamp()
        if abs(closes - (start + BAR)) < 60:
            return float(m["yes_bid_dollars"]), float(m["yes_ask_dollars"])
    raise LookupError("no open market for this interval")


# ---------------------------------------------------------------- model

def phi(x):
    """Standard normal CDF."""
    return 0.5 * math.erfc(-x / math.sqrt(2))


def phase(minutes_left):
    """Which confidence adjustment applies: 0 early, 1 mid, 2 late in the interval."""
    return 0 if minutes_left > 10 else 1 if minutes_left > 5 else 2


def sigma_per_min(candles, upto, window):
    """Typical 1-minute log move over the `window` minutes before `upto`."""
    closes = [candles[t][1] for t in range(upto - (window + 1) * 60, upto, 60) if t in candles]
    if len(closes) < max(min(20, window), window // 2):
        return None
    sq = [math.log(b / a) ** 2 for a, b in zip(closes, closes[1:])]
    return math.sqrt(sum(sq) / len(sq)) or None


def prob_up(start_price, price, sigma, minutes_left, k=1.0, tilt=0.0):
    """Probability the interval ends above its start price.
    tilt is the learned adjustment for how the price got here (0 = none)."""
    if minutes_left <= 0:
        return 1.0 if price > start_price else 0.0
    return phi(k * math.log(price / start_price) / (sigma * math.sqrt(minutes_left)) + tilt)


# ------------------------------------------------------------- learning

# Signals the tuner may use. TILT signals lean the projection up or down; SCALE
# signals describe how much the price is swinging and make the current lead
# count for more or less (that is, they raise or lower confidence).
TILT = ("momentum", "steady", "crossings", "giveback", "trend", "streak")
SCALE = ("burst", "chop", "range", "swing75")
FEATURES = TILT + SCALE
FAST = 5                           # minutes in the fast volatility measure
BLENDS = (0.0, 0.2, 0.4, 0.6)      # share of fast volatility the tuner may mix in
KEEP_T = 4.0                       # how clearly a path signal must show up in history to be kept


def clip(x, lim=4.0):
    return max(-lim, min(lim, x))


def blend(sig, sig_fast, share):
    """Mix the slow volatility with the last few minutes' volatility."""
    if not share or not sig_fast:
        return sig
    return math.sqrt((1 - share) * sig * sig + share * sig_fast * sig_fast)


def path_features(start_price, price, points, ref_price, lookback_min, sigma, minutes_left):
    """How the price got here, as four numbers (see FEATURES).
    points: the price at each whole minute of the interval so far, then the price now.
    momentum : move over the last few minutes, in typical-move units (its own direction)
    steady   : share of the interval spent on the side we are on now (-1..1, signed by side)
    crossings: how often it has crossed the start price (0..1, signed by side)
    giveback : how much of its best lead it has handed back, in typical-move units (signed by side)"""
    g = math.log(price / start_price)
    side = (g > 0) - (g < 0)
    mom = clip(math.log(price / ref_price) / (sigma * math.sqrt(lookback_min))) if lookback_min >= 0.5 else 0.0
    sides = [(p > start_price) - (p < start_price) for p in points]
    seq = [s for s in sides if s]
    cross = sum(1 for a, b in zip(seq, seq[1:]) if a != b)
    share = sum(1 for s in sides if s == side) / len(sides)
    lead = max(side * math.log(p / start_price) for p in points)
    return (mom, side * (2 * share - 1), side * min(cross, 4) / 4,
            clip(side * (lead - abs(g)) / (sigma * math.sqrt(minutes_left))))


def swing_state(sig_slow, sig_fast, sig75, chop, range_units):
    """How much the price is swinging, as four numbers (see SCALE). Zero means 'as usual'.
    burst  : last 5 minutes' swings compared with the usual
    chop   : how often it has reversed direction minute to minute in this interval
    range  : how wide this interval's high-to-low range is for the time elapsed
    swing75: the last five intervals' swings compared with the usual"""
    return (clip(math.log(sig_fast / sig_slow), 1.5) if sig_fast and sig_fast > 0 else 0.0,
            chop - 0.5,
            clip(range_units) - 1.6,
            clip(math.log(sig75 / sig_slow), 1.5) if sig75 and sig75 > 0 else 0.0)


def build_samples(candles):
    """One sample per (past interval, minute into it). Fields:
    0 interval, 1 minute, 2 gap / sqrt(time left), 3 ended up?, 4 volatility under each
    window, 5 fast volatility, 6 momentum, 7 steady, 8 crossings, 9 giveback, 10 chop,
    11 range, 12 volatility over the previous five intervals, 13 trend over them,
    14 streak (6, 9, 11 and 13 still to be divided by volatility)."""
    lo, hi = min(candles), max(candles)
    n = (hi - lo) // 60 + 1
    opn, close, have, last = [0.0] * n, [0.0] * n, [False] * n, None
    for i in range(n):
        v = candles.get(lo + 60 * i)
        if v:
            opn[i], close[i], have[i], last = v[0], v[1], True, v[1]
        else:
            opn[i] = close[i] = last           # rare missing minute: carry the price
    pre = [0.0, 0.0]                           # running sum of squared 1-minute moves
    for i in range(1, n):
        pre.append(pre[-1] + math.log(close[i] / close[i - 1]) ** 2)

    samples = []
    first = max(WINDOWS) + 1
    first += (-(lo // 60 + first)) % 15        # align to a quarter-hour
    for i in range(first, n - 14, 15):
        if not all(have[i:i + 15]):
            continue
        start_price, up = opn[i], close[i + 14] > opn[i]
        prev = cross = pos = neg = rev = prev_move = 0
        hi_g = lo_g = 0.0
        sig75 = math.sqrt((pre[i] - pre[i - 75]) / 75)            # swings over the previous five intervals
        trend = math.log(start_price / close[i - 76]) / math.sqrt(75)
        earlier = [close[i - 15 * k + 14] > opn[i - 15 * k] for k in range(1, 6)]   # did they finish up?
        for m in range(1, 15):
            j = i + m - 1                      # close[j] = price m minutes in
            g = math.log(close[j] / start_price)
            side = (g > 0) - (g < 0)
            if side:
                cross += bool(prev) and side != prev
                prev = side
                pos += side > 0
                neg += side < 0
            hi_g, lo_g = max(hi_g, g), min(lo_g, g)
            move = close[j] - (start_price if m == 1 else close[j - 1])
            move = (move > 0) - (move < 0)
            if move:
                rev += bool(prev_move) and move != prev_move
                prev_move = move
            if not side:
                continue
            streak = 0
            for was_up in earlier:
                if was_up != (side > 0):
                    break
                streak += 1
            sig = tuple(math.sqrt((pre[j + 1] - pre[j + 1 - w]) / w) for w in WINDOWS)
            if min(sig) <= 0:
                continue
            left, lb = 15 - m, min(m, 3)
            ref = start_price if lb == m else close[j - lb]
            samples.append((lo + 60 * i, m, g / math.sqrt(left), up, sig,
                            math.sqrt((pre[j + 1] - pre[j + 1 - FAST]) / FAST),
                            math.log(close[j] / ref) / math.sqrt(lb),
                            side * (2 * (pos if side > 0 else neg) / m - 1),
                            side * min(cross, 4) / 4,
                            side * ((hi_g if side > 0 else -lo_g) - abs(g)) / math.sqrt(left),
                            rev / (m - 1) if m > 1 else 0.5, (hi_g - lo_g) / math.sqrt(m),
                            sig75, trend, side * streak / 5))
    return samples


def sample_sigma(model, s):
    return blend(s[4][WINDOWS.index(model["window"])], s[5], model.get("fast", 0))


def sample_features(model, s):
    """(gap in typical-move units, the signal values in FEATURES order) for one sample."""
    slow = s[4][WINDOWS.index(model["window"])]
    sg = blend(slow, s[5], model.get("fast", 0))
    z = s[2] / sg
    zc = clip(z, 6)
    g = swing_state(slow, s[5], s[12], s[10], s[11] / sg)
    return z, (clip(s[6] / sg), s[7], s[8], clip(s[9] / sg), clip(s[13] / sg), s[14],
               zc * g[0], zc * g[1], zc * g[2], zc * g[3])


def sample_score(model, s):
    """The number that goes into the bell curve for one historical sample."""
    b = model.get("beta")
    if not b:
        return model["k"][phase(15 - s[1])] * s[2] / sample_sigma(model, s)
    z, f = sample_features(model, s)
    return model["k"][phase(15 - s[1])] * z + sum(b[n] * f[i] for i, n in enumerate(FEATURES) if n in b)


def score(pairs):
    """Score (probability of up, went up?) pairs.
    ece = average gap between the stated confidence and how often it was right."""
    ll, hits, b = 0.0, 0, [[0.0, 0, 0] for _ in range(5)]
    for p, up in pairs:
        p = min(max(p, 1e-6), 1 - 1e-6)
        ll -= math.log(p if up else 1 - p)
        conf, hit = max(p, 1 - p), (p > 0.5) == bool(up)
        hits += hit
        x = b[min(int((conf - 0.5) * 10), 4)]
        x[0] += conf
        x[1] += hit
        x[2] += 1
    n = len(pairs)
    return {"n": n, "ll": ll / n, "acc": hits / n,
            "ece": sum(abs(c - h) for c, h, _ in b) / n,
            "buckets": [[h, c] for _, h, c in b]}


def evaluate(model, samples):
    by_min, pairs = {m: [0, 0] for m in range(1, 15)}, []
    for s in samples:
        p = phi(sample_score(model, s))
        pairs.append((p, s[3]))
        by_min[s[1]][0] += (p > 0.5) == s[3]
        by_min[s[1]][1] += 1
    out = score(pairs)
    out["by_min"] = {m: h / t for m, (h, t) in by_min.items() if t}
    return out


def track_record(model, samples):
    """How past readings turned out. grid: by minute into the interval and stated
    confidence (50-60% ... 90%+), [times right, times seen]. cross: what happened
    after a reading first reached 60 / 70 / 80 / 90% in an interval."""
    grid = {m: [[0, 0] for _ in range(5)] for m in range(1, 15)}
    cross = {level: [0, 0] for level in (60, 70, 80, 90)}
    swing = {name: [[0, 0] for _ in range(5)] for name in ("quiet", "normal", "choppy")}
    wi, interval, seen = WINDOWS.index(model["window"]), None, set()
    for s in samples:
        if s[0] != interval:
            interval, seen = s[0], set()
        p = phi(sample_score(model, s))
        conf, hit = max(p, 1 - p), (p > 0.5) == s[3]
        ratio = s[5] / s[4][wi]                  # last 5 minutes' swings vs the usual
        for cell in (grid[s[1]][min(int((conf - 0.5) * 10), 4)],
                     swing["quiet" if ratio < 0.7 else "normal" if ratio <= 1.3 else "choppy"][min(int((conf - 0.5) * 10), 4)]):
            cell[0] += hit
            cell[1] += 1
        for level in cross:
            if level not in seen and conf * 100 >= level:
                seen.add(level)
                cross[level][0] += hit
                cross[level][1] += 1
    return {"grid": grid, "cross": cross, "swing": swing}


def fit_k(pairs):
    """Confidence multiplier k that best fits P = Phi(k*a + b) over (a, b) pairs
    already signed so that a positive total means 'right'."""
    f = lambda k: sum(math.log(max(phi(k * a + b), 1e-6)) for a, b in pairs)
    a, b, g = 0.3, 2.5, (math.sqrt(5) - 1) / 2
    c, d = b - g * (b - a), a + g * (b - a)
    fc, fd = f(c), f(d)
    for _ in range(20):                        # golden-section search (f is concave in k)
        if fc > fd:
            b, d, fd = d, c, fc
            c = b - g * (b - a)
            fc = f(c)
        else:
            a, c, fc = c, d, fd
            d = a + g * (b - a)
            fd = f(d)
    return (a + b) / 2, max(fc, fd)


def fit_ks(tr, model):
    """Best early / mid / late confidence multipliers for this model's volatility and tilt."""
    tilt_only, groups = {**model, "k": [0.0, 0.0, 0.0]}, [[], [], []]
    for s in tr:
        q = 1 if s[3] else -1
        groups[phase(15 - s[1])].append((q * s[2] / sample_sigma(model, s), q * sample_score(tilt_only, s)))
    fits = [fit_k(g) for g in groups]
    return [round(k, 3) for k, _ in fits], sum(ll for _, ll in fits)


def solve(A, b):
    """Solve A x = b for a small matrix."""
    n = len(b)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(M[r][c]))
        M[c], M[p] = M[p], M[c]
        for r in range(c + 1, n):
            f = M[r][c] / M[c][c]
            for k in range(c, n + 1):
                M[r][k] -= f * M[c][k]
    x = [0.0] * n
    for r in range(n - 1, -1, -1):
        x[r] = (M[r][n] - sum(M[r][k] * x[k] for k in range(r + 1, n))) / M[r][r]
    return x


def fit_beta(tr, model, names):
    """Weights for the named path signals (Newton's method), holding the rest of the
    model fixed. Returns {name: (weight, how clearly it shows up in the data)}."""
    idx, n, plain, rows = [FEATURES.index(x) for x in names], len(names), {**model, "beta": {}}, []
    for s in tr:
        f = sample_features(model, s)[1]
        rows.append((1 if s[3] else -1, sample_score(plain, s), [f[i] for i in idx]))
    beta = [0.0] * n
    for _ in range(12):
        g = [-b for b in beta]                                   # light ridge penalty keeps it stable
        H = [[float(i == j) for j in range(n)] for i in range(n)]
        for q, off, x in rows:
            sc = clip(off + sum(b * v for b, v in zip(beta, x)), 8)
            lam = q * math.exp(-sc * sc / 2) / 2.5066282746310002 / max(phi(q * sc), 1e-12)
            w = lam * (lam + sc)
            for i in range(n):
                g[i] += lam * x[i]
                for j in range(i, n):
                    H[i][j] += w * x[i] * x[j]
        for i in range(n):
            for j in range(i):
                H[i][j] = H[j][i]
        step = solve(H, g)
        beta = [b + d for b, d in zip(beta, step)]
        if max(abs(d) for d in step) < 1e-4:
            break
    out = {}
    for i, name in enumerate(names):
        var = solve(H, [float(i == j) for j in range(n)])[i]
        out[name] = (round(beta[i], 4), beta[i] / math.sqrt(var) if var > 0 else 0.0)
    return out


def train(candles, champion):
    """Build candidates on older history, then test them, the current version and
    the original formula on the most recent days none of them were tuned on."""
    cutoff = time.time() - MAX_TRAIN_DAYS * 86400
    samples = build_samples({t: v for t, v in candles.items() if t >= cutoff})
    if len(samples) < 3000:
        return None
    cut = samples[int(len(samples) * (1 - HOLDOUT))][0]
    tr = [s for s in samples if s[0] < cut]
    ho = [s for s in samples if s[0] >= cut]

    # 1. Plain candidate: best volatility window and confidence multipliers.
    best = None
    for w in WINDOWS:
        ks, ll = fit_ks(tr, {"window": w, "k": [1.0, 1.0, 1.0]})
        if best is None or ll > best[0]:
            best = (ll, {"window": w, "k": ks})
    plain = best[1]

    # 2. Path-aware candidate: mix in the last few minutes' volatility...
    for share in BLENDS[1:]:
        ks, ll = fit_ks(tr, {**plain, "fast": share})
        if ll > best[0]:
            best = (ll, {**plain, "k": ks, "fast": share})
    path = dict(best[1])
    # ...then add whichever path signals show up clearly in the history.
    signals = fit_beta(tr, path, FEATURES)
    kept = [name for name in FEATURES if abs(signals[name][1]) >= KEEP_T]
    if kept:
        path["beta"] = {name: b for name, (b, _) in fit_beta(tr, path, kept).items()}
        path["k"], _ = fit_ks(tr, path)

    res = {"at": time.time(), "n_train": len(tr), "train_days": (cut - samples[0][0]) / 86400,
           "hold_days": (samples[-1][0] + BAR - cut) / 86400,
           "signals": {name: [b, round(t, 1), name in kept] for name, (b, t) in signals.items()},
           "plain": evaluate(plain, ho), "cur": evaluate(champion, ho), "base": evaluate(BASE, ho),
           "path": evaluate(path, ho) if path != plain else None}
    use_path = res["path"] is not None and res["path"]["ll"] < res["plain"]["ll"]
    res["challenger"], res["new"] = (path, res["path"]) if use_path else (plain, res["plain"])
    res["adopted"] = res["new"]["ll"] < res["cur"]["ll"] - MARGIN
    res["track"] = track_record(res["challenger"] if res["adopted"] else champion, ho)
    return res


# ------------------------------------------------------------- database

def open_db(path):
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")      # lets the recorder thread write alongside the main loop
    db.executescript("""
        CREATE TABLE IF NOT EXISTS candles (t INTEGER PRIMARY KEY, o REAL, c REAL);
        CREATE TABLE IF NOT EXISTS models (id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL,
            window INTEGER, k TEXT, adopted INTEGER, report TEXT);
        CREATE TABLE IF NOT EXISTS readings (t REAL, start INTEGER, left REAL,
            p REAL, p_base REAL, model INTEGER, up INTEGER);
        CREATE INDEX IF NOT EXISTS readings_start ON readings (start);
        CREATE TABLE IF NOT EXISTS flow (t REAL, start INTEGER, bid REAL, bid_size REAL, ask REAL,
            ask_size REAL, buy_vol REAL, sell_vol REAL, trades INTEGER, kraken REAL, bitstamp REAL,
            mkt_bid REAL, mkt_ask REAL);
        CREATE TABLE IF NOT EXISTS gaps (a REAL, b REAL, reason TEXT);
    """)
    have = {r[1] for r in db.execute("PRAGMA table_info(readings)")}
    for col in ("price REAL", "start_price REAL", "sigma REAL", "fine INTEGER DEFAULT 0", "hi REAL", "lo REAL"):
        if col.split()[0] not in have:         # databases made by earlier versions
            db.execute(f"ALTER TABLE readings ADD COLUMN {col}")
    if "extra" not in {r[1] for r in db.execute("PRAGMA table_info(models)")}:
        db.execute("ALTER TABLE models ADD COLUMN extra TEXT")
    db.commit()
    return db


def store_candles(db, rows, now):
    db.executemany("INSERT OR REPLACE INTO candles VALUES (?,?,?)",
                   [(t, o, c) for t, (o, c) in rows.items() if t + 60 <= now])   # finished minutes only
    db.commit()


def sync_history(feed, db, candles, days):
    """Download whatever is missing: newer minutes since the last run, and older
    minutes if more history is asked for than is stored."""
    now = int(time.time()) // 60 * 60
    want = now - days * 86400
    spans = [(want, now)]
    if candles and max(candles) > want:
        spans = [(max(candles) - 120, now)]
        if min(candles) > want + 3600:
            spans.insert(0, (want, min(candles)))
    total = sum(b - a for a, b in spans)
    big, done = total > 6 * 3600, 0
    if big:
        print(f"Downloading {total / 86400:.1f} days of 1-minute history "
              "(about a minute per 30 days; later starts only fetch what is new)...")
    for a, end in spans:
        while a < end:
            b = min(a + 43200, end)
            rows = feed.minutes(a, b)
            candles.update(rows)
            store_candles(db, rows, now)
            done, a = done + b - a, b
            if big:
                print(f"\r  {100 * done // total}%", end="", flush=True)
    if big:
        print()


def backfill_outcomes(db, candles):
    """Score calls from earlier sessions whose interval ended after the script stopped."""
    now = time.time()
    for (start,) in db.execute("SELECT DISTINCT start FROM readings WHERE up IS NULL").fetchall():
        if start + BAR <= now and start in candles and start + 840 in candles:
            db.execute("UPDATE readings SET up=? WHERE start=?",
                       (int(candles[start + 840][1] > candles[start][0]), start))
    db.commit()


def load_model(db):
    row = db.execute("SELECT id, window, k, extra FROM models WHERE adopted=1 ORDER BY id DESC LIMIT 1").fetchone()
    model = {"id": row[0], "window": row[1], "k": json.loads(row[2]), **json.loads(row[3] or "{}")} if row else dict(BASE)
    return model, db.execute("SELECT MAX(at) FROM models").fetchone()[0] or 0


def live_stats(db):
    """How the calls actually made while running have turned out (last 30 days)."""
    rows = db.execute("SELECT p, p_base, up FROM readings WHERE up IS NOT NULL AND fine = 0 AND t > ?",
                      (time.time() - 30 * 86400,)).fetchall()
    if not rows:
        return None
    mine, base = score([(p, u) for p, _, u in rows]), score([(pb, u) for _, pb, u in rows])
    n = db.execute("SELECT COUNT(DISTINCT start) FROM readings WHERE up IS NOT NULL AND t > ?",
                   (time.time() - 30 * 86400,)).fetchone()[0]
    since = db.execute("SELECT MIN(t) FROM readings").fetchone()[0]
    gap = db.execute("SELECT COALESCE(SUM(b - a), 0) FROM gaps").fetchone()[0]
    return {"intervals": n, "acc": mine["acc"], "ece": mine["ece"], "base_ece": base["ece"],
            "since": since, "gap_hours": gap / 3600}


def learn_info(db, model):
    row = db.execute("SELECT at, adopted, report FROM models ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        return None
    tuned = db.execute("SELECT at FROM models WHERE id=?", (model["id"],)).fetchone()
    return {"version": db.execute("SELECT COUNT(*) FROM models WHERE adopted=1").fetchone()[0],
            "tuned_at": tuned[0] if tuned else None, "checked_at": row[0], "kept_old": not row[1],
            "window": model["window"], "k": model["k"], "fast": model.get("fast", 0),
            "beta": model.get("beta", {}), **json.loads(row[2]), "live": live_stats(db)}


def describe(model):
    k, text = model["k"], f"{model['window']}-minute volatility"
    if model.get("fast"):
        text += f" mixed with {model['fast']:.0%} of the last {FAST} minutes'"
    text += f", confidence x{k[0]:.2f} early / x{k[1]:.2f} mid / x{k[2]:.2f} late"
    if model.get("beta"):
        text += ", path signals: " + ", ".join(f"{n} {b:+.2f}" for n, b in model["beta"].items())
    return text


def record_training(db, res, model):
    """Save a tuning check, switch to the candidate if it won, and report."""
    new, active = res["challenger"], res["new"] if res["adopted"] else res["cur"]
    report = {"ece": active["ece"], "base_ece": res["base"]["ece"], "hold_days": res["hold_days"],
              "by_min": active["by_min"], "n_train": res["n_train"], "signals": res["signals"], **res["track"]}
    cur = db.execute("INSERT INTO models (at, window, k, adopted, report, extra) VALUES (?,?,?,?,?,?)",
                     (res["at"], new["window"], json.dumps(new["k"]), int(res["adopted"]), json.dumps(report),
                      json.dumps({x: new[x] for x in ("fast", "beta") if x in new})))
    db.commit()
    if res["adopted"]:
        model = {"id": cur.lastrowid, **new}

    cols = [("original", "base"), ("current", "cur"), ("plain", "plain")] + ([("path-aware", "path")] if res["path"] else [])
    print(f"\nSelf-tuning check: tuned on {res['train_days']:.1f} days, tested on the most recent "
          f"{res['hold_days']:.1f} days it did not see")
    print(f"  {'':<30}" + "".join(f"{name:>12}" for name, _ in cols))
    print(f"  {'Percentages off by (points)':<30}" + "".join(f"{100 * res[x]['ece']:>12.1f}" for _, x in cols))
    print(f"  {'Log loss (lower is better)':<30}" + "".join(f"{res[x]['ll']:>12.4f}" for _, x in cols))
    kept = [f"{n} {b:+.3f}" for n, (b, _, keep) in res["signals"].items() if keep]
    print("  Signals clear enough to try: " + (", ".join(kept) or "none"))
    print("  Too weak, dropped: " + (", ".join(n for n, v in res["signals"].items() if not v[2]) or "none"))
    if res["adopted"]:
        print(f"  -> Adopted the {'path-aware' if new is not None and ('beta' in new or 'fast' in new) else 'plain'} "
              f"candidate: {describe(model)}.")
    else:
        print("  -> Kept the current version: no candidate was better on unseen days.")
    print("  After a reading first reached: " + "  ".join(
        f"{level}% -> right {h / n:.0%}" for level, (h, n) in res["track"]["cross"].items() if n))
    print("  Up/down call right, by minutes into the interval: " + "  ".join(
        f"{m}m {active['by_min'][m]:.0%}" for m in (1, 3, 5, 8, 10, 12, 14) if m in active["by_min"]) + "\n")
    publish(learn=learn_info(db, model))
    return model


# ----------------------------------------------------------------- live

SNAP = {"price": None, "ticks": [], "results": [], "learn": None, "error": None}


SNAP_LOCK = threading.Lock()
RETRAIN = threading.Event()        # set by the page's "Retrain now" button


def publish(**kw):
    """Update what the chart page sees (replaced whole, so readers never see half a write)."""
    global SNAP
    with SNAP_LOCK:
        SNAP = {**SNAP, **kw}


def recorder(args, path):
    """Runs beside the main loop. Every 10 seconds it saves the order book, trade
    flow, other exchanges' prices and (optionally) market odds for later testing.
    Nothing here affects the calls, and a failing source never stops the others."""
    db, feed, last_id, cache, warned = open_db(path), make_feed(args.source), None, {}, set()
    while True:
        t = time.time()
        try:
            start, row, ok = int(t) // BAR * BAR, {}, {}

            def grab(name, fn):
                try:
                    row.update(fn())
                    ok[name] = True
                    warned.discard(name)
                except Exception as e:
                    ok[name] = False
                    if name not in warned:     # say so once, not every 10 seconds
                        warned.add(name)
                        print(f"\n(recording) {name} not available right now: {e}")

            grab("order book", lambda: dict(zip(("bid", "bid_size", "ask", "ask_size"), feed.book())))
            if "trade flow" in warned:
                last_id = None                 # after an outage, start counting afresh
            flow = {}
            grab("trade flow", lambda: flow.update(zip(("buy_vol", "sell_vol", "trades", "id"), feed.flow(last_id))) or {})
            if flow:
                if last_id is not None:
                    row.update({k: flow[k] for k in ("buy_vol", "sell_vol", "trades")})
                last_id = flow["id"]
            grab("Kraken price", lambda: {"kraken": other_price(feed.s, "kraken")})
            grab("Bitstamp price", lambda: {"bitstamp": other_price(feed.s, "bitstamp")})
            if args.market:
                grab(f"{args.market} odds", lambda: dict(zip(("mkt_bid", "mkt_ask"),
                                                             market_odds(feed.s, args.market, start, cache))))
                publish(market={"name": args.market, "bid": row.get("mkt_bid"), "ask": row.get("mkt_ask"), "start": start})
            cols = ["t", "start"] + list(row)
            db.execute(f"INSERT INTO flow ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                       [t, start] + list(row.values()))
            db.commit()
            publish(recording=ok)
        except Exception as e:
            print(f"\n(recording) error, will retry: {e}")
        time.sleep(max(1.0, 10 - (time.time() - t)))


def outcome(start_price, end_price):
    return "UP" if end_price > start_price else "DOWN" if end_price < start_price else "FLAT"


def live(feed, args, db, candles, model, last_check):
    cur = None           # start time of the interval being tracked
    start_price = None
    exact = False        # False while start_price is only a stand-in
    override = args.start_price
    sig = sig_base = sig_fast = sig75 = None
    earlier = []         # did each of the previous five intervals finish up? (most recent first)
    hi10 = lo10 = None   # highest and lowest reading since the last saved one
    marks = []           # price at each whole minute of the current interval
    prev = None          # previous reading
    sig_at = logged_at = 0
    price = None
    ticks = []           # (time, price) readings in the current interval
    results = []         # recently finished intervals
    seeded = filled = False
    trainer = {"thread": None, "result": None}
    pace = args.every    # seconds between readings; raised if the exchange says we are too fast
    fails = 0            # consecutive failed readings (drives the retry pause)
    fine_at = 0
    last_err = None
    last_ok = db.execute("SELECT MAX(t) FROM readings").fetchone()[0]   # so downtime shows up as a gap

    def start_training():
        """Tune a candidate in the background; the loop adopts it (or not) when it finishes."""
        nonlocal last_check
        snapshot, champion, last_check = dict(candles), model, time.time()
        trainer["result"] = None

        def job():
            try:
                trainer["result"] = train(snapshot, champion)
            except Exception as e:
                print(f"\nSelf-tuning check failed, will try again later: {e}")

        trainer["thread"] = threading.Thread(target=job, daemon=True)
        trainer["thread"].start()
        publish(training=True)

    print(f"Reading BTC every {args.every:g}s. Times are UTC. Ctrl+C to stop.\n")
    while True:
        tick = time.time()
        try:
            if trainer["thread"] and not trainer["thread"].is_alive():   # a re-tune just finished
                trainer["thread"] = None
                if trainer["result"]:
                    model = record_training(db, trainer["result"], model)
                    sig_at = 0
                publish(training=False, train_note=None if trainer["result"] else
                        "Last retrain did not finish: not enough history yet, or an error (see terminal).")
            if RETRAIN.is_set():                                         # button on the page
                RETRAIN.clear()
                if not trainer["thread"]:
                    print("\nRetrain requested from the page...")
                    start_training()

            start = int(tick) // BAR * BAR
            if start != cur:
                if cur is not None and price is not None and start_price is not None:
                    if last_ok and cur + BAR - last_ok <= 15:    # we saw the price right up to the end
                        print(("" if args.log else "\n") +
                              f"== {utc(cur)}-{utc(cur + BAR)} finished {outcome(start_price, price)}: "
                              f"start {start_price:,.2f}, end {price:,.2f} "
                              f"({price / start_price - 1:+.3%})\n")
                        results = (results + [{"start": cur, "result": outcome(start_price, price)}])[-12:]
                        db.execute("UPDATE readings SET up=? WHERE start=?", (int(price > start_price), cur))
                        db.commit()
                    backfill_outcomes(db, candles)               # intervals that ended during an outage
                    if SNAP["learn"]:
                        publish(learn={**SNAP["learn"], "live": live_stats(db)})
                    if not trainer["thread"] and tick - last_check > args.retrain_hours * 3600:
                        start_training()
                filled = cur is not None     # watched from the first second: nothing to backfill
                cur, start_price, exact, ticks, marks = start, None, False, [], []
                if override:
                    start_price, exact, override = override, True, None

            # Once a minute: fetch the newest 1-minute candles, save them, and
            # re-measure volatility. Also look up the interval's true opening
            # price until the exchange has published it.
            if tick - sig_at > 60 or not exact:
                since = int(tick) - 290 * 60
                if candles:
                    since = max(since, max(candles) - 180)
                recent = feed.minutes(since // 60 * 60, int(tick) + 60)
                candles.update(recent)
                store_candles(db, recent, tick)
                upto = int(tick) // 60 * 60 + 60
                sig_base = sigma_per_min(candles, upto, BASE["window"]) or sig_base
                sig = sigma_per_min(candles, upto, model["window"]) or sig_base
                sig_fast = sigma_per_min(candles, upto, FAST) or sig_fast
                sig75 = sigma_per_min(candles, cur, 75) or sig75
                earlier = []
                for T in range(cur - BAR, cur - 6 * BAR, -BAR):
                    if T not in candles or T + 840 not in candles:
                        break
                    earlier.append(candles[T + 840][1] > candles[T][0])
                sig_at = tick
                if not exact and cur in candles:
                    start_price, exact = candles[cur][0], True
                if not seeded:               # how the last few intervals ended
                    results = [{"start": T, "result": outcome(candles[T][0], candles[T + 840][1])}
                               for T in range(cur - 8 * BAR, cur, BAR)
                               if T in candles and T + 840 in candles]
                    seeded = True
                if not filled and cur in candles:   # started mid-interval: draw the part we missed
                    ticks = ([(cur, candles[cur][0])] +
                             [(t + 60, candles[t][1]) for t in range(cur, int(tick), 60)
                              if t in candles and t + 60 <= tick] + ticks)
                    marks = [candles[t][1] for t in range(cur, int(tick), 60) if t in candles and t + 60 <= tick]
                    filled = True

            prev, price = price, feed.price()
            if start_price is None:
                start_price = price          # stand-in until the real open arrives
            if sig is None:
                raise RuntimeError("no volatility data yet")
            ticks.append((round(tick, 1), price))
            if last_ok and tick - last_ok > 30:      # we were blind for a while: note it
                db.execute("INSERT INTO gaps VALUES (?,?,?)", (last_ok, tick, last_err or "script not running"))
                db.commit()
            last_ok, last_err, fails = tick, None, 0

            while len(marks) < int((tick - cur) // 60):          # a whole minute has passed
                marks.append(prev if prev is not None else price)

            left = cur + BAR - time.time()
            sg = blend(sig, sig_fast, model.get("fast", 0))
            k_now, tilt, beta = model["k"][phase(left / 60)], 0.0, model.get("beta")
            if beta and price != start_price and left > 0:      # how the price got here, and how much it is swinging
                elapsed = tick - cur
                lb = min(elapsed, 180.0)
                ref = start_price if lb < 180 else ticks[min(bisect.bisect_left(ticks, (tick - lb,)), len(ticks) - 1)][1]
                side_up, back = price > start_price, candles.get(cur - 76 * 60)
                vals = dict(zip(TILT, path_features(start_price, price, marks + [price], ref, lb / 60, sg, max(left / 60, 1.0))))
                vals["trend"] = clip(math.log(start_price / back[1]) / math.sqrt(75) / sg) if back else 0.0
                streak = next((i for i, was_up in enumerate(earlier) if was_up != side_up), len(earlier))
                vals["streak"] = (1 if side_up else -1) * streak / 5
                tilt = sum(beta[n] * vals[n] for n in TILT if n in beta)
                seq = [start_price] + marks + [price]
                moves = [m for m in ((b > a) - (b < a) for a, b in zip(seq, seq[1:])) if m]
                chop = sum(1 for a, b in zip(moves, moves[1:]) if a != b) / (len(seq) - 2) if len(seq) > 2 else 0.5
                gaps = [math.log(p / start_price) for p in seq]
                g = swing_state(sig, sig_fast, sig75, chop, (max(gaps) - min(gaps)) / math.sqrt(max(elapsed / 60, 1.0)) / sg)
                k_now = max(0.2, k_now + sum(beta[n] * v for n, v in zip(SCALE, g) if n in beta))
            p = prob_up(start_price, price, sg, left / 60, k_now, tilt)
            hi10, lo10 = max(price, hi10 or price), min(price, lo10 or price)
            # Keep a record to score later: one reading per 10 s, plus every
            # reading (up to 2 a second) in the last two minutes.
            coarse = tick - logged_at >= 10
            if exact and sig_base and (coarse or (left <= 120 and tick - fine_at >= 0.5)):
                db.execute("INSERT INTO readings (t, start, left, p, p_base, model, price, start_price, sigma, fine, hi, lo) "
                           "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                           (tick, cur, left, p, prob_up(start_price, price, sig_base, left / 60), model["id"],
                            price, start_price, sg, 0 if coarse else 1,
                            hi10 if coarse else None, lo10 if coarse else None))
                db.commit()
                fine_at = tick
                if coarse:
                    logged_at, hi10, lo10 = tick, None, None
            publish(now=tick, start=cur, end=cur + BAR, start_price=start_price, exact=exact,
                    price=price, sigma=sg, k=model["k"], k_now=k_now, tilt=tilt, p_up=p,
                    swing=(sig_fast / sig) if sig_fast and sig else None, ticks=list(ticks),
                    results=results, error=None)
            call = f"UP {p:.0%}" if p >= 0.5 else f"DOWN {1 - p:.0%}"
            line = (f"{utc(tick, '%H:%M:%S')}  {utc(cur)}-{utc(cur + BAR)}  "
                    f"start {'' if exact else '~'}{start_price:,.2f}  "
                    f"now {price:,.2f} ({price / start_price - 1:+.3%})  "
                    f"left {int(left) // 60}:{int(left) % 60:02d}  ->  {call}")
            print(line if args.log else "\r" + line.ljust(100), end="\n" if args.log else "", flush=True)
        except Exception as e:                   # never let one bad reading end a week-long run
            fails += 1
            last_err = str(e)[:200]
            publish(error=last_err)
            print(("" if args.log else "\n") + f"{utc(tick, '%H:%M:%S')}  read failed "
                  f"(retrying in {min(30, 2 ** min(fails, 5) - 1)}s): {last_err}")
            if getattr(getattr(e, "response", None), "status_code", None) == 429 and pace < 5:
                pace = min(5.0, max(0.5, pace * 2))
                print(f"The exchange says that is too fast; now reading every {pace:g}s.")

        time.sleep(max(0.0, pace - (time.time() - tick)) + (min(30, 2 ** min(fails, 5) - 1) if fails else 0))


# ------------------------------------------------------------ chart page

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/data"):
            body, ctype = json.dumps(SNAP).encode(), "application/json"
        elif self.path in ("/", "/index.html"):
            body, ctype = PAGE.encode(), "text/html; charset=utf-8"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/retrain":
            self.send_error(404)
            return
        RETRAIN.set()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *a):
        pass


def start_web(args):
    try:
        server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    except OSError as e:
        print(f"Could not start the chart page on port {args.port} ({e}). Try --port 8080.\n")
        return
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://localhost:{args.port}"
    print(f"Live chart: {url}\n")
    if not args.no_browser:
        webbrowser.open(url)


def main():
    ap = argparse.ArgumentParser(description="Live, self-tuning up/down call on the current 15-minute BTC interval.")
    ap.add_argument("--source", choices=["coinbase", "binance", "binanceus"], default="coinbase")
    ap.add_argument("--every", type=float, default=2.0, help="seconds between readings (default 2)")
    ap.add_argument("--start-price", type=float, help="override the start price of the interval in progress")
    ap.add_argument("--log", action="store_true", help="print each reading on its own line")
    ap.add_argument("--history-days", type=int, default=30,
                    help="days of history to keep and tune on (default 30); raise it later to download older data")
    ap.add_argument("--retrain-hours", type=float, default=24, help="hours between self-tuning checks (default 24)")
    ap.add_argument("--db", help="database file (default btc_15m_<source>.db)")
    ap.add_argument("--market", choices=["polymarket", "kalshi"],
                    help="also record this market's bid/ask for Up in its 15-minute BTC market")
    ap.add_argument("--no-record", action="store_true",
                    help="skip the extra 10-second recording (order book, trade flow, other exchanges)")
    ap.add_argument("--port", type=int, default=8000, help="port for the chart page (default 8000)")
    ap.add_argument("--no-web", action="store_true", help="do not serve the chart page")
    ap.add_argument("--no-browser", action="store_true", help="serve the chart page but do not open it")
    args = ap.parse_args()

    global MAX_TRAIN_DAYS
    MAX_TRAIN_DAYS = max(MAX_TRAIN_DAYS, args.history_days)   # tune on everything that was asked for
    feed = make_feed(args.source)
    path = args.db or f"btc_15m_{args.source}.db"
    db = open_db(path)
    candles = {t: (o, c) for t, o, c in db.execute(
        "SELECT t, o, c FROM candles WHERE t >= ?", (time.time() - MAX_TRAIN_DAYS * 86400,))}
    publish(source=args.source, every=args.every)
    try:
        if not args.no_web:
            start_web(args)
        try:
            sync_history(feed, db, candles, args.history_days)
        except requests.RequestException as e:
            print(f"\nHistory download failed ({e}); continuing with what is stored.\n")
        backfill_outcomes(db, candles)

        model, last_check = load_model(db)
        if time.time() - last_check > args.retrain_hours * 3600:
            print("Running the self-tuning check (a minute or two with long history)...")
            try:
                res = train(candles, model)
            except Exception as e:
                res = print(f"Self-tuning check failed ({e}).")
            if res:
                model, last_check = record_training(db, res, model), res["at"]
            else:
                print("Not enough history yet to tune; using the original formula.\n")
        else:
            print(f"Using saved version: {describe(model)}.\n")
            publish(learn=learn_info(db, model))
        if not args.no_record:
            threading.Thread(target=recorder, args=(args, path), daemon=True).start()
        live(feed, args, db, candles, model, last_check)
    except KeyboardInterrupt:
        print("\nStopped.")


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 15m</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@400;500;600;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #090a0c; --card: #101216; --line: #1e2229; --grid: #16191e;
    --text: #eef0f3; --muted: #868d9a; --faint: #565c67;
    --up: #2ebd85; --down: #f6465d; --heat: 76,141,255;
    --mono: "JetBrains Mono", ui-monospace, Menlo, Consolas, monospace;
    --sans: "Archivo", "Helvetica Neue", Arial, sans-serif;
  }
  * { box-sizing: border-box; }
  [hidden] { display: none !important; }
  html, body { margin: 0; }
  body { background: var(--bg); color: var(--text); font-family: var(--sans); font-size: 14px; line-height: 1.45;
         padding: 0 clamp(12px, 2.5vw, 32px) 32px; }
  .page { max-width: 1320px; margin: 0 auto; display: flex; flex-direction: column; gap: 16px; }

  header { display: flex; align-items: center; justify-content: space-between; gap: 16px; flex-wrap: wrap;
           padding: 16px 0; border-bottom: 1px solid var(--line); }
  .brand { display: flex; align-items: center; gap: 12px; font-weight: 800; font-size: 15px; letter-spacing: .04em; text-transform: uppercase; }
  .brand .src { font-weight: 400; color: var(--muted); letter-spacing: .02em; text-transform: none; }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--up); box-shadow: 0 0 0 3px rgba(46,189,133,.18); }
  .stale .dot { background: var(--down); box-shadow: 0 0 0 3px rgba(246,70,93,.18); }
  .clock { display: flex; align-items: baseline; gap: 14px; font-family: var(--mono); }
  .clock .win { color: var(--muted); font-size: 13px; }
  .clock .left { font-size: 22px; font-weight: 700; }
  .clock .left small { font-family: var(--sans); font-size: 12px; font-weight: 400; color: var(--muted); margin-left: 6px; }
  .alert { display: none; background: rgba(246,70,93,.12); border: 1px solid rgba(246,70,93,.4); color: var(--down);
           padding: 8px 12px; border-radius: 6px; font-size: 13px; }
  .stale .alert { display: block; }

  .card { background: var(--card); border: 1px solid var(--line); border-radius: 8px; min-width: 0; }
  .card > h2 { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin: 0;
               padding: 12px 16px; border-bottom: 1px solid var(--line);
               font-size: 11px; font-weight: 600; letter-spacing: .1em; text-transform: uppercase; color: var(--muted); }
  .pad { padding: 16px; }
  .sub { color: var(--faint); font-size: 12px; margin: 16px 0 6px; }
  .sub:first-child { margin-top: 0; }
  .up { color: var(--up); } .down { color: var(--down); }

  /* 1. the answer: projection, confidence, conditions */
  .hero { display: grid; gap: 16px; grid-template-columns: minmax(250px, 1fr) minmax(280px, 1.25fr) minmax(280px, 1.1fr); }
  @media (max-width: 980px) { .hero { grid-template-columns: 1fr; } }
  .dir { font-size: 13px; font-weight: 600; letter-spacing: .1em; text-transform: uppercase; color: var(--muted); }
  .big { font-family: var(--mono); font-weight: 700; font-size: 68px; line-height: 1; letter-spacing: -.04em; margin: 6px 0 2px; }
  .big sup { font-size: 28px; font-weight: 500; vertical-align: top; position: relative; top: 7px; margin-left: 2px; letter-spacing: 0; }
  .bar { display: flex; height: 8px; margin: 12px 0 6px; background: var(--down); border-radius: 4px; overflow: hidden; }
  .bar i { display: block; background: var(--up); transition: width .4s ease; border-right: 2px solid var(--card); }
  .barlabels { display: flex; justify-content: space-between; font-family: var(--mono); font-size: 12px; }
  .rating { font-size: 30px; font-weight: 800; line-height: 1.1; margin: 4px 0 10px; }
  .meter { display: flex; gap: 4px; }
  .meter i { flex: 1; height: 8px; border-radius: 2px; background: #23272f; }
  .meter i.on { background: var(--text); }
  .note { margin: 12px 0 0; color: var(--muted); font-size: 13px; }
  .note b { color: var(--text); font-family: var(--mono); font-weight: 500; }
  .note .warn { color: var(--down); }

  .kv { margin: 0; }
  .kv div { display: flex; justify-content: space-between; align-items: baseline; gap: 16px; padding: 9px 16px; border-bottom: 1px solid var(--line); }
  .kv div:last-child { border-bottom: 0; }
  .kv dt { color: var(--muted); font-size: 13px; white-space: nowrap; }
  .kv dd { margin: 0; font-family: var(--mono); font-size: 13px; text-align: right; }
  .kv dd small { color: var(--faint); font-size: 12px; }

  /* 2. the picture */
  .stats { display: flex; flex-wrap: wrap; border-bottom: 1px solid var(--line); }
  .stats div { flex: 1 1 150px; padding: 10px 16px; border-right: 1px solid var(--line); }
  .stats div:last-child { border-right: 0; }
  .stats span { display: block; color: var(--muted); font-size: 12px; }
  .stats b { font-family: var(--mono); font-weight: 500; font-size: 15px; }
  .stats small { font-family: var(--mono); color: var(--faint); font-size: 12px; margin-left: 4px; }
  canvas { display: block; width: 100%; height: clamp(320px, 50vh, 560px); }
  .legend { display: flex; gap: 20px; flex-wrap: wrap; padding: 10px 16px 12px; border-top: 1px solid var(--line); color: var(--muted); font-size: 12px; }
  .legend span { display: inline-flex; align-items: center; gap: 8px; }
  .k { display: inline-block; width: 20px; height: 0; border-top: 2px solid var(--text); }
  .k.start { border-top: 1px dashed var(--muted); }
  .k.cone { height: 12px; border: 0; background: linear-gradient(var(--up), var(--up)) top/100% 50% no-repeat,
            linear-gradient(var(--down), var(--down)) bottom/100% 50% no-repeat; opacity: .45; border-radius: 2px; }
  .k.path { border-top: 1px solid var(--muted); opacity: .7; }

  /* 3. the evidence */
  .lower { display: grid; gap: 16px; grid-template-columns: minmax(0, 1.25fr) minmax(0, 1fr); align-items: start; }
  @media (max-width: 980px) { .lower { grid-template-columns: 1fr; } }
  .stack { display: flex; flex-direction: column; gap: 16px; min-width: 0; }
  table.heat { width: 100%; border-collapse: separate; border-spacing: 2px; font-family: var(--mono); font-size: 11px; }
  table.heat th { font-weight: 400; color: var(--faint); padding: 2px 4px; text-align: center; }
  table.heat th.m { text-align: right; padding-right: 8px; white-space: nowrap; }
  table.heat td { text-align: center; padding: 3px 0; border-radius: 3px; border: 1px solid transparent; }
  table.heat td.thin { color: var(--faint); }
  table.heat td.over { box-shadow: inset 0 -2px 0 var(--down); }
  table.heat td.cur { border-color: var(--text); }
  .key { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 18px; margin: 0 0 12px; color: var(--muted); font-size: 12px; }
  .key span { display: inline-flex; align-items: center; gap: 6px; }
  .key .ramp { width: 110px; height: 10px; border-radius: 2px; background: linear-gradient(90deg, rgba(var(--heat), .06), rgba(var(--heat), .56)); }
  .key .sw { width: 22px; height: 12px; border-radius: 3px; background: rgba(var(--heat), .3); border: 1px solid transparent; }
  .key .sw.cur { border-color: var(--text); } .key .sw.over { box-shadow: inset 0 -2px 0 var(--down); } .key .sw.thin { background: none; border-color: var(--line); }

  button { font: 600 12px var(--sans); letter-spacing: .04em; color: var(--text); background: #1a1e25; border: 1px solid #2c323c;
           border-radius: 6px; padding: 6px 12px; cursor: pointer; text-transform: none; }
  button:hover:not(:disabled) { background: #222730; border-color: #3a414d; }
  button:focus-visible { outline: 2px solid var(--text); outline-offset: 2px; }
  button:disabled { color: var(--muted); cursor: default; }
  .chips { display: flex; gap: 6px; flex-wrap: wrap; }
  .chip { font-family: var(--mono); font-size: 12px; border: 1px solid var(--line); border-radius: 4px; padding: 3px 8px; white-space: nowrap; color: var(--muted); }
  .chip.UP, .chip.on { color: var(--up); } .chip.DOWN, .chip.off { color: var(--down); }
  footer { color: var(--faint); font-size: 12px; }
</style>
</head>
<body>
<div class="page" id="root">
  <header>
    <div class="brand"><span class="dot"></span>BTC 15m <span class="src" id="status">connecting</span></div>
    <div class="clock"><span class="win" id="win"></span><span class="left"><span id="count">--:--</span><small>left</small></span></div>
  </header>
  <div class="alert" id="alert">No fresh data. Is the script still running?</div>

  <div class="hero">
    <section class="card" aria-live="polite">
      <h2>Projection</h2>
      <div class="pad">
        <div class="dir" id="call">Waiting</div>
        <div class="big" id="pct">--</div>
        <div class="bar" aria-hidden="true"><i id="barfill" style="width:50%"></i></div>
        <div class="barlabels"><span class="up" id="lblup">&#9650; Up</span><span class="down" id="lbldown">Down &#9660;</span></div>
      </div>
    </section>
    <section class="card">
      <h2>Confidence</h2>
      <div class="pad">
        <div class="rating" id="rating">&ndash;</div>
        <div class="meter" id="meter" aria-hidden="true"><i></i><i></i><i></i><i></i><i></i></div>
        <p class="note" id="track">Waiting for the first price reading.</p>
      </div>
    </section>
    <section class="card">
      <h2>Conditions</h2>
      <dl class="kv">
        <div><dt>Swing level</dt><dd id="s_swing">&ndash;</dd></div>
        <div><dt>Lead</dt><dd id="s_lead">&ndash;</dd></div>
        <div><dt>Typical swing left</dt><dd id="s_typ">&ndash;</dd></div>
        <div id="tiltrow" hidden><dt>Path adjustment</dt><dd id="s_tilt">&ndash;</dd></div>
        <div id="confrow" hidden><dt>Swing adjustment</dt><dd id="s_conf">&ndash;</dd></div>
        <div id="mktrow" hidden><dt id="mktlbl">Market, Up</dt><dd id="s_mkt">&ndash;</dd></div>
      </dl>
    </section>
  </div>

  <section class="card">
    <h2>This interval</h2>
    <div class="stats">
      <div><span>Start</span><b id="s_start">&ndash;</b></div>
      <div><span>Now</span><b id="s_now">&ndash;</b><small id="s_chg"></small></div>
      <div><span>Likely finish (9 in 10)</span><b id="s_range">&ndash;</b></div>
      <div><span>Typical 1-min move</span><b id="s_vol">&ndash;</b><small id="s_volp"></small></div>
    </div>
    <canvas id="chart" role="img" aria-label="Price so far in this 15-minute interval with the projected range to the end"></canvas>
    <div class="legend">
      <span><i class="k"></i>Price</span>
      <span><i class="k start"></i>Start</span>
      <span><i class="k cone"></i>Likely range (50% / 90%)</span>
      <span><i class="k path"></i>Example paths</span>
    </div>
  </section>

  <div class="lower">
    <section class="card">
      <h2>Track record</h2>
      <div class="pad">
        <div class="key">
          <span>right 50%<i class="ramp"></i>100%</span>
          <span><i class="sw cur"></i>now</span>
          <span><i class="sw over"></i>was overconfident</span>
          <span><i class="sw thin"></i>under 30 cases</span>
        </div>
        <div class="sub">How often readings were right, by minutes into the interval and the % it stated</div>
        <table class="heat" id="heat"><tbody><tr><td class="thin">Press Retrain now to measure this</td></tr></tbody></table>
        <div class="sub">The same, by how much the price was swinging</div>
        <table class="heat" id="heat2"><tbody><tr><td class="thin">&ndash;</td></tr></tbody></table>
        <div class="sub">After a reading first reached a level in an interval</div>
        <div class="chips" id="cross"><span class="chip">&ndash;</span></div>
      </div>
    </section>
    <div class="stack">
      <section class="card">
        <h2>Model <button id="retrain" type="button">Retrain now</button></h2>
        <dl class="kv" id="tune"><div><dt>Status</dt><dd>Not tuned yet</dd></div></dl>
      </section>
      <section class="card">
        <h2>History</h2>
        <div class="pad">
          <div class="sub">Recent intervals</div>
          <div class="chips" id="recent"></div>
          <div class="sub">Being saved for later tests</div>
          <div class="chips" id="sources"><span class="chip">starting</span></div>
        </div>
      </section>
    </div>
  </div>
  <footer>Confidence and the track record are measured on recent days the model was not tuned on. The cone shows where the price is likely to be until the interval ends; example paths show that amount of movement and are not predictions. Not financial advice.</footer>
</div>

<script>
const $ = id => document.getElementById(id);
const CSS = getComputedStyle(document.documentElement);
const C = n => CSS.getPropertyValue(n).trim();
const UP = C('--up'), DN = C('--down'), PAPER = C('--text'), MUTED = C('--muted'), GRID = C('--grid'), RULE = C('--line'), HEAT = C('--heat');
const MONO = '11px ' + C('--mono');
const NPATH = 10;
const BUCKETS = ['50–60%', '60–70%', '70–80%', '80–90%', '90%+'], MIDS = [0.55, 0.65, 0.75, 0.85, 0.95];
let D = null, lastOk = 0, yr = null, W = null, wKey = null, asked = 0, heatKey = null;

const money = (p, d = 2) => p.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
const clock = t => new Date(t * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
const mmss = s => Math.floor(s / 60) + ':' + String(Math.floor(s % 60)).padStart(2, '0');
const rgba = (hex, a) => { const n = parseInt(hex.slice(1), 16); return `rgba(${n >> 16},${n >> 8 & 255},${n & 255},${a})`; };
const ago = t => { const s = Date.now() / 1000 - t; return s < 90 ? 'just now' : s < 5400 ? Math.round(s / 60) + ' min ago' : s < 172800 ? Math.round(s / 3600) + ' h ago' : Math.round(s / 86400) + ' d ago'; };
const pts = v => (v * 100).toFixed(1) + ' pts';
const el = (tag, cls, text) => { const e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };

function phi(z) {                      // standard normal CDF
  const x = Math.abs(z) / Math.SQRT2, t = 1 / (1 + 0.3275911 * x);
  const erf = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-x * x);
  return 0.5 * (1 + (z < 0 ? -erf : erf));
}

// Example paths: fixed random walks per interval, so they glide instead of flickering.
function buildPaths(seed) {
  let a = seed >>> 0;
  const r = () => { a = a + 0x6D2B79F5 | 0; let t = Math.imul(a ^ a >>> 15, 1 | a); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; };
  const g = () => Math.sqrt(-2 * Math.log(r() || 1e-9)) * Math.cos(2 * Math.PI * r());
  W = [];
  for (let k = 0; k < NPATH; k++) { const w = [0]; for (let i = 1; i <= 180; i++) w.push(w[i - 1] + g() * Math.sqrt(5)); W.push(w); }
}
const wAt = (w, s) => { s = Math.max(0, Math.min(900, s)); const i = Math.min(179, Math.floor(s / 5)), f = s / 5 - i; return w[i] + (w[i + 1] - w[i]) * f; };

function state() {
  if (!D || !D.price || !D.sigma) return null;
  const now = Math.max(D.start, Math.min(Date.now() / 1000, D.end));
  const left = D.end - now, P = D.price, S = D.start_price;
  const kBase = (D.k || [1, 1, 1])[left > 600 ? 0 : left > 300 ? 1 : 2] || 1;
  const kk = D.k_now || kBase;                                  // learned confidence multiplier, including swing signals
  const sg = D.sigma / kk, spread = sg * Math.sqrt(left / 60);  // typical log move over the time left
  const mu = (D.tilt || 0) * spread;                            // learned lean from how the price got here
  const z = Math.log(P / S);
  const p = left > 0 ? phi((z + mu) / spread) : (P > S ? 1 : 0);
  const pNoTilt = left > 0 ? phi(z / spread) : p;
  const pNoSwing = left > 0 ? phi((z + mu * kk / kBase) / (spread * kk / kBase)) : p;
  return { now, left, P, S, sg, spread, mu, p, pNoTilt, pNoSwing };
}

function kv(box, rows) {
  box.textContent = '';
  for (const [k, v, note] of rows) { const d = el('div'); const dd = el('dd', null, v); if (note) dd.append(' ', el('small', null, note)); d.append(el('dt', null, k), dd); box.append(d); }
}

function cell(hit, n, mid, id) {
  const td = el('td', n < 30 ? 'thin' : null, n ? Math.round(100 * hit / n) + '%' : '·'); td.id = id; td.title = `${n} past cases`;
  if (n >= 30) { td.style.background = `rgba(${HEAT},${(0.06 + 0.5 * Math.max(0, hit / n - 0.5) * 2).toFixed(2)})`; if (mid - hit / n > 0.07) td.classList.add('over'); }
  return td;
}
function table(id, label, rows, prefix) {
  const body = el('tbody'), head = el('tr'); head.append(el('th', 'm', label));
  for (const b of BUCKETS) head.append(el('th', null, b)); body.append(head);
  for (const [name, key, data] of rows) { const row = el('tr'); row.append(el('th', 'm', name));
    (data || []).forEach(([hit, n], c) => row.append(cell(hit, n, MIDS[c], `${prefix}${key}_${c}`))); body.append(row); }
  $(id).replaceChildren(body);
}
const swingKey = r => r == null ? null : r < 0.7 ? 'quiet' : r <= 1.3 ? 'normal' : 'choppy';
function heat(Lr, m, bk) {
  if (!Lr || !Lr.grid) return;
  if (heatKey !== Lr.checked_at) {                              // rebuild only when a retrain changes it
    heatKey = Lr.checked_at;
    table('heat', 'min in', Array.from({ length: 14 }, (_, i) => [String(i + 1), i + 1, Lr.grid[i + 1]]), 'h');
    if (Lr.swing) table('heat2', 'swings', [['quiet', 'quiet', Lr.swing.quiet], ['normal', 'normal', Lr.swing.normal], ['choppy', 'choppy', Lr.swing.choppy]], 'w');
    const cx = $('cross'); cx.textContent = '';
    for (const [level, [hit, n]] of Object.entries(Lr.cross || {})) if (n) cx.append(el('span', 'chip', `${level}% → right ${Math.round(100 * hit / n)}%`));
  }
  const want = [document.getElementById(`h${m}_${bk}`), document.getElementById(`w${swingKey(D.swing)}_${bk}`)];
  document.querySelectorAll('table.heat td.cur').forEach(e => { if (!want.includes(e)) e.classList.remove('cur'); });
  want.forEach(e => e && e.classList.add('cur'));
}

function model() {
  const Lr = D && D.learn, btn = $('retrain'), busy = D && D.training;
  btn.disabled = !!busy || Date.now() - asked < 3000;
  btn.textContent = busy ? 'Retraining…' : 'Retrain now';
  if (!Lr) { kv($('tune'), [['Status', busy ? 'First tuning check running' : (D && D.train_note) || 'Not tuned yet']]); return; }
  const used = Object.entries(Lr.beta || {}).map(([k, v]) => `${k} ${v > 0 ? '+' : ''}${v.toFixed(2)}`);
  const rows = [
    ['Version', Lr.version ? 'v' + Lr.version : 'Original formula', Lr.version && Lr.tuned_at ? 'tuned ' + ago(Lr.tuned_at) : ''],
    ['Last check', busy ? 'running now' : ago(Lr.checked_at), busy ? '' : Lr.kept_old ? 'kept current version' : 'adopted new version'],
    ['Volatility', Lr.window + ' min' + (Lr.fast ? ` + ${Math.round(Lr.fast * 100)}% last 5 min` : '')],
    ['Confidence scaling', Lr.k.map(v => '×' + v.toFixed(2)).join(' / '), 'early / mid / late'],
    ['Signals in use', used.length ? used.join(' · ') : 'none', Lr.signals ? `of ${Object.keys(Lr.signals).length} tested` : ''],
    ['Error on unseen days', pts(Lr.ece), 'original ' + pts(Lr.base_ece)],
  ];
  if (Lr.live) {
    rows.push(['Live record', `${Lr.live.intervals} intervals · ${Math.round(Lr.live.acc * 100)}% right`, 'off by ' + pts(Lr.live.ece)]);
    if (Lr.live.since) rows.push(['Recording since', new Date(Lr.live.since * 1000).toLocaleDateString([], { month: 'short', day: 'numeric' }),
                                  Lr.live.gap_hours >= 0.05 ? Lr.live.gap_hours.toFixed(1) + ' h of gaps' : 'no gaps']);
  } else rows.push(['Live record', 'starts after the first full interval']);
  if (D.train_note) rows.push(['Note', D.train_note]);
  kv($('tune'), rows);
}

function update() {
  $('root').classList.toggle('stale', Date.now() - lastOk > 8000);
  model();
  if (D && D.recording) { const sc = $('sources'); sc.textContent = '';
    for (const [k, v] of Object.entries(D.recording)) sc.append(el('span', 'chip ' + (v ? 'on' : 'off'), k + (v ? ' ✓' : ' ✗'))); }
  const st = state();
  if (!st) { if (D && D.error) $('track').textContent = 'Waiting for data: ' + D.error; return; }
  const { left, P, S, sg, spread, mu, p, pNoTilt, pNoSwing } = st, up = p >= 0.5, conf = up ? p : 1 - p, Lr = D.learn;

  // header + projection
  $('status').textContent = D.source || '';
  $('win').textContent = `${clock(D.start)} – ${clock(D.end)}`;
  $('count').textContent = mmss(left);
  const call = $('call'); call.textContent = conf < 0.55 ? (up ? '▲ Leaning up' : '▼ Leaning down') : (up ? '▲ Up' : '▼ Down'); call.className = 'dir ' + (up ? 'up' : 'down');
  const pc = $('pct'); pc.textContent = Math.round(conf * 100); pc.append(el('sup', null, '%')); pc.className = 'big ' + (up ? 'up' : 'down');
  $('barfill').style.width = (p * 100).toFixed(1) + '%';
  $('lblup').textContent = '▲ Up ' + Math.round(p * 100) + '%';
  $('lbldown').textContent = 'Down ' + Math.round((1 - p) * 100) + '% ▼';

  // confidence: how readings like this one (same minute, same stated %, same swing level) actually turned out
  const tr = $('track'); tr.textContent = '';
  const m = Math.max(1, Math.min(14, Math.floor((900 - left) / 60))), bk = Math.min(Math.floor((conf - 0.5) * 10), 4), sk = swingKey(D.swing);
  let name = 'Not measured yet', bars = 0;
  if (Lr && Lr.grid && Lr.grid[m]) {
    const [hit, n] = Lr.grid[m][bk], steady = (h, c) => (h + 20 * MIDS[bk]) / (c + 20);   // leans on the stated % when cases are few
    const sw = Lr.swing && sk && Lr.swing[sk] ? Lr.swing[sk][bk] : null, swOk = sw && sw[1] >= 30;
    const r = Math.min(steady(hit, n), swOk ? steady(sw[0], sw[1]) : 1);
    if (n < 30) { name = 'Unproven'; bars = 1; tr.append(`Only ${n} past case${n === 1 ? '' : 's'} like this (${m} min in, saying ${BUCKETS[bk]}).`); }
    else {
      [name, bars] = r < 0.57 ? ['Coin flip', 1] : r < 0.67 ? ['Slight', 2] : r < 0.77 ? ['Moderate', 3] : r < 0.88 ? ['Strong', 4] : ['Very strong', 5];
      tr.append(`Readings like this (${m} min in, saying ${BUCKETS[bk]}) were right `, el('b', null, Math.round(100 * hit / n) + '%'), ` of the time in ${n.toLocaleString()} past cases`);
      if (swOk) tr.append(`; with ${sk} swings like now, `, el('b', null, Math.round(100 * sw[0] / sw[1]) + '%'));
      tr.append('.');
      if (MIDS[bk] - Math.min(hit / n, swOk ? sw[0] / sw[1] : 1) > 0.07) tr.append(' ', el('span', 'warn', 'It has been overconfident here.'));
    }
  } else tr.append('Press Retrain now to measure how readings like this have turned out.');
  $('rating').textContent = name;
  [...$('meter').children].forEach((e, i) => e.classList.toggle('on', i < bars));
  heat(Lr, m, bk);

  // conditions
  const gap = P - S, swing = P * spread;
  const sws = $('s_swing'); sws.textContent = D.swing == null ? '–' : (D.swing < 0.7 ? 'Quiet' : D.swing <= 1.3 ? 'Normal' : D.swing <= 2 ? 'Choppy' : 'Wild') + ' ';
  if (D.swing != null) sws.append(el('small', null, D.swing.toFixed(1) + '× usual'));
  $('s_lead').textContent = Math.abs(gap) < 0.005 ? 'on the start price' : `$${money(Math.abs(gap))} ${gap > 0 ? 'above' : 'below'} start`;
  const ty = $('s_typ'); ty.textContent = `±$${money(swing, 0)} `; ty.append(el('small', null, Math.abs(gap) > swing ? 'lead is bigger' : 'lead can flip'));
  const beta = (Lr && Lr.beta) || {}, hasTilt = ['momentum', 'steady', 'crossings', 'giveback', 'trend', 'streak'].some(k => k in beta),
        hasSwing = ['burst', 'chop', 'range', 'swing75'].some(k => k in beta);
  const adj = (d) => Math.abs(d) < 0.05 ? 'none right now' : `${Math.abs(d).toFixed(1)} pts toward ${d > 0 ? 'Up' : 'Down'}`;
  $('tiltrow').hidden = !hasTilt; if (hasTilt) $('s_tilt').textContent = adj((p - pNoTilt) * 100);
  $('confrow').hidden = !hasSwing;
  if (hasSwing) { const d = (conf - Math.max(pNoSwing, 1 - pNoSwing)) * 100; $('s_conf').textContent = Math.abs(d) < 0.05 ? 'none right now' : `${d > 0 ? '+' : '−'}${Math.abs(d).toFixed(1)} pts confidence`; }
  const mk = D.market, mkOn = mk && mk.start === D.start && (mk.bid != null || mk.ask != null);
  $('mktrow').hidden = !mkOn;
  if (mkOn) {
    $('mktlbl').textContent = mk.name[0].toUpperCase() + mk.name.slice(1) + ', Up';
    const c = v => v == null ? '–' : Math.round(v * 100) + '¢';
    $('s_mkt').textContent = `${c(mk.bid)} bid / ${c(mk.ask)} ask`;
  }

  // chart stats
  $('s_start').textContent = (D.exact ? '' : '~') + money(S);
  $('s_now').textContent = money(P); $('s_chg').textContent = `${gap >= 0 ? '+' : ''}${(100 * (P / S - 1)).toFixed(3)}%`;
  $('s_range').textContent = `${money(P * Math.exp(mu - 1.645 * spread), 0)} – ${money(P * Math.exp(mu + 1.645 * spread), 0)}`;
  $('s_vol').textContent = `±$${money(P * sg, 0)}`; $('s_volp').textContent = `${(sg * 100).toFixed(3)}%`;

  const rc = $('recent'); rc.textContent = '';
  for (const r of (D.results || [])) rc.append(el('span', 'chip ' + r.result, clock(r.start) + ' ' + (r.result === 'UP' ? '▲' : r.result === 'DOWN' ? '▼' : '–')));
  if (!rc.childNodes.length) rc.append(el('span', 'chip', 'none yet'));
  draw(st);
}

function draw(st) {
  const cv = $('chart'), dpr = window.devicePixelRatio || 1, w = cv.clientWidth, h = cv.clientHeight;
  if (cv.width !== Math.round(w * dpr) || cv.height !== Math.round(h * dpr)) { cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr); }
  const g = cv.getContext('2d'); g.setTransform(dpr, 0, 0, dpr, 0, 0); g.clearRect(0, 0, w, h);
  const L = 12, R = 128, T = 18, B = 30, pw = w - L - R, ph = h - T - B;
  const { now, left, P, S, sg, mu } = st, t0 = D.start;
  const band = (z, sec) => P * Math.exp((left > 0 ? mu * sec / left : 0) + z * sg * Math.sqrt(Math.max(0, sec) / 60));

  // Vertical range: everything seen so far, the start price, and the cone's far end.
  let lo = Math.min(S, P, band(-1.645, left)), hi = Math.max(S, P, band(1.645, left));
  for (const [, p] of D.ticks) { lo = Math.min(lo, p); hi = Math.max(hi, p); }
  const pad = (hi - lo) * 0.14 || P * 0.0004; lo -= pad; hi += pad;
  yr = yr && yr.k === t0 ? { k: t0, lo: yr.lo + (lo - yr.lo) * 0.25, hi: yr.hi + (hi - yr.hi) * 0.25 } : { k: t0, lo, hi };
  const X = t => L + (t - t0) / 900 * pw, Y = p => T + (yr.hi - p) / (yr.hi - yr.lo) * ph;
  const yS = Y(S), xN = X(now);

  // Grid
  g.font = MONO; g.lineWidth = 1; g.textBaseline = 'middle';
  const raw = (yr.hi - yr.lo) / 6, mag = Math.pow(10, Math.floor(Math.log10(raw))), step = [1, 2, 5, 10].map(m => m * mag).find(v => v >= raw);
  g.strokeStyle = GRID; g.fillStyle = MUTED; g.textAlign = 'left';
  for (let v = Math.ceil(yr.lo / step) * step; v < yr.hi; v += step) {
    const y = Math.round(Y(v)) + 0.5; g.beginPath(); g.moveTo(L, y); g.lineTo(L + pw, y); g.stroke();
    if (Math.abs(y - yS) > 12 && Math.abs(y - Y(P)) > 12) g.fillText(money(v, step < 1 ? 2 : 0), L + pw + 10, y);
  }
  g.textBaseline = 'top';
  for (let m = 0; m <= 15; m += 5) {
    const x = Math.round(X(t0 + m * 60)) + 0.5; g.strokeStyle = m % 15 ? GRID : RULE;
    g.beginPath(); g.moveTo(x, T); g.lineTo(x, T + ph); g.stroke();
    g.textAlign = m === 0 ? 'left' : m === 15 ? 'right' : 'center'; g.fillText(clock(t0 + m * 60), x, T + ph + 10);
  }

  // Cone: tinted by which side of the start price it is on.
  if (left > 0) {
    const N = 48, cone = z => { g.beginPath();
      for (let i = 0; i <= N; i++) { const s = left * i / N; g.lineTo(X(now + s), Y(band(z, s))); }
      for (let i = N; i >= 0; i--) { const s = left * i / N; g.lineTo(X(now + s), Y(band(-z, s))); }
      g.closePath(); g.fill(); };
    for (const [color, y0, y1] of [[UP, T, yS], [DN, yS, T + ph]]) {
      if (y1 <= y0) continue;
      g.save(); g.beginPath(); g.rect(xN, Math.max(T, y0), L + pw - xN, Math.min(T + ph, y1) - Math.max(T, y0)); g.clip();
      g.fillStyle = rgba(color, 0.12); cone(1.645); g.fillStyle = rgba(color, 0.16); cone(0.674); g.restore();
    }
    // Example paths
    if (wKey !== t0) { buildPaths(t0); wKey = t0; }
    const sNow = now - t0, sgS = sg / Math.sqrt(60);
    g.save(); g.beginPath(); g.rect(xN, T, L + pw - xN, ph); g.clip(); g.lineWidth = 1;
    for (const wk of W) {
      const base = wAt(wk, sNow), end = P * Math.exp(mu + sgS * (wk[180] - base));
      g.strokeStyle = rgba(end > S ? UP : DN, 0.3); g.beginPath(); g.moveTo(xN, Y(P));
      for (let s = Math.ceil(sNow / 5) * 5; s <= 900; s += 5) g.lineTo(X(t0 + s), Y(P * Math.exp(mu * (s - sNow) / left + sgS * (wAt(wk, s) - base))));
      g.stroke();
    }
    g.restore();
  }

  // Start price line
  g.setLineDash([5, 5]); g.strokeStyle = MUTED; g.lineWidth = 1;
  g.beginPath(); g.moveTo(L, Math.round(yS) + 0.5); g.lineTo(L + pw, Math.round(yS) + 0.5); g.stroke(); g.setLineDash([]);
  g.fillStyle = MUTED; g.textAlign = 'left'; g.textBaseline = 'middle';
  g.fillText('start ' + money(S), L + pw + 10, yS);

  // Price so far
  g.strokeStyle = PAPER; g.lineWidth = 2; g.lineJoin = 'round'; g.beginPath();
  let first = true;
  for (const [t, p] of D.ticks) { if (t < t0 || t > now) continue; first ? g.moveTo(X(t), Y(p)) : g.lineTo(X(t), Y(p)); first = false; }
  first ? g.moveTo(xN, Y(P)) : g.lineTo(xN, Y(P)); g.stroke();

  // Now marker and price tag
  g.strokeStyle = rgba(PAPER, 0.22); g.lineWidth = 1; g.beginPath(); g.moveTo(Math.round(xN) + 0.5, T); g.lineTo(Math.round(xN) + 0.5, T + ph); g.stroke();
  const col = P >= S ? UP : DN;
  g.fillStyle = rgba(col, 0.25); g.beginPath(); g.arc(xN, Y(P), 8, 0, 7); g.fill();
  g.fillStyle = col; g.beginPath(); g.arc(xN, Y(P), 3.5, 0, 7); g.fill();
  const yP = Math.abs(Y(P) - yS) < 13 ? yS + (Y(P) >= yS ? 13 : -13) : Y(P);
  g.fillStyle = col; g.beginPath(); g.roundRect ? g.roundRect(L + pw + 5, yP - 9, R - 13, 18, 3) : g.rect(L + pw + 5, yP - 9, R - 13, 18); g.fill();
  g.fillStyle = C('--bg'); g.font = '700 ' + MONO; g.fillText(money(P), L + pw + 10, yP + 0.5);
}

$('retrain').addEventListener('click', async () => {
  asked = Date.now(); $('retrain').disabled = true; $('retrain').textContent = 'Retraining…';
  try { await fetch('/retrain', { method: 'POST' }); } catch (e) { /* the stale banner covers a stopped script */ }
});

async function poll() {
  try { const r = await fetch('/data', { cache: 'no-store' }); D = await r.json(); if (D.now && Date.now() / 1000 - D.now < 8) lastOk = Date.now(); }
  catch (e) { /* script stopped; the stale banner will show */ }
  update();
  setTimeout(poll, D && D.every ? Math.max(250, Math.min(2000, D.every * 1000)) : 1000);
}
setInterval(update, 250);
addEventListener('resize', update);
poll();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
