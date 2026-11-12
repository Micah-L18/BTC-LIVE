#!/usr/bin/env python3
"""
btc_lab.py - side testing for new signals, without touching the live predictor.

Runs next to btc_15.py with its own database (btc_lab.db) and its own page at
http://localhost:8001/testing (the live page's /testing redirects there).
It only ever READS the live database and the live page's /data feed.

What it does
  1. Backtest. Keeps its own copy of 1-minute candles WITH high, low and volume
     (the live database only has open/close), then tests each experiment as an
     add-on to the live model: P = Phi(live score + sum of weight * signal).
     Weights are fitted on older history and scored on the most recent 20% it
     never saw, exactly like the live self-tuning check. Re-runs every few hours
     and on the page's button.
  2. Live side-by-side. Every couple of seconds it takes the live model's
     probability from http://localhost:8000/data, applies each experiment on top,
     records all of them every 10 s and scores them when the interval ends.
     This is the real test: history can flatter an idea, live data cannot.
  3. Market odds. Records Polymarket and Kalshi bid/ask for "Up" every 10 s, so
     the market itself shows up in the side-by-side and can be blended in later.

Experiments (each a signal or two; see features())
  tod      expected swings for the time of day ahead vs the hour just gone
  volume   last 5 minutes' volume vs the last hour's
  flow     volume-weighted direction of this interval's 1-minute candles
  trend    price vs 1 hour and 4 hours ago
  range    where the price sits in the last hour's high-low range
  round    a round $500 level between the start price and now
  levels   the previous interval's high or low between the start price and now
  combo    every signal above that shows up clearly in the training data

Nothing here changes live calls. To promote a winner, it has to be added to
btc_15.py's self-tuning, where it must pass the same test again.

Usage
  python btc_lab.py                      # page on :8001, reads live on :8000
  python btc_lab.py --retest-hours 4 --days 30
"""
import argparse
import json
import math
import os
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from statistics import NormalDist

import requests

import btc_15 as live

HERE = os.path.dirname(os.path.abspath(__file__))
BAR = live.BAR
LIGHT_PORT = 8002                     # where btc_light.py serves /testing-2 (passed through here)
LEVEL = 500.0                         # spacing of "round number" levels, in dollars
SCALE_X = {"tod", "volburst"}         # signals that raise/lower confidence (multiplied by the live score)
EXPERIMENTS = [
    ("tod", "Time of day", "Expected swings for the time of day ahead vs the hour just gone (e.g. heading into the US open).", ["tod"]),
    ("volume", "Volume burst", "Last 5 minutes' volume compared with the last hour's.", ["volburst"]),
    ("flow", "Candle buying pressure", "Volume-weighted direction of this interval's 1-minute candles.", ["flow"]),
    ("trend", "Higher-timeframe trend", "Price now vs 1 hour and 4 hours ago, in typical-move units.", ["trend1h", "trend4h"]),
    ("range", "Range position", "Where the price sits in the last hour's high-low range.", ["rangepos"]),
    ("round", "Round-number levels", f"A ${LEVEL:,.0f} level sitting between the start price and now (support / resistance).", ["round"]),
    ("levels", "Previous interval high/low", "The previous interval's high or low sitting between the start price and now.", ["prevlevels"]),
]
ALL_FEATURES = [f for e in EXPERIMENTS for f in e[3]]
ND = NormalDist()


def clip(x, lim=4.0):
    return max(-lim, min(lim, x))


# ----------------------------------------------------------------- data

def open_lab(path):
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS candles (t INTEGER PRIMARY KEY, o REAL, h REAL, l REAL, c REAL, v REAL);
        CREATE TABLE IF NOT EXISTS backtests (id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL, report TEXT);
        CREATE TABLE IF NOT EXISTS shadow (t REAL, start INTEGER, left REAL, name TEXT, p REAL, up INTEGER);
        CREATE INDEX IF NOT EXISTS shadow_start ON shadow (start);
        CREATE TABLE IF NOT EXISTS market (t REAL, start INTEGER, name TEXT, bid REAL, ask REAL, error TEXT);
    """)
    db.commit()
    return db


def lab_minutes(feed, start, end):
    """Coinbase 1-minute candles as {open_time: (open, high, low, close, volume)} for [start, end)."""
    out, t = {}, start
    while t < end:
        e = min(t + 299 * 60, end)
        rows = feed.get(feed.base + "/candles", granularity=60,
                        start=live.datetime.fromtimestamp(t, live.timezone.utc).isoformat(),
                        end=live.datetime.fromtimestamp(e, live.timezone.utc).isoformat())
        for r in rows:                     # [time, low, high, open, close, volume]
            out[int(r[0])] = (float(r[3]), float(r[2]), float(r[1]), float(r[4]), float(r[5]))
        t = e
        if t < end:
            time.sleep(0.15)
    return out


def store(db, rows, now):
    db.executemany("INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?,?)",
                   [(t, *x) for t, x in rows.items() if t + 60 <= now])
    db.commit()


def sync_history(feed, db, days):
    now = int(time.time()) // 60 * 60
    want = now - days * 86400
    lo, hi = db.execute("SELECT MIN(t), MAX(t) FROM candles").fetchone()
    spans = [(want, now)] if hi is None or hi < want else [(hi - 120, now)]
    if lo is not None and hi >= want and lo > want + 3600:
        spans.insert(0, (want, lo))
    total, done = sum(b - a for a, b in spans), 0
    if total > 6 * 3600:
        print(f"Downloading {total / 86400:.1f} days of 1-minute candles with high/low/volume...", flush=True)
    for a, end in spans:
        while a < end:
            b = min(a + 43200, end)
            store(db, lab_minutes(feed, a, b), now)
            done, a = done + b - a, b
    print("Candle history up to date.", flush=True)


class Series:
    """1-minute candles laid out as arrays, with running sums for fast windows."""

    def __init__(self, candles):
        self.lo = min(candles)
        n = self.n = (max(candles) - self.lo) // 60 + 1
        self.o, self.h, self.l, self.c, self.v = ([0.0] * n for _ in range(5))
        self.have, last = [False] * n, None
        for k in range(n):
            x = candles.get(self.lo + 60 * k)
            if x:
                self.o[k], self.h[k], self.l[k], self.c[k], self.v[k] = x
                self.have[k], last = True, x[3]
            else:
                self.o[k] = self.h[k] = self.l[k] = self.c[k] = last
        self.pre = [0.0, 0.0]              # pre[j+1] = sum of squared log moves up to minute j
        for k in range(1, n):
            self.pre.append(self.pre[-1] + math.log(self.c[k] / self.c[k - 1]) ** 2)
        self.cv = [0.0]                    # cv[k] = volume of minutes 0..k-1
        for x in self.v:
            self.cv.append(self.cv[-1] + x)

    def idx(self, t):
        return (int(t) - self.lo) // 60

    def sig(self, j, w):
        return math.sqrt(max(self.pre[j + 1] - self.pre[j + 1 - w], 0.0) / w)


def bucket(t):
    """Half-hour of the day, split weekday / weekend (96 buckets)."""
    t = int(t)
    return (48 if ((t // 86400) + 3) % 7 >= 5 else 0) + (t % 86400) // 1800


def tod_table(S, cut):
    """Typical 1-minute move for each half-hour bucket, from data before `cut` only."""
    acc, cnt = [0.0] * 96, [0] * 96
    for k in range(1, S.n):
        t = S.lo + 60 * k
        if t >= cut:
            break
        if S.have[k] and S.have[k - 1]:
            b = bucket(t)
            acc[b] += math.log(S.c[k] / S.c[k - 1]) ** 2
            cnt[b] += 1
    allv = math.sqrt(sum(acc) / max(sum(cnt), 1))
    return [math.sqrt(a / c) if c >= 30 and a > 0 else allv for a, c in zip(acc, cnt)]


def features(S, i, j, price, start, left, tod):
    """Signal values for an interval starting at minute i, last finished minute j,
    current price, start price and minutes left. Direction signals point toward UP."""
    if j < 241 or j >= S.n or i > S.n or i < 15:
        return None
    sig = S.sig(j, 60)
    if sig <= 0:
        return None
    side = (price > start) - (price < start)
    now = S.lo + 60 * j + 60
    f = {}
    if tod:
        behind = math.sqrt((tod[bucket(now - 2700)] ** 2 + tod[bucket(now - 900)] ** 2) / 2)
        f["tod"] = clip(math.log(tod[bucket(now + left * 30)] / behind), 1.5)
    else:
        f["tod"] = 0.0
    v5, v60 = S.cv[j + 1] - S.cv[j - 4], S.cv[j + 1] - S.cv[j - 59]
    f["volburst"] = clip(math.log(v5 * 12 / v60), 1.5) if v5 > 0 and v60 > 0 else 0.0
    num = den = 0.0
    for k in range(i, j + 1):
        d = S.c[k] - S.o[k]
        num += ((d > 0) - (d < 0)) * S.v[k]
        den += S.v[k]
    f["flow"] = num / den if den > 0 else 0.0
    f["trend1h"] = clip(math.log(price / S.c[j - 60]) / (sig * math.sqrt(60)))
    f["trend4h"] = clip(math.log(price / S.c[j - 240]) / (sig * math.sqrt(240)))
    H, L = max(S.h[j - 59:j + 1]), min(S.l[j - 59:j + 1])
    f["rangepos"] = max(-0.5, min(0.5, (price - L) / (H - L) - 0.5)) if H > L else 0.0
    unit = price * sig * math.sqrt(max(left, 1.0))          # a typical dollar move over the time left
    f["round"] = f["prevlevels"] = 0.0
    if side:
        a, b = min(start, price), max(start, price)
        R = math.floor(price / LEVEL) * LEVEL if side > 0 else math.ceil(price / LEVEL) * LEVEL
        if a < R < b:
            f["round"] = side * math.exp(-abs(price - R) / unit)
        for lev in (max(S.h[i - 15:i]), min(S.l[i - 15:i])):
            if a < lev < b:
                f["prevlevels"] += side * math.exp(-abs(price - lev) / unit)
    return f


def xval(f, name, off):
    return f[name] * (clip(off, 6) if name in SCALE_X else 1.0)


def score_of(off, f, beta):
    return off + sum(b * xval(f, n, off) for n, b in beta.items())


# ------------------------------------------------------------- backtest

def champion(live_path):
    try:
        con = sqlite3.connect(f"file:{live_path}?mode=ro", uri=True, timeout=10)
        model, _ = live.load_model(con)
        con.close()
        return model
    except Exception:
        return dict(live.BASE)


def fit(rows, names):
    """Weights for `names` on top of the live score (probit, Newton's method, light ridge).
    Returns ({name: weight}, {name: t-stat})."""
    n = len(names)
    data = [(q, off, [xval(f, x, off) for x in names]) for q, off, f, *_ in rows]
    beta, H = [0.0] * n, None
    for _ in range(15):
        g = [-b for b in beta]
        H = [[float(i == j) for j in range(n)] for i in range(n)]
        for q, off, x in data:
            sc = clip(off + sum(b * v for b, v in zip(beta, x)), 8)
            lam = q * math.exp(-sc * sc / 2) / 2.5066282746310002 / max(live.phi(q * sc), 1e-12)
            w = lam * (lam + sc)
            for i in range(n):
                g[i] += lam * x[i]
                for j in range(i, n):
                    H[i][j] += w * x[i] * x[j]
        for i in range(n):
            for j in range(i):
                H[i][j] = H[j][i]
        step = live.solve(H, g)
        beta = [b + d for b, d in zip(beta, step)]
        if max(abs(d) for d in step) < 1e-5:
            break
    ts = {}
    for i, name in enumerate(names):
        var = live.solve(H, [float(i == j) for j in range(n)])[i]
        ts[name] = round(beta[i] / math.sqrt(var), 2) if var > 0 else 0.0
    return {name: round(b, 5) for name, b in zip(names, beta)}, ts


def evaluate(rows, beta):
    pairs, early = [], [0, 0]
    for q, off, f, m, *_ in rows:
        p, up = live.phi(score_of(off, f, beta)), q > 0
        pairs.append((p, up))
        if m <= 5:
            early[0] += (p > 0.5) == up
            early[1] += 1
    out = live.score(pairs)
    out.pop("buckets", None)
    out["early"] = early[0] / early[1] if early[1] else None
    return out


def verdict(e, base):
    e["d_ll"] = base["ll"] - e["hold"]["ll"]
    clear = max(abs(t) for t in e["t"].values()) >= 2
    e["verdict"] = "helps" if e["d_ll"] >= live.MARGIN and clear else "slight" if e["d_ll"] > 0 else "no help"


def track(rows, beta):
    """The live page's track record (by minute and stated %, by swing level, after first
    reaching 60-90%), measured on the unseen test days for this set of weights."""
    grid = {m: [[0, 0] for _ in range(5)] for m in range(1, 15)}
    swing = {n: [[0, 0] for _ in range(5)] for n in ("quiet", "normal", "choppy")}
    cross, interval, seen = {lv: [0, 0] for lv in (60, 70, 80, 90)}, None, set()
    for q, off, f, m, start, ratio in rows:
        if start != interval:
            interval, seen = start, set()
        p = live.phi(score_of(off, f, beta))
        conf, hit = max(p, 1 - p), (p > 0.5) == (q > 0)
        bk = min(int((conf - 0.5) * 10), 4)
        for cell in (grid[m][bk], swing["quiet" if ratio < 0.7 else "normal" if ratio <= 1.3 else "choppy"][bk]):
            cell[0] += hit
            cell[1] += 1
        for lv in cross:
            if lv not in seen and conf * 100 >= lv:
                seen.add(lv)
                cross[lv][0] += hit
                cross[lv][1] += 1
    return {"grid": grid, "swing": swing, "cross": cross}


def backtest(args):
    db = sqlite3.connect(args.db, timeout=30)
    candles = {r[0]: r[1:] for r in db.execute("SELECT t, o, h, l, c, v FROM candles WHERE t >= ?",
                                               (time.time() - args.days * 86400,))}
    db.close()
    if len(candles) < 3 * 1440:
        raise RuntimeError("not enough candle history yet")
    S = Series(candles)
    champ = champion(args.live_db)
    samples = live.build_samples({t: (x[0], x[3]) for t, x in candles.items()})
    cut = samples[int(len(samples) * (1 - live.HOLDOUT))][0]
    tod = tod_table(S, cut)
    tr, ho, wi = [], [], live.WINDOWS.index(champ["window"])
    for s in samples:
        i = S.idx(s[0])
        j = i + s[1] - 1
        f = features(S, i, j, S.c[j], S.o[i], 15 - s[1], tod)
        if f is not None:
            (tr if s[0] < cut else ho).append((1 if s[3] else -1, live.sample_score(champ, s), f, s[1],
                                               s[0], s[5] / s[4][wi]))

    base = evaluate(ho, {})
    exps, strong = [], []
    for key, name, desc, names in EXPERIMENTS:
        beta, ts = fit(tr, names)
        strong += [n for n in names if abs(ts[n]) >= live.KEEP_T]
        exps.append({"key": key, "name": name, "desc": desc, "beta": beta, "t": ts, "hold": evaluate(ho, beta)})
    if len(strong) >= 2:
        beta, ts = fit(tr, strong)
        exps.append({"key": "combo", "name": "Combination", "beta": beta, "t": ts, "hold": evaluate(ho, beta),
                     "desc": "Every signal above that showed up clearly in training (t >= %g): %s."
                             % (live.KEEP_T, ", ".join(strong))})
    for e in exps:
        verdict(e, base)
    passed = [n for e in exps if e["verdict"] == "helps" and e["key"] != "combo" for n in e["beta"]]
    test = None
    if passed:
        beta, ts = fit(tr, passed)
        test = {"key": "test", "name": "Test model", "beta": beta, "t": ts, "hold": evaluate(ho, beta),
                "desc": "The projection on /testing: the live model plus every signal that passed (%s). Picked using "
                        "these same test days, so this row flatters it; the live side-by-side is the real check."
                        % ", ".join(passed)}
        verdict(test, base)
        test["track"] = track(ho, beta)
        exps.append(test)
    return {"at": time.time(), "champion": live.describe(champ), "base": base, "tod": tod,
            "base_track": track(ho, {}), "test": test,
            "n_train": len(tr), "n_hold": len(ho), "train_days": (cut - samples[0][0]) / 86400,
            "hold_days": (samples[-1][0] + BAR - cut) / 86400, "experiments": exps}


# --------------------------------------------------------- shared state

STATE = {"series": None, "report": None, "now": None, "board": [], "status": {}, "busy": False, "error": None, "test_error": None}
LOCK = threading.Lock()
RERUN = threading.Event()
MARKETS = {}                           # name -> (bid, ask, start, t)


def publish(**kw):
    global STATE
    with LOCK:
        STATE = {**STATE, **kw}


def tester(args):
    db = open_lab(args.db)
    row = db.execute("SELECT report FROM backtests ORDER BY id DESC LIMIT 1").fetchone()
    if row:
        publish(report=json.loads(row[0]))
    if row and "base_track" in json.loads(row[0]) and time.time() - json.loads(row[0])["at"] < args.retest_hours * 3600:
        RERUN.wait(args.retest_hours * 3600 - (time.time() - json.loads(row[0])["at"]))
        RERUN.clear()
    while True:
        publish(busy=True)
        try:
            print("Running backtest...", flush=True)
            rep = backtest(args)
            db.execute("INSERT INTO backtests (at, report) VALUES (?, ?)", (rep["at"], json.dumps(rep)))
            db.commit()
            publish(report=rep, test_error=None)
            print("Backtest done: " + ", ".join(f"{e['key']} {e['d_ll']:+.4f}" for e in rep["experiments"]), flush=True)
        except Exception as e:
            publish(test_error=str(e)[:300])
            print(f"Backtest failed: {e}", flush=True)
        publish(busy=False)
        RERUN.wait(args.retest_hours * 3600)
        RERUN.clear()


def market_recorder(args):
    db, s, caches = open_lab(args.db), requests.Session(), {"polymarket": {}, "kalshi": {}}
    s.headers["User-Agent"] = "btc-15m-lab/1.0"
    while True:
        t = time.time()
        start = int(t) // BAR * BAR
        for name, cache in caches.items():
            bid = ask = err = None
            try:
                bid, ask = live.market_odds(s, name, start, cache)
                MARKETS[name] = (bid, ask, start, t)
            except Exception as e:
                err = str(e)[:200]
                cache.clear()
            db.execute("INSERT INTO market VALUES (?,?,?,?,?,?)", (t, start, name, bid, ask, err))
        db.commit()
        time.sleep(max(1.0, 10 - (time.time() - t)))


def score_finished(db):
    now = time.time()
    for (start,) in db.execute("SELECT DISTINCT start FROM shadow WHERE up IS NULL").fetchall():
        if start + BAR + 90 > now:
            continue
        got = None
        try:                               # score it the way the live predictor did (the contract's official result)
            con = sqlite3.connect(f"file:{ARGS.live_db}?mode=ro", uri=True, timeout=5)
            got = con.execute("SELECT up FROM readings WHERE start=? AND up IS NOT NULL LIMIT 1", (start,)).fetchone()
            con.close()
        except sqlite3.Error:
            pass
        if got:
            db.execute("UPDATE shadow SET up=? WHERE start=?", (got[0], start))
            continue
        if start + BAR + 600 > now:
            continue
        a = db.execute("SELECT o FROM candles WHERE t=?", (start,)).fetchone()
        b = db.execute("SELECT c FROM candles WHERE t=?", (start + 840,)).fetchone()
        if a and b:
            db.execute("UPDATE shadow SET up=? WHERE start=?", (int(b[0] > a[0]), start))
    db.commit()


def scoreboard(db):
    by_t, ups, starts = {}, {}, {}
    for t, start, name, p, up in db.execute("SELECT t, start, name, p, up FROM shadow WHERE up IS NOT NULL AND t > ?",
                                            (time.time() - 30 * 86400,)):
        by_t.setdefault(t, {})[name] = p
        ups[t], starts[t] = up, start
    names = sorted({n for ps in by_t.values() for n in ps})
    out = []
    for name in names:
        ts = [t for t, ps in by_t.items() if name in ps and "live" in ps]
        if not ts:
            continue
        a = live.score([(by_t[t][name], ups[t]) for t in ts])
        b = live.score([(by_t[t]["live"], ups[t]) for t in ts])
        early = [t for t in ts if by_t[t] and t - starts[t] <= 300]
        ea = live.score([(by_t[t][name], ups[t]) for t in early]) if early else None
        eb = live.score([(by_t[t]["live"], ups[t]) for t in early]) if early else None
        out.append({"name": name, "n": len(ts), "intervals": len({starts[t] for t in ts}),
                    "ll": a["ll"], "acc": a["acc"], "ece": a["ece"], "d_ll": b["ll"] - a["ll"],
                    "early": ea["acc"] if ea else None, "base_ll": b["ll"], "base_acc": b["acc"],
                    "base_ece": b["ece"], "base_early": eb["acc"] if eb else None})
    return out


def status(db, args):
    st = {}
    n, lo = db.execute("SELECT COUNT(*), MIN(t) FROM candles").fetchone()
    st["candles"] = {"n": n, "days": (time.time() - lo) / 86400 if lo else 0}
    try:
        con = sqlite3.connect(f"file:{args.live_db}?mode=ro", uri=True, timeout=5)
        n, lo = con.execute("SELECT COUNT(*), MIN(t) FROM flow").fetchone()
        con.close()
        st["flow"] = {"n": n, "days": (time.time() - lo) / 86400 if lo else 0}
    except Exception as e:
        st["flow"] = {"error": str(e)[:200]}
    st["market"] = {}
    for name, n, ok, lo in db.execute("SELECT name, COUNT(*), SUM(bid IS NOT NULL), MIN(t) FROM market GROUP BY name"):
        err = db.execute("SELECT error FROM market WHERE name=? ORDER BY t DESC LIMIT 1", (name,)).fetchone()[0]
        st["market"][name] = {"n": n, "ok": ok or 0, "days": (time.time() - lo) / 86400, "last_error": err}
    st["intervals"] = db.execute("SELECT COUNT(DISTINCT start) FROM shadow WHERE up IS NOT NULL").fetchone()[0]
    return st


def shadow(args):
    db, feed = open_lab(args.db), live.make_feed("coinbase")
    s = requests.Session()
    recent = {r[0]: r[1:] for r in db.execute("SELECT t, o, h, l, c, v FROM candles WHERE t >= ?",
                                              (time.time() - 420 * 60,))}
    S, cand_at, rec_at = None, 0.0, 0.0
    while True:
        tick = time.time()
        try:
            if tick - cand_at > 60:
                since = max(recent) - 180 if recent else int(tick) - 420 * 60
                rows = {t: x for t, x in lab_minutes(feed, since // 60 * 60, int(tick) + 60).items() if t + 60 <= tick}
                recent.update(rows)
                store(db, rows, tick)
                for t in [t for t in recent if t < tick - 420 * 60]:
                    del recent[t]
                S, cand_at = Series(recent), tick
                publish(series=S)
                score_finished(db)
                publish(board=scoreboard(db), status=status(db, args))

            d = s.get(args.live_url.rstrip("/") + "/data", timeout=3).json()
            p = d.get("p_up")
            if p is None or not d.get("exact") or not d.get("start"):
                publish(error="waiting for the live predictor at " + args.live_url)
            else:
                start, left = d["start"], (d["end"] - d["now"]) / 60
                rep = STATE["report"]
                probs, f = {"live": p}, None
                if left > 0 and rep and S:
                    off = ND.inv_cdf(min(max(p, 1e-6), 1 - 1e-6))
                    f = features(S, S.idx(start), S.n - 1, d["price"], d["start_price"], left, rep["tod"])
                    if f:
                        for e in rep["experiments"]:
                            probs[e["key"]] = live.phi(score_of(off, f, e["beta"]))
                for name, (bid, ask, mstart, mt) in list(MARKETS.items()):
                    if mstart == start and tick - mt < 30 and bid is not None and ask is not None:
                        probs["mkt_" + name] = (bid + ask) / 2
                if left > 0 and tick - rec_at >= 10:
                    db.executemany("INSERT INTO shadow (t, start, left, name, p) VALUES (?,?,?,?,?)",
                                   [(d["now"], start, left * 60, n, v) for n, v in probs.items()])
                    db.commit()
                    rec_at = tick
                publish(error=None, now={"t": d["now"], "start": start, "end": d["end"], "price": d["price"],
                                         "start_price": d["start_price"], "probs": probs, "features": f})
        except Exception as e:
            publish(error=str(e)[:300])
        time.sleep(max(0.2, args.every - (time.time() - tick)))


# ---------------------------------------------------------------- page

def test_snapshot():
    """The live page's own data, re-projected with the test model, plus what the comparison
    with the live model needs. The chart works out P(up) from sigma, k_now and tilt, so the
    test model only has to adjust those two: its confidence signals scale k_now and its
    direction signals add to the tilt."""
    try:
        d = requests.get(ARGS.live_url.rstrip("/") + "/data", timeout=3).json()
    except Exception as e:
        return {"error": f"live predictor not reachable: {e}"[:200]}
    st = STATE
    rep, S = st["report"], st["series"]
    test = rep.get("test") if rep else None
    k_live, tilt_live = d.get("k_now"), d.get("tilt") or 0.0
    d.update(live_k_now=k_live, live_tilt=tilt_live, live_p=d.get("p_up"))
    info = {"names": list(test["beta"]) if test else [], "beta": test["beta"] if test else {}, "values": {},
            "active": False}
    if test and S and k_live and d.get("price") and d.get("start_price") and d.get("end"):
        left = (d["end"] - d["now"]) / 60
        f = features(S, S.idx(d["start"]), S.n - 1, d["price"], d["start_price"], left, rep["tod"]) if left > 0 else None
        if f:
            scale = max(0.2, 1 + sum(b * f[n] for n, b in test["beta"].items() if n in SCALE_X))
            lean = sum(b * f[n] for n, b in test["beta"].items() if n not in SCALE_X)
            d.update(k_now=k_live * scale, tilt=tilt_live * scale + lean)
            info.update(active=True, values={n: f[n] for n in test["beta"]}, scale=scale, lean=lean)
    board = {b["name"]: b for b in st["board"]}
    if rep:
        info.update(backtest={"base": rep["base"], "test": test["hold"] if test else None,
                              "d_ll": test["d_ll"] if test else None, "hold_days": rep["hold_days"]},
                    live=board.get("test"))
        d["learn"] = {**((test or {}).get("track") or rep.get("base_track") or {}), "checked_at": rep["at"],
                      "ece": (test or {"hold": rep["base"]})["hold"]["ece"], "base_ece": rep["base"]["ece"]}
    else:
        d["learn"] = None
    d.update(test=info, training=st["busy"],
             train_note=st["test_error"] or (None if test or not rep else
                                             "No experiment passed the backtest yet, so the test model equals the live model."))
    return d



class Handler(BaseHTTPRequestHandler):
    def send(self, code, body, ctype, extra=()):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in extra:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def light(self, method):
        """Pass /testing-2 through to the traffic-light script (btc_light.py) on its own port, so it
        is reachable on the same address as this page and the live page."""
        try:
            r = requests.request(method, f"http://127.0.0.1:{LIGHT_PORT}{self.path}", timeout=10, allow_redirects=False)
            self.send(r.status_code, r.content, r.headers.get("Content-Type", "text/plain"))
        except requests.RequestException:
            self.send(502, b"The traffic-light script (btc_light.py) is not running.", "text/plain; charset=utf-8")

    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        if path.startswith("/testing-2"):
            self.light("GET")
        elif path in ("", "/index.html"):
            self.send(302, b"", "text/plain", [("Location", "/testing")])
        elif path == "/testing":
            self.send(200, TEST_PAGE.encode(), "text/html; charset=utf-8")
        elif path == "/testing/results":
            self.send(200, PAGE.encode(), "text/html; charset=utf-8")
        elif path == "/testing/live":
            self.send(200, json.dumps(test_snapshot()).encode(), "application/json")
        elif path == "/testing/data":
            with LOCK:
                body = json.dumps({**{k: v for k, v in STATE.items() if k != "series"}, "live_port": ARGS.live_url.rsplit(":", 1)[-1].strip("/"), "lab_port": ARGS.port}).encode()
            self.send(200, body, "application/json")
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path.startswith("/testing-2"):
            self.light("POST")
            return
        if self.path != "/testing/rerun":
            self.send_error(404)
            return
        RERUN.set()
        self.send(200, b"{}", "application/json")

    def log_message(self, *a):
        pass


ARGS = None


def main():
    global ARGS
    ap = argparse.ArgumentParser(description="Side testing for new signals next to the live BTC 15-minute predictor.")
    ap.add_argument("--db", default=os.path.join(HERE, "btc_lab.db"), help="lab database (default btc_lab.db)")
    ap.add_argument("--live-db", default=os.path.join(HERE, "btc_15m_coinbase.db"), help="live database, read only")
    ap.add_argument("--live-url", default="http://127.0.0.1:8000", help="live predictor's page")
    ap.add_argument("--port", type=int, default=8001, help="port for the /testing page (default 8001)")
    ap.add_argument("--every", type=float, default=2.0, help="seconds between side-by-side readings (default 2)")
    ap.add_argument("--days", type=int, default=30, help="days of history to backtest on (default 30)")
    ap.add_argument("--retest-hours", type=float, default=4, help="hours between backtests (default 4)")
    ARGS = args = ap.parse_args()

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"Testing page: http://localhost:{args.port}/testing", flush=True)
    db = open_lab(args.db)
    try:
        sync_history(live.make_feed("coinbase"), db, args.days)
    except requests.RequestException as e:
        print(f"History download failed ({e}); continuing with what is stored.", flush=True)
    publish(status=status(db, args))
    db.close()
    threading.Thread(target=market_recorder, args=(args,), daemon=True).start()
    threading.Thread(target=tester, args=(args,), daemon=True).start()
    try:
        shadow(args)
    except KeyboardInterrupt:
        print("\nStopped.")


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 15m Lab</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@400;500;600;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #090a0c; --card: #101216; --line: #1e2229;
    --text: #eef0f3; --muted: #868d9a; --faint: #565c67;
    --up: #2ebd85; --down: #f6465d; --warn: #e8b84a;
    --mono: "JetBrains Mono", ui-monospace, Menlo, Consolas, monospace;
    --sans: "Archivo", "Helvetica Neue", Arial, sans-serif;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; }
  body { background: var(--bg); color: var(--text); font-family: var(--sans); font-size: 14px; line-height: 1.45;
         padding: 0 clamp(16px, 2.5vw, 32px) 32px; }
  .page { max-width: 1320px; margin: 0 auto; display: flex; flex-direction: column; gap: 16px; }
  header { display: flex; align-items: center; justify-content: space-between; gap: 16px; flex-wrap: wrap;
           padding: 16px 0; border-bottom: 1px solid var(--line); }
  .brand { display: flex; align-items: center; gap: 12px; font-weight: 800; font-size: 15px; letter-spacing: .04em; text-transform: uppercase; }
  .brand a { font-weight: 400; color: var(--muted); letter-spacing: .02em; text-transform: none; text-decoration: none; }
  .brand a:hover { color: var(--text); }
  .tag { font-size: 11px; font-weight: 600; letter-spacing: .1em; padding: 2px 8px; border-radius: 4px;
         background: rgba(232,184,74,.14); color: var(--warn); }
  .clock { font-family: var(--mono); color: var(--muted); font-size: 13px; }
  .clock b { color: var(--text); font-size: 18px; margin-left: 8px; }
  .alert { background: rgba(246,70,93,.12); border: 1px solid rgba(246,70,93,.4); color: var(--down);
           padding: 8px 12px; border-radius: 6px; font-size: 13px; }
  .card { background: var(--card); border: 1px solid var(--line); border-radius: 8px; min-width: 0; }
  .card > h2 { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin: 0;
               padding: 12px 16px; border-bottom: 1px solid var(--line);
               font-size: 11px; font-weight: 600; letter-spacing: .1em; text-transform: uppercase; color: var(--muted); }
  .meta { padding: 10px 16px; color: var(--muted); font-size: 13px; border-bottom: 1px solid var(--line); }
  .meta b { color: var(--text); font-family: var(--mono); font-weight: 500; }
  .foot { padding: 10px 16px 14px; color: var(--faint); font-size: 12px; }
  .scroll { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; }
  th, td { padding: 9px 16px; border-bottom: 1px solid var(--line); text-align: right; white-space: nowrap; }
  th { font-size: 11px; font-weight: 600; letter-spacing: .06em; text-transform: uppercase; color: var(--faint); }
  th:first-child, td:first-child { text-align: left; white-space: normal; min-width: 220px; }
  td { font-family: var(--mono); font-size: 13px; }
  td:first-child { font-family: var(--sans); }
  td small { display: block; color: var(--faint); font-family: var(--sans); font-size: 12px; max-width: 460px; }
  tr.base td { color: var(--muted); }
  tr:last-child td { border-bottom: 0; }
  .up { color: var(--up); } .down { color: var(--down); } .dim { color: var(--faint); }
  .pill { font-family: var(--sans); font-size: 12px; font-weight: 600; padding: 2px 8px; border-radius: 10px; }
  .pill.helps { background: rgba(46,189,133,.15); color: var(--up); }
  .pill.slight { background: rgba(232,184,74,.14); color: var(--warn); }
  .pill.no { background: #1a1d22; color: var(--muted); }
  button { font: 600 12px var(--sans); color: var(--text); background: #1a1d22; border: 1px solid var(--line);
           border-radius: 6px; padding: 6px 12px; cursor: pointer; letter-spacing: .02em; text-transform: none; }
  button:hover:not(:disabled) { border-color: var(--muted); }
  button:disabled { color: var(--faint); cursor: default; }
  .grid2 { display: grid; gap: 16px; grid-template-columns: 1.3fr 1fr; }
  @media (max-width: 980px) { .grid2 { grid-template-columns: 1fr; } }
  .kv { margin: 0; }
  .kv div { display: flex; justify-content: space-between; align-items: baseline; gap: 16px; padding: 9px 16px; border-bottom: 1px solid var(--line); }
  .kv div:last-child { border-bottom: 0; }
  .kv dt { color: var(--muted); font-size: 13px; }
  .kv dd { margin: 0; font-family: var(--mono); font-size: 13px; text-align: right; }
  .kv dd small { display: block; color: var(--faint); font-family: var(--sans); font-size: 12px; white-space: normal; }
</style>
</head>
<body>
<div class="page">
  <header>
    <div class="brand">BTC 15m <span class="tag">LAB</span> <a id="back" href="#">&larr; Live chart</a> <a href="/testing">Test chart</a></div>
    <div class="clock"><span id="win">&ndash;</span><b id="left">&ndash;</b></div>
  </header>
  <div class="alert" id="err" hidden></div>

  <div class="grid2">
    <section class="card">
      <h2>Right now <span id="nowsub" style="text-transform:none;letter-spacing:0;font-weight:400"></span></h2>
      <div class="scroll"><table id="nowt"></table></div>
      <div class="foot">Each experiment is applied on top of the live model's probability. Market rows are the midpoint of the bid/ask for "Up".</div>
    </section>
    <section class="card">
      <h2>Signals now</h2>
      <dl class="kv" id="sig"></dl>
    </section>
  </div>

  <section class="card">
    <h2>Backtest on history <button id="rerun">Re-run backtest</button></h2>
    <div class="meta" id="btmeta">No backtest yet.</div>
    <div class="scroll"><table id="bt"></table></div>
    <div class="foot">Fitted on older history, scored on recent days it never saw. <b>Log loss gain</b>: how much better than the live model (positive is better; +0.0005 is the bar the live self-tuning uses).
      <b>Signal t</b>: how clearly the signal showed up in training (|t| &ge; 4 is what live tuning needs). <b>% off</b>: average gap between stated confidence and how often it was right.</div>
  </section>

  <section class="card">
    <h2>Live side-by-side</h2>
    <div class="meta" id="bdmeta"></div>
    <div class="scroll"><table id="bd"></table></div>
    <div class="foot">Readings every 10 s, scored when each interval ends, compared on exactly the same moments as the live model. Treat differences as noise until a few hundred intervals (several days) have been scored.</div>
  </section>

  <section class="card">
    <h2>Data being collected</h2>
    <dl class="kv" id="data"></dl>
  </section>
</div>
<script>
const $ = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const pct = (x, d = 1) => x == null ? '&ndash;' : (x * 100).toFixed(d) + '%';
const sgn = (x, d = 4) => x == null ? '&ndash;' : `<span class="${x > 0 ? 'up' : x < 0 ? 'down' : ''}">${x > 0 ? '+' : ''}${x.toFixed(d)}</span>`;
const clock = t => new Date(t * 1000).toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
const ago = t => { const m = (Date.now() / 1000 - t) / 60; return m < 60 ? `${Math.round(m)} min ago` : `${(m / 60).toFixed(1)} h ago`; };
let D = null, names = {};
const label = k => k === 'live' ? 'Live model' : k === 'mkt_polymarket' ? 'Polymarket odds' : k === 'mkt_kalshi' ? 'Kalshi odds' : (names[k] || k);
const verdict = v => `<span class="pill ${v === 'helps' ? 'helps' : v === 'slight' ? 'slight' : 'no'}">${v}</span>`;

function render() {
  if (!D) return;
  // Opened through the live page (or a tunnel to it): the live chart is just /. Opened on the lab's own port: go to the live port.
  $('back').href = location.port === String(D.lab_port) ? `${location.protocol}//${location.hostname}:${D.live_port}/` : '/';
  const err = D.error || D.test_error;
  $('err').hidden = !err; $('err').textContent = err ? (D.error ? 'Live feed: ' : 'Backtest: ') + err : '';
  const R = D.report;
  names = {}; (R?.experiments || []).forEach(e => names[e.key] = e.name);

  const N = D.now;
  if (N) {
    $('win').textContent = `${clock(N.start)}–${clock(N.end)}`;
    $('nowsub').textContent = `start ${N.start_price.toLocaleString(undefined, {maximumFractionDigits: 2})} · now ${N.price.toLocaleString(undefined, {maximumFractionDigits: 2})}`;
    const base = N.probs.live;
    $('nowt').innerHTML = '<tr><th>Model</th><th>P(up)</th><th>vs live</th><th>Call</th></tr>' +
      Object.entries(N.probs).map(([k, p]) => `<tr class="${k === 'live' ? 'base' : ''}"><td>${esc(label(k))}</td><td>${pct(p)}</td>` +
        `<td>${k === 'live' ? '' : sgn((p - base) * 100, 1) + ' pts'}</td>` +
        `<td class="${p >= .5 ? 'up' : 'down'}">${p >= .5 ? 'UP ' + pct(p, 0) : 'DOWN ' + pct(1 - p, 0)}</td></tr>`).join('');
    const F = N.features;
    $('sig').innerHTML = F ? Object.entries(F).map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${sgn(v, 2)}</dd></div>`).join('')
                           : '<div><dt>Waiting for a backtest and candles</dt><dd></dd></div>';
  }

  $('rerun').disabled = D.busy; $('rerun').textContent = D.busy ? 'Running…' : 'Re-run backtest';
  if (R) {
    $('btmeta').innerHTML = `Live model: <b>${esc(R.champion)}</b><br>Tuned on <b>${R.train_days.toFixed(1)}</b> days, tested on the most recent <b>${R.hold_days.toFixed(1)}</b> days ` +
      `(${R.n_hold.toLocaleString()} readings) · run ${ago(R.at)}`;
    const row = (name, desc, t, h, d, v, cls = '') => `<tr class="${cls}"><td>${esc(name)}<small>${esc(desc)}</small></td><td>${t}</td>` +
      `<td>${d}</td><td>${h.ll.toFixed(4)}</td><td>${pct(h.acc)}</td><td>${pct(h.early)}</td><td>${(h.ece * 100).toFixed(1)}</td><td>${v}</td></tr>`;
    $('bt').innerHTML = '<tr><th>Experiment</th><th>Signal t</th><th>Log loss gain</th><th>Log loss</th><th>Right</th><th>Right, min 1–5</th><th>% off</th><th>Verdict</th></tr>' +
      row('Live model', 'The baseline everything is compared with.', '', R.base, '', '', 'base') +
      [...R.experiments].sort((a, b) => b.d_ll - a.d_ll).map(e => row(e.name, e.desc,
        Object.entries(e.t).map(([k, t]) => `<span class="${Math.abs(t) >= 4 ? '' : 'dim'}">${t.toFixed(1)}</span>`).join(' / '),
        e.hold, sgn(e.d_ll), verdict(e.verdict))).join('');
  }

  const B = D.board || [], live = B.find(b => b.name === 'live');
  $('bdmeta').innerHTML = `<b>${D.status?.intervals ?? 0}</b> intervals scored so far.`;
  $('bd').innerHTML = B.length ? '<tr><th>Model</th><th>Intervals</th><th>Readings</th><th>Log loss gain</th><th>Log loss</th><th>Right</th><th>Right, first 5 min</th><th>% off</th></tr>' +
    [...B].sort((a, b) => (b.name === 'live') - (a.name === 'live') || b.d_ll - a.d_ll).map(b => `<tr class="${b.name === 'live' ? 'base' : ''}"><td>${esc(label(b.name))}</td>` +
      `<td>${b.intervals}</td><td>${b.n.toLocaleString()}</td><td>${b.name === 'live' ? '' : sgn(b.d_ll)}</td><td>${b.ll.toFixed(4)}</td>` +
      `<td>${pct(b.acc)}</td><td>${pct(b.early)}</td><td>${(b.ece * 100).toFixed(1)}</td></tr>`).join('')
    : '<tr><td class="dim">Nothing scored yet. The first results appear when the current interval ends.</td></tr>';

  const S = D.status || {}, rows = [];
  if (S.candles) rows.push(['Candles with high/low/volume', `${S.candles.n.toLocaleString()} min`, `${S.candles.days.toFixed(1)} days, for backtests`]);
  if (S.flow) rows.push(['Order book &amp; trade flow (live recorder)', S.flow.error ? 'unavailable' : `${S.flow.n.toLocaleString()} rows`,
    S.flow.error ? esc(S.flow.error) : `${S.flow.days.toFixed(1)} days · backtestable after ~14 days`]);
  for (const [k, m] of Object.entries(S.market || {}))
    rows.push([`${k[0].toUpperCase() + k.slice(1)} odds`, `${m.ok.toLocaleString()} / ${m.n.toLocaleString()} ok`,
      m.last_error ? `last attempt failed: ${esc(m.last_error)}` : `${m.days.toFixed(1)} days`]);
  rows.push(['Side-by-side intervals scored', `${S.intervals ?? 0}`, '']);
  $('data').innerHTML = rows.map(([a, b, c]) => `<div><dt>${a}</dt><dd>${b}${c ? `<small>${c}</small>` : ''}</dd></div>`).join('');
}

function tick() {
  if (D?.now) {
    const left = Math.max(0, D.now.end - Date.now() / 1000);
    $('left').textContent = `${Math.floor(left / 60)}:${String(Math.floor(left % 60)).padStart(2, '0')}`;
  }
}
async function poll() {
  try { D = await (await fetch('/testing/data', {cache: 'no-store'})).json(); render(); }
  catch (e) { $('err').hidden = false; $('err').textContent = 'Lab not reachable.'; }
}
$('rerun').onclick = async () => { $('rerun').disabled = true; await fetch('/testing/rerun', {method: 'POST'}); poll(); };
poll(); setInterval(poll, 2000); setInterval(tick, 250);
</script>
</body>
</html>
"""

TEST_CSS = """  .tag { font-size: 11px; font-weight: 600; letter-spacing: .1em; padding: 2px 8px; border-radius: 4px;
         background: rgba(232,184,74,.14); color: #e8b84a; }
  .nav { font-weight: 400; font-size: 13px; letter-spacing: .02em; text-transform: none; display: flex; gap: 14px; }
  .nav a { color: var(--muted); text-decoration: none; } .nav a:hover { color: var(--text); }
  .k.livecone { border-top: 2px dashed rgba(238,240,243,.6); }
  .cmp table { width: 100%; border-collapse: collapse; }
  .cmp th, .cmp td { padding: 8px 16px; border-bottom: 1px solid var(--line); text-align: right; white-space: nowrap; font-size: 13px; }
  .cmp th { font-size: 11px; font-weight: 600; letter-spacing: .06em; text-transform: uppercase; color: var(--faint); }
  .cmp td { font-family: var(--mono); }
  .cmp th:first-child, .cmp td:first-child { text-align: left; font-family: var(--sans); color: var(--muted); white-space: normal; }
  .cmp tr.grp td { color: var(--faint); font-size: 11px; font-weight: 600; letter-spacing: .06em; text-transform: uppercase; padding-top: 14px; }
  .cmp tr:last-child td { border-bottom: 0; }
  .cmp .scroll { overflow-x: auto; }
  .cmp h2 a { color: var(--muted); text-decoration: none; text-transform: none; letter-spacing: 0; font-weight: 400; font-size: 12px; }
</style>"""

TEST_CARD = """  <section class="card cmp">
    <h2>Test model vs main page <a href="/testing/results">every experiment &rarr;</a></h2>
    <div class="scroll"><table id="cmp"><tbody><tr><td>Waiting for data</td></tr></tbody></table></div>
  </section>

  <div class="hero">"""

LIVE_BAND_JS = """    // The main page's 90% range, for comparison
    if (D.live_k_now) {
      const sgL = D.sigma / D.live_k_now, spL = sgL * Math.sqrt(left / 60), muL = (D.live_tilt || 0) * spL;
      const bandL = (z, s) => P * Math.exp((left > 0 ? muL * s / left : 0) + z * sgL * Math.sqrt(Math.max(0, s) / 60));
      g.save(); g.setLineDash([3, 4]); g.strokeStyle = rgba(PAPER, 0.6); g.lineWidth = 1.25;
      for (const z of [1.645, -1.645]) { g.beginPath(); for (let i = 0; i <= N; i++) { const s = left * i / N; g.lineTo(X(now + s), Y(bandL(z, s))); } g.stroke(); }
      g.restore();
    }
    // Example paths
"""

TEST_JS = r"""</script>
<script>
const LABELS = { volburst: 'Volume burst', rangepos: 'Range position', tod: 'Time of day', flow: 'Candle buying pressure',
                 trend1h: 'Trend 1 h', trend4h: 'Trend 4 h', round: 'Round-number level', prevlevels: 'Previous high/low' };
const SCALE_X = ['tod', 'volburst'];
const f1 = v => (v * 100).toFixed(1) + '%';

function liveP(st) {                    // what the main page says right now, from its own numbers
  if (!D.live_k_now) return st.p;
  const spL = D.sigma / D.live_k_now * Math.sqrt(st.left / 60), muL = (D.live_tilt || 0) * spL;
  return st.left > 0 ? phi((Math.log(st.P / st.S) + muL) / spL) : st.p;
}

function model() {                      // replaces the main page's Model panel
  const T = D && D.test, btn = $('retrain'), busy = D && D.training;
  btn.disabled = !!busy || Date.now() - asked < 3000;
  btn.textContent = busy ? 'Backtest running…' : 'Re-run backtest';
  if (!T) { kv($('tune'), [['Status', (D && D.error) || 'Waiting for the lab']]); return; }
  const rows = [];
  if (!T.names.length) rows.push(['Signals', 'none passed yet', 'same as the main page']);
  for (const n of T.names) {
    const v = T.values[n], b = T.beta[n];
    rows.push([LABELS[n] || n, v == null ? '–' : (v > 0 ? '+' : '') + v.toFixed(2),
               `weight ${b > 0 ? '+' : ''}${b.toFixed(3)} · ${SCALE_X.includes(n) ? 'confidence' : 'direction'}`]);
  }
  if (T.active) rows.push(['Effect now', `confidence ×${T.scale.toFixed(2)}`, `lean ${T.lean >= 0 ? '+' : ''}${T.lean.toFixed(3)}`]);
  if (D.learn) rows.push(['Last backtest', busy ? 'running now' : ago(D.learn.checked_at)]);
  if (D.train_note) rows.push(['Note', D.train_note]);
  kv($('tune'), rows);
}

function compare() {
  const st = state(), T = D && D.test, box = $('cmp');
  if (!st || !T) return;
  const pl = liveP(st), pt = st.p, B = T.backtest, L = T.live;
  const pp = v => v == null ? '–' : f1(v);
  const cls = d => d > 1e-9 ? 'up' : d < -1e-9 ? 'down' : '';
  const higher = (a, b) => a == null || b == null ? '–' : `<span class="${cls(a - b)}">${a - b >= 0 ? '+' : ''}${((a - b) * 100).toFixed(1)} pts</span>`;
  const lower = (a, b, d, unit = '') => a == null || b == null ? '–' : `<span class="${cls(b - a)}">${b - a >= 0 ? '+' : ''}${(b - a).toFixed(d)}${unit}</span>`;
  const grp = t => `<tr class="grp"><td colspan="4">${t}</td></tr>`;
  const block = (base, test) =>
    `<tr><td>Calls right</td><td>${pp(base.acc)}</td><td>${pp(test.acc)}</td><td>${higher(test.acc, base.acc)}</td></tr>` +
    `<tr><td>Calls right, first 5 min</td><td>${pp(base.early)}</td><td>${pp(test.early)}</td><td>${higher(test.early, base.early)}</td></tr>` +
    `<tr><td>Log loss (lower is better)</td><td>${base.ll.toFixed(4)}</td><td>${test.ll.toFixed(4)}</td><td>${lower(test.ll, base.ll, 4)}</td></tr>` +
    `<tr><td>% off (stated vs actual)</td><td>${(base.ece * 100).toFixed(1)}</td><td>${(test.ece * 100).toFixed(1)}</td><td>${lower(test.ece * 100, base.ece * 100, 1, ' pts')}</td></tr>`;
  let h = '<tr><th></th><th>Main page</th><th>Test model</th><th>Test better by</th></tr>';
  h += grp('Right now');
  h += st.early ? '<tr><td>Chance of Up</td><td>–</td><td>–</td><td>shown from 5 minutes in</td></tr>'
    : `<tr><td>Chance of Up</td><td>${pp(pl)}</td><td>${pp(pt)}</td><td>${Math.abs(pt - pl) < 0.0005 ? 'same' : `${Math.abs((pt - pl) * 100).toFixed(1)} pts toward ${pt > pl ? 'Up' : 'Down'}`}</td></tr>`;
  h += grp(L ? `Live side-by-side · ${L.intervals} intervals, scored on the same moments` : 'Live side-by-side · nothing scored yet (first results when an interval ends)');
  if (L) h += block({ acc: L.base_acc, early: L.base_early, ll: L.base_ll, ece: L.base_ece }, L);
  if (B && B.test) { h += grp(`Backtest · ${B.hold_days.toFixed(1)} unseen days`); h += block(B.base, B.test); }
  box.innerHTML = h;
}
setInterval(() => { try { compare(); } catch (e) {} }, 500);
</script>
</body>"""


def build_test_page():
    """The main page, pointed at the test model's data, with a comparison table on top and
    the main page's 90% range drawn over the test model's cone."""
    page = live.PAGE

    def sub(old, new):
        nonlocal page
        assert old in page, old[:80]
        page = page.replace(old, new)

    sub("<title>BTC 15m</title>", "<title>BTC 15m Test</title>")
    sub("</style>", TEST_CSS)
    sub('<div class="brand"><span class="dot"></span>BTC 15m <span class="src" id="status">connecting</span></div>',
        '<div class="brand"><span class="dot"></span>BTC 15m <span class="tag">TEST</span> <span class="src" id="status">connecting</span>'
        '<span class="nav"><a href="/">&larr; Main page</a><a href="/testing/results">All results</a></span></div>')
    sub('  <div class="hero">', TEST_CARD)
    sub('<span><i class="k path"></i>Example paths</span>',
        '<span><i class="k path"></i>Example paths</span>\n      <span><i class="k livecone"></i>Main page&rsquo;s 90% range</span>')
    sub('<h2>Model <button id="retrain" type="button">Retrain now</button></h2>',
        '<h2>Test model <button id="retrain" type="button">Re-run backtest</button></h2>')
    sub("<footer>Confidence and the track record are measured on recent days the model was not tuned on.",
        "<footer>TEST PROJECTION: the main page&rsquo;s model plus the signals that passed the backtest. Nothing here changes "
        "the main page. Confidence and the track record are measured on recent days the model was not tuned on.")
    sub("fetch('/data', { cache: 'no-store' })", "fetch('/testing/live', { cache: 'no-store' })")
    sub("fetch('/retrain', { method: 'POST' })", "fetch('/testing/rerun', { method: 'POST' })")
    page = page.replace("Press Retrain now", "Run a backtest")
    sub("    // Example paths\n", LIVE_BAND_JS)
    sub("</script>\n</body>", TEST_JS)
    return page


TEST_PAGE = build_test_page()

if __name__ == "__main__":
    main()
