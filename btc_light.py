#!/usr/bin/env python3
"""
btc_light.py - a red / yellow / green light for the live call, with its own
database (btc_light.db) and its own page at http://localhost:8002/testing-2
(also reachable as /testing-2 on the live page and the lab).

It only ever READS the live predictor (its /data feed and its database) and the
lab's /testing/data feed. Nothing here changes a live call.

What the light means
  "How much should the current call be trusted right now?" It works from a hold
  chance: the probability that the side the main page is calling is still the
  right side when the interval ends.
    RED     a safety stop is on (live feed down or stale, exchanges disagree),
            the hold chance is under the red line, or a proven "stop" flag is on
    GREEN   the hold chance has been over the green line for a minute and no
            proven warning flag is on
    YELLOW  everything else
  For the first 5 minutes of an interval there is no light, call or projection:
  too much can still change. Those minutes are recorded but never scored or fitted.

What it records (every 10 s)
  depth   Coinbase's full order book boiled down to: BTC resting within $10 ...
          $1,000 of the price on each side, the five largest single price levels
          per side, any of those that shrank by half since the last look, and
          BTC resting in each $5 band out to $400 either side (drawn on the chart).
          Past order books cannot be downloaded, so this only grows from now.
  light   the colour, the hold chance, every warning flag and the numbers behind
          them, scored right or wrong when the interval ends.

How it gets better over time
  1. The green and red lines are fitted on history and checked on the most
     recent days the fit never saw (same rule as the live self-tuning).
  2. Warning flags (thin book, large order ahead, large order eaten or pulled,
     sweep, choppy, wild) are logged from day one but only "watched". A flag may
     change the colour once the record shows calls made while it was on were
     clearly worse than their percentages claimed. It loses that right again if
     the record stops showing it.
  3. Once 14 days of order-book readings exist, the book signals are fitted on
     top of the live model (older 80% to fit, newest 20% to score). If they beat
     the live model on the unseen days, the light uses them for its hold chance.
  4. Every training run is stored, so the page shows whether the light improves.

Usage
  python btc_light.py                    # page on :8002, reads live on :8000, lab on :8001
  python btc_light.py --retrain-hours 4 --days 30
"""
import argparse
import bisect
import json
import math
import os
import sqlite3
import statistics
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from statistics import NormalDist

import requests

import btc_15 as live
import btc_lab as lab

HERE = os.path.dirname(os.path.abspath(__file__))
BAR = live.BAR
ND = NormalDist()

DISTS = (10, 25, 50, 100, 250, 500, 1000)   # dollars from the price; BTC resting within each is recorded
BIN, SPAN = 5, 400         # the chart's picture of the book: BTC resting in each $5 band, out to $400 either side
GREEN_TARGET = 0.85        # the green line is the lowest hold chance at which calls were right this often
RED_TARGET = 0.65          # the red line is the highest hold chance just under which calls were right this rarely
MIN_N = 200                # readings needed in a 5-point band before it is trusted
HOLD_SECS = 60             # how long the hold chance must stay over the green line
WARMUP = 300               # seconds into an interval before any call, light or projection is shown or scored
MIN_DEPTH_DAYS = 14        # order-book history needed before the book signals are fitted
PROVE_BLOCKS, PROVE_GAP, PROVE_T = 40, 0.05, 2.0   # what a flag needs before it may change the colour
FLAG_SECS = 60             # how long an "eaten" / "pulled" flag stays on after it is seen

# Starting thresholds for the flags. They decide when a flag is "on"; whether a flag matters is learned.
CROSS_N = 3                # crossings of the start price in this interval
WILD_X = 1.5               # recent swings vs the usual
THIN_ICE = 0.5             # resting BTC between price and start, as a share of what should trade in the time left
WALL_X = 2.0               # a single level this many times the usual large level
SWEEP_X = 4.0              # 30 s of market orders against the lead, vs a usual 30 s

# key: (name, what it does once proven, what it looks for)
FLAGS = {
    "choppy": ("Choppy", "yellow", f"The price has crossed the start {CROSS_N}+ times this interval."),
    "wild": ("Wild market", "yellow", f"Recent swings are {WILD_X:g}x the usual or more."),
    "thin": ("Thin book", "yellow", "Little is resting between the price and the start, compared with how much is trading."),
    "wall": ("Large order ahead", "yellow", "An unusually large order sits close by, between the price and the start."),
    "pulled": ("Large order pulled", "yellow", "A large order protecting the lead vanished without being traded."),
    "eaten": ("Large order eaten", "red", "A large order protecting the lead is being traded away."),
    "sweep": ("Sweep against the lead", "red", f"Market orders against the lead are {SWEEP_X:g}x a usual 30 seconds."),
}
# Book signals the hold chance may learn to use. The first three raise or lower confidence
# (multiplied by the live score, as the lab's "tod" and "volburst" are); the last two lean UP or DOWN.
SCALERS = ("ice", "wallp", "crossx")
MODEL_X = SCALERS + ("booktilt", "netflow")
lab.SCALE_X.update(SCALERS)                # this process's copy only; lets lab.fit / lab.score_of treat them as scalers


def clip(x, lim):
    return max(-lim, min(lim, x))


# ----------------------------------------------------------------- data

def open_db(path):
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS depth (t REAL, start INTEGER, mid REAL, bids TEXT, asks TEXT, walls TEXT, drops TEXT);
        CREATE TABLE IF NOT EXISTS light (t REAL, start INTEGER, left REAL, color TEXT, hold REAL, p REAL,
            p_live REAL, p_test REAL, flags TEXT, feats TEXT, version INTEGER, up INTEGER);
        CREATE INDEX IF NOT EXISTS light_start ON light (start);
        CREATE TABLE IF NOT EXISTS versions (id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL, report TEXT);
    """)
    if "profile" not in {r[1] for r in db.execute("PRAGMA table_info(depth)")}:
        db.execute("ALTER TABLE depth ADD COLUMN profile TEXT")
    have = {r[1] for r in db.execute("PRAGMA table_info(light)")}
    for col in ("p_book", "cone_lo", "cone_hi", "book_lo", "book_hi", "close"):
        if col not in have:                # added with the order-book projection
            db.execute(f"ALTER TABLE light ADD COLUMN {col} REAL")
    db.commit()
    return db


def read_live(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)


def summarise(g):
    """Coinbase's full book (level 2) as a small summary, plus the size at every price within
    $1,000 so the next look can tell which large orders shrank."""
    mid = (float(g["bids"][0][0]) + float(g["asks"][0][0])) / 2
    out, sizes, prof = {"mid": mid}, {}, {}
    for side in ("bids", "asks"):
        cum, tot, k, levels, seen = [], 0.0, 0, [], {}
        for row in g[side]:
            price, size = float(row[0]), float(row[1])
            dist = abs(price - mid)
            while k < len(DISTS) and dist > DISTS[k]:
                cum.append(tot)
                k += 1
            if k == len(DISTS):
                break
            tot += size
            seen[price] = size
            if dist <= 500:
                levels.append((size, price))
            if dist <= SPAN:
                prof[int(price // BIN)] = prof.get(int(price // BIN), 0.0) + size
        cum += [tot] * (len(DISTS) - len(cum))
        levels.sort(reverse=True)
        out[side] = [round(x, 4) for x in cum]
        out[side[0] + "walls"] = [[p, round(s, 4)] for s, p in levels[:5]]
        sizes[side[0]] = seen
    lo = int((mid - SPAN) // BIN)
    out["profile"] = {"lo": lo * BIN, "n": [round(prof.get(lo + i, 0.0), 2) for i in range(2 * SPAN // BIN + 1)]}
    return out, sizes


def interp(cum, dist):
    """BTC resting within `dist` dollars, read off the recorded distances."""
    x0 = y0 = 0.0
    for x, y in zip(DISTS, cum):
        if dist <= x:
            return y0 + (y - y0) * (dist - x0) / (x - x0)
        x0, y0 = x, y
    return y0


DEPTH = deque(maxlen=400)                  # about the last hour of book summaries
DEPTH_ERR = [None]


def depth_recorder(args):
    db, feed, prev = open_db(args.db), live.Coinbase(), None
    while True:
        t = time.time()
        try:
            book, sizes = summarise(feed.get(feed.base + "/book", level=2))
            book.update(t=t, drops=[])
            if prev and t - prev["t"] < 60:
                for side in "ba":
                    for price, old in prev[side + "walls"]:
                        new = sizes[side].get(price, 0.0)
                        if new <= old * 0.5:
                            book["drops"].append([side, price, old, round(new, 4)])
            db.execute("INSERT INTO depth (t, start, mid, bids, asks, walls, drops, profile) VALUES (?,?,?,?,?,?,?,?)",
                       (t, int(t) // BAR * BAR, book["mid"], json.dumps(book["bids"]), json.dumps(book["asks"]),
                        json.dumps({"b": book["bwalls"], "a": book["awalls"]}), json.dumps(book["drops"]),
                        json.dumps(book["profile"])))
            db.commit()
            DEPTH.append(book)
            prev, DEPTH_ERR[0] = book, None
        except Exception as e:
            DEPTH_ERR[0] = str(e)[:200]
        time.sleep(max(1.0, args.depth_every - (time.time() - t)))


def load_depth(db):
    """The last hour of book summaries, so a restart does not blank the chart or the usual-order size."""
    for t, mid, bids, asks, walls, drops, profile in db.execute(
            "SELECT t, mid, bids, asks, walls, drops, profile FROM depth WHERE t > ? ORDER BY t", (time.time() - 3600,)):
        w = json.loads(walls)
        DEPTH.append({"t": t, "mid": mid, "bids": json.loads(bids), "asks": json.loads(asks), "bwalls": w["b"],
                      "awalls": w["a"], "drops": json.loads(drops), "profile": json.loads(profile) if profile else None})


def flow_window(path, now):
    """Trade pace and how far the exchanges disagree, from the live recorder's last half hour."""
    try:
        con = read_live(path)
        rows = con.execute("SELECT t, bid, ask, buy_vol, sell_vol, kraken, bitstamp FROM flow "
                           "WHERE rowid > (SELECT MAX(rowid) FROM flow) - 200 AND t > ? ORDER BY t", (now - 1800,)).fetchall()
        con.close()
    except sqlite3.Error:
        return {}
    vols = [(t, b, s) for t, _, _, b, s, _, _ in rows if b is not None and s is not None]
    out = {}
    if vols and now - vols[-1][0] < 60:
        last5 = [(b, s) for t, b, s in vols if t > now - 300]
        out["pace"] = sum(b + s for b, s in last5) / 5                      # BTC a minute
        out["typ30"] = statistics.median(b + s for _, b, s in vols) * 3    # a usual 30 seconds
        for secs in (20, 30):
            out[f"buy{secs}"] = sum(b for t, b, s in vols if t > now - secs)
            out[f"sell{secs}"] = sum(s for t, b, s in vols if t > now - secs)
    gaps = []
    for t, bid, ask, _, _, kr, bs in rows:
        others = [x for x in (kr, bs) if x]
        if bid and ask and others:
            gaps.append((t, (bid + ask) / 2 - sum(others) / len(others)))
    if len(gaps) >= 30 and now - gaps[-1][0] < 60:
        med = statistics.median(g for _, g in gaps)
        out["gapdev"] = gaps[-1][1] - med
        out["gapthr"] = max(10.0, 4 * statistics.median(abs(g - med) for _, g in gaps))
    return out


# -------------------------------------------------------------- signals

RECENT = {}                                # flag -> (until, text), for flags that should linger


def signals(d, fl, now):
    """Everything the light looks at for one reading: (numbers, flags that are on, safety stops)."""
    price, start = d["price"], d["start_price"]
    left = max((d["end"] - d["now"]) / 60, 0.01)
    side = (price > start) - (price < start)
    lead = abs(price - start)
    f = {"side": side, "lead": round(lead, 2), "left": round(left, 2)}
    flags, stops = {}, []
    basis = d.get("basis") or 0.0          # the live page shifts exchange prices by this; book prices get the same shift

    last, signs = 0.0, []
    for t, p in d.get("ticks") or []:
        if t >= last + 10:
            last = t
            if p != start:
                signs.append(p > start)
    f["cross"] = sum(1 for a, b in zip(signs, signs[1:]) if a != b)
    f["crossx"] = min(f["cross"], 4) / 4
    if f["cross"] >= CROSS_N:
        flags["choppy"] = f"Crossed the start {f['cross']} times this interval"
    f["swing"] = d.get("swing")
    if f["swing"] and f["swing"] >= WILD_X:
        flags["wild"] = f"Swings are {f['swing']:.1f}x the usual"

    for k in ("ice", "wallp", "booktilt", "netflow"):
        f[k] = 0.0
    if fl.get("typ30"):
        f["pace"] = round(fl["pace"], 3)
        f["netflow"] = clip((fl["buy30"] - fl["sell30"]) / fl["typ30"], 4) / 4
        against = fl["sell30"] if side > 0 else fl["buy30"] if side < 0 else 0.0
        f["sweepx"] = round(against / fl["typ30"], 2)
        if f["sweepx"] >= SWEEP_X:
            flags["sweep"] = f"{against:.1f} BTC of market orders against the lead in 30 s ({f['sweepx']:.0f}x usual)"
    if "gapdev" in fl:
        f["gapdev"] = round(fl["gapdev"], 2)
        if abs(fl["gapdev"]) > fl["gapthr"] and abs(fl["gapdev"]) >= 0.5 * lead:
            stops.append(f"Exchanges disagree: Coinbase is ${abs(fl['gapdev']):.0f} off its usual gap to Kraken and Bitstamp")

    books = list(DEPTH)                    # a copy: the recorder thread appends while this runs
    book = books[-1] if books and now - books[-1]["t"] < 30 else None
    if book:
        b50, a50 = book["bids"][2], book["asks"][2]
        f["booktilt"] = (b50 - a50) / (b50 + a50) if b50 + a50 > 0 else 0.0
        f["book"] = {"bids": book["bids"], "asks": book["asks"]}
        typical = statistics.median(s for bk in books for w in (bk["bwalls"], bk["awalls"]) for _, s in w) \
            if len(books) >= 30 else None
        if side:
            prot, walls = (book["bids"], book["bwalls"]) if side > 0 else (book["asks"], book["awalls"])
            f["cover"] = round(interp(prot, lead), 3)
            if fl.get("pace"):
                f["ice_raw"] = f["cover"] / max(fl["pace"] * left, 1e-9)
                f["ice"] = clip(math.log(max(f["ice_raw"], 1e-6)), 2) / 2
                if f["ice_raw"] < THIN_ICE:
                    flags["thin"] = (f"Only {f['cover']:.1f} BTC between price and start, "
                                     f"about {f['cover'] / fl['pace'] * 60:.0f} s of trading")
            between = [(s, p + basis) for p, s in walls if min(start, price) < p + basis < max(start, price)]
            if between and typical:
                size, wp = max(between)
                f.update(wall=size, wallprice=wp, walldist=round(abs(price - wp), 2), wallx=round(size / typical, 2))
                f["wallp"] = min(f["wallx"], 6) / 6
                near = max(25.0, 0.5 * price * (d.get("sigma") or 0) * math.sqrt(left))
                if f["wallx"] >= WALL_X and f["walldist"] <= near:
                    flags["wall"] = f"{size:.1f} BTC resting ${f['walldist']:.0f} away, between price and start"
            if typical:
                for s, wp, old, new in book["drops"]:
                    wp += basis
                    if s == ("b" if side > 0 else "a") and old >= WALL_X * typical and side * (wp - start) > 0:
                        traded = fl.get("sell20" if side > 0 else "buy20")
                        kind = "eaten" if traded is not None and traded >= 0.5 * (old - new) else "pulled"
                        RECENT[kind] = (now + FLAG_SECS, f"{old:.1f} BTC at ${wp:,.0f} "
                                        + ("was traded away" if kind == "eaten" else "vanished without trading"))
    for k, (until, text) in list(RECENT.items()):
        if now < until:
            flags[k] = text
        else:
            del RECENT[k]
    return f, flags, stops


# ----------------------------------------------------------- projection

def curves(book):
    """Each side of the book as [(dollars from the price, BTC resting within that)], nearest first."""
    prof, mid = book["profile"], book["mid"]
    cells = [(prof["lo"] + (i + 0.5) * BIN, v) for i, v in enumerate(prof["n"])]
    bids, asks, cb, ca = [(0.0, 0.0)], [(0.0, 0.0)], 0.0, 0.0
    for c, v in cells:
        if c > mid:
            ca += v
            asks.append((c - mid + BIN / 2, ca))
    for c, v in reversed(cells):
        if c <= mid:
            cb += v
            bids.append((mid - c + BIN / 2, cb))
    return bids, asks


def within(curve, x):
    """BTC resting within x dollars. Past the recorded span it carries on at the average thickness."""
    xl, yl = curve[-1]
    if x >= xl:
        return yl + (x - xl) * yl / xl if xl else 0.0
    i = bisect.bisect_left(curve, (x, -1.0))
    (x0, y0), (x1, y1) = curve[i - 1], curve[i]
    return y0 + (y1 - y0) * (x - x0) / (x1 - x0)


def reach(curve, btc):
    """How many dollars away the price gets if `btc` of resting orders is used up."""
    xl, yl = curve[-1]
    if btc >= yl:
        return xl + (btc - yl) * xl / yl if yl else xl
    for (x0, y0), (x1, y1) in zip(curve, curve[1:]):
        if y1 >= btc:
            return x0 + (x1 - x0) * (btc - y0) / (y1 - y0) if y1 > y0 else x0
    return xl


def projection(book, d, now):
    """The main page's likely range, bent by the resting orders.

    The main page's range says how far the price typically gets by each moment. Here that
    distance is turned into an amount of BTC (how much rests within it, averaged over both
    sides), and each side then gets as far as that same amount of BTC carries it. So the
    range keeps its overall size but stops short where orders are thick and runs further
    where they are thin. The same idea gives an order-adjusted chance of UP. Experimental:
    resting orders can be pulled, and this is scored live before anything relies on it."""
    P, S, left = d["price"], d["start_price"], d["end"] - d["now"]
    sg = (d.get("sigma") or 0) / (d.get("k_now") or 1)
    if not book or not book.get("profile") or now - book["t"] > 30 or sg <= 0 or left <= 0:
        return None
    bids, asks = curves(book)
    spread = sg * math.sqrt(left / 60)
    mu = (d.get("tilt") or 0.0) * spread
    avg = lambda x: (within(bids, x) + within(asks, x)) / 2
    out = {"t": d["now"], "s": [], "up90": [], "dn90": [], "up50": [], "dn50": []}
    for i in range(25):
        sec = left * i / 24
        mid = P * math.exp(mu * sec / left)
        out["s"].append(round(sec, 1))
        for z, tag in ((1.645, "90"), (0.674, "50")):
            e = avg(P * (math.exp(z * sg * math.sqrt(sec / 60)) - 1))
            out["up" + tag].append(round(mid + reach(asks, e), 2))
            out["dn" + tag].append(round(mid - reach(bids, e), 2))
    out["cone"] = [P * math.exp(mu - 1.645 * spread), P * math.exp(mu + 1.645 * spread)]
    out["book"] = [out["dn90"][-1], out["up90"][-1]]
    side, gap = (P > S) - (P < S), abs(P - S)
    e = within(bids if side > 0 else asks, gap)        # what has to be used up to get back to the start
    lo, hi = 0.0, 8.0 * SPAN
    for _ in range(40):                                # the distance that much BTC is usually worth
        m = (lo + hi) / 2
        lo, hi = (m, hi) if avg(m) < e else (lo, m)
    out["p_book"] = live.phi((side * math.log(1 + lo / P) + mu) / spread)
    return out


RATE_MIDS = (0.55, 0.65, 0.75, 0.85, 0.95)
RATE_NAMES = ("50–60%", "60–70%", "70–80%", "80–90%", "90%+")


def rating(d, left):
    """The main page's confidence rating: how readings like this one (same minute, same stated %, same swing
    level) turned out on days the model was not tuned on. Same rule as the Confidence card in btc_15.py."""
    lr, p = d.get("learn") or {}, d["p_up"]
    conf = max(p, 1 - p)
    m, bk = max(1, min(14, int((BAR - left) // 60))), min(int((conf - 0.5) * 10), 4)
    row = (lr.get("grid") or {}).get(str(m))
    if not row:
        return {"name": "Not measured yet", "bars": 0}
    hit, n = row[bk]
    out = {"min": m, "says": RATE_NAMES[bk], "n": n, "right": hit / n if n else None}
    if n < 30:
        return {**out, "name": "Unproven", "bars": 1}
    steady = lambda h, c: (h + 20 * RATE_MIDS[bk]) / (c + 20)      # leans on the stated % when cases are few
    sw_r = d.get("swing")
    sk = None if sw_r is None else "quiet" if sw_r < 0.7 else "normal" if sw_r <= 1.3 else "choppy"
    sw = (lr.get("swing") or {}).get(sk)
    sw = sw[bk] if sw and sw[bk][1] >= 30 else None
    r = min(steady(hit, n), steady(*sw) if sw else 1)
    name, bars = (("Coin flip", 1) if r < 0.57 else ("Slight", 2) if r < 0.67 else ("Moderate", 3) if r < 0.77
                  else ("Strong", 4) if r < 0.88 else ("Very strong", 5))
    if sw:
        out.update(swing=sk, swing_right=sw[0] / sw[1])
    out["over"] = RATE_MIDS[bk] - min(hit / n, sw[0] / sw[1] if sw else 1) > 0.07
    return {**out, "name": name, "bars": bars}


def light_state(hold, held, stops, flags, lines, proven):
    """(colour, reason). `flags` are the ones on now; only the proven ones may change the colour."""
    active = [k for k in flags if k in proven]
    if stops:
        return "red", stops[0]
    if hold is None:
        return "red", "No reading from the live predictor yet"
    for k in active:
        if FLAGS[k][1] == "red":
            return "red", f"{FLAGS[k][0]}: {flags[k]}"
    if hold < lines["red"]:
        return "red", f"Hold chance {hold * 100:.0f}% is under the red line ({lines['red'] * 100:.0f}%)"
    if active:
        return "yellow", f"{FLAGS[active[0]][0]}: {flags[active[0]]}"
    if hold < lines["green"]:
        return "yellow", f"Hold chance {hold * 100:.0f}% is under the green line ({lines['green'] * 100:.0f}%)"
    if held < HOLD_SECS:
        return "yellow", f"Over the green line for {held:.0f} s; needs {HOLD_SECS} s"
    return "green", f"Hold chance {hold * 100:.0f}% has stayed over the green line ({lines['green'] * 100:.0f}%)"


# ------------------------------------------------------------- training

def fit_lines(rows):
    """Green and red lines from (hold chance, was right) pairs. Each line is set by how calls
    AT that level turned out (a 5-point band), so it corrects for percentages that run hot or cold."""
    rows = sorted(rows)
    confs, pre = [c for c, _ in rows], [0]
    for _, hit in rows:
        pre.append(pre[-1] + hit)

    def band(a):                           # share right among hold chances in [a, a + 5 points), if there are enough
        i, j = bisect.bisect_left(confs, a / 100), bisect.bisect_left(confs, (a + 5) / 100)
        return (pre[j] - pre[i]) / (j - i) if j - i >= MIN_N else None

    green = 90
    for g in range(60, 94):
        here, above = band(g), band(g + 5)
        if here is not None and here >= GREEN_TARGET and (above is None or above >= GREEN_TARGET):
            green = g
            break
    red = 55
    for r in range(55, green - 4):
        here = band(r - 5)
        if here is not None and here <= RED_TARGET:
            red = r
    return {"green": green / 100, "red": red / 100}


def record(rows, lines):
    """How (hold chance, was right) pairs turned out under each colour's line."""
    out = {}
    for color, sel in (("green", lambda c: c >= lines["green"]), ("yellow", lambda c: lines["red"] <= c < lines["green"]),
                       ("red", lambda c: c < lines["red"])):
        got = [(c, h) for c, h in rows if sel(c)]
        out[color] = {"n": len(got), "share": len(got) / len(rows) if rows else 0,
                      "stated": sum(c for c, _ in got) / len(got) if got else None,
                      "right": sum(h for _, h in got) / len(got) if got else None}
    return out


def depth_minutes(db):
    return db.execute("SELECT COUNT(DISTINCT CAST(t / 60 AS INTEGER)) FROM depth").fetchone()[0]


def train(args, db):
    con = read_live(args.live_db)
    candles = {t: (o, c) for t, o, c in con.execute("SELECT t, o, c FROM candles WHERE t >= ?",
                                                    (time.time() - args.days * 86400,))}
    con.close()
    if len(candles) < 3 * 1440:
        raise RuntimeError("not enough candle history yet")
    champ = lab.champion(args.live_db)
    samples = live.build_samples(candles)
    cut = samples[int(len(samples) * (1 - live.HOLDOUT))][0]
    tr, ho = [], []
    for s in samples:
        if s[1] * 60 < WARMUP:             # nothing is shown this early, so it is not fitted or checked either
            continue
        p = live.phi(live.sample_score(champ, s))
        (tr if s[0] < cut else ho).append((max(p, 1 - p), (p > 0.5) == s[3]))
    lines = fit_lines(tr)
    rep = {"at": time.time(), "champion": live.describe(champ), "lines": lines, "lines_from": "history",
           "train": record(tr, lines), "hold": record(ho, lines),
           "train_days": (cut - samples[0][0]) / 86400, "hold_days": (samples[-1][0] + BAR - cut) / 86400,
           "beta": {}, "model": {"status": "collecting"}}

    days = depth_minutes(db) / 1440
    rep["depth_days"] = days
    if days >= MIN_DEPTH_DAYS:
        rows, seen = [], set()
        for t, start, left, p_live, feats, up in db.execute(
                "SELECT t, start, left, p_live, feats, up FROM light WHERE up IS NOT NULL AND p_live IS NOT NULL AND left <= ? ORDER BY t",
                (BAR - WARMUP,)):
            m = int((BAR - left) // 60) + 1
            f = json.loads(feats)
            if (start, m) in seen or "book" not in f:      # one reading a minute, and only ones that saw the book
                continue
            seen.add((start, m))
            rows.append((1 if up else -1, ND.inv_cdf(min(max(p_live, 1e-6), 1 - 1e-6)), {k: f.get(k, 0.0) for k in MODEL_X}, m))
        k = int(len(rows) * (1 - live.HOLDOUT))
        tr2, ho2 = rows[:k], rows[k:]
        if len(ho2) >= 1000:
            base = lab.evaluate(ho2, {})
            _, ts = lab.fit(tr2, list(MODEL_X))
            keep = [n for n in MODEL_X if abs(ts[n]) >= live.KEEP_T]
            beta, kept_t, hold = ({}, {}, base)
            if keep:
                beta, kept_t = lab.fit(tr2, keep)
                hold = lab.evaluate(ho2, beta)
            d_ll = base["ll"] - hold["ll"]
            adopted = bool(keep) and d_ll >= live.MARGIN
            rep["model"] = {"status": "in use" if adopted else "tested, not better", "t": ts, "kept": keep,
                            "d_ll": d_ll, "base": base, "hold": hold, "n_train": len(tr2), "n_hold": len(ho2)}
            if adopted:                    # the hold chance changes, so the lines are refitted on the recorded readings
                def pairs(rs):
                    out = []
                    for q, off, f, _ in rs:
                        p = live.phi(lab.score_of(off, f, beta))
                        out.append((max(p, 1 - p), (p > 0.5) == (q > 0)))
                    return out
                lines = fit_lines(pairs(tr2))
                rep.update(beta=beta, lines=lines, lines_from="recorded readings",
                           train=record(pairs(tr2), lines), hold=record(pairs(ho2), lines))
                rep["model"]["kept_t"] = kept_t
    return rep


def evidence(db):
    """For each flag: how calls made while it was on turned out, counted per interval."""
    per = {k: {} for k in FLAGS}
    for start, hold, p, flags, up in db.execute(
            "SELECT start, hold, p, flags, up FROM light WHERE up IS NOT NULL AND flags != '[]' AND t > ? AND left <= ?",
            (time.time() - 60 * 86400, BAR - WARMUP)):
        for k in json.loads(flags):
            if k in per:
                x = per[k].setdefault(start, [0.0, 0, 0])
                x[0] += hold
                x[1] += (p > 0.5) == bool(up)
                x[2] += 1
    out = {}
    for k, blocks in per.items():
        gaps = [(st - hit) / n for st, hit, n in blocks.values()]
        e = {"blocks": len(blocks), "readings": sum(n for *_, n in blocks.values()), "proven": False}
        if blocks:
            e["stated"] = sum(st / n for st, _, n in blocks.values()) / len(blocks)
            e["right"] = sum(hit / n for _, hit, n in blocks.values()) / len(blocks)
            e["gap"] = e["stated"] - e["right"]
            sd = statistics.pstdev(gaps) if len(gaps) > 1 else 0.0
            e["t"] = e["gap"] / (sd / math.sqrt(len(gaps))) if sd > 0 else 0.0
            e["proven"] = e["blocks"] >= PROVE_BLOCKS and e["gap"] >= PROVE_GAP and e["t"] >= PROVE_T
        out[k] = e
    return out


def board(db):
    """Live record: how each colour's readings turned out, the calls they sat beside, and how
    often the final price landed inside each 9-in-10 range."""
    cols = {c: [0, set(), 0.0, 0] for c in ("green", "yellow", "red")}
    calls = {k: [0, 0.0, 0, 0.0, 0.0] for k in ("live", "test", "book")}     # n, said, right, log loss, main's log loss
    cover = {"cone": [0, 0], "book": [0, 0]}
    total, wrong_green, green_blocks = 0, set(), set()
    ll = lambda q, up: -math.log(min(max(q if up else 1 - q, 1e-6), 1.0))
    for start, color, hold, p, p_live, p_test, p_book, c_lo, c_hi, b_lo, b_hi, close, up in db.execute(
            "SELECT start, color, hold, p, p_live, p_test, p_book, cone_lo, cone_hi, book_lo, book_hi, close, up "
"FROM light WHERE up IS NOT NULL AND t > ? AND left <= ?", (time.time() - 30 * 86400, BAR - WARMUP)):
        total += 1
        hit = (p > 0.5) == bool(up)
        x = cols[color]
        x[0] += 1
        x[1].add(start)
        x[2] += hold
        x[3] += hit
        if color == "green":
            green_blocks.add(start)
            if not hit:
                wrong_green.add(start)
        for name, q in (("live", p_live), ("test", p_test), ("book", p_book)):
            if q is not None and p_live is not None:
                y = calls[name]
                y[0] += 1
                y[1] += max(q, 1 - q)
                y[2] += (q > 0.5) == bool(up)
                y[3] += ll(q, up)
                y[4] += ll(p_live, up)
        if close is not None and b_lo is not None:     # both ranges, on the same readings
            for name, (a, b) in (("cone", (c_lo, c_hi)), ("book", (b_lo, b_hi))):
                cover[name][0] += a <= close <= b
                cover[name][1] += 1
    out = {"total": total, "colors": {}, "calls": {}, "green_blocks": len(green_blocks), "green_wrong": len(wrong_green),
           "cover": {k: {"n": n, "inside": hit / n if n else None} for k, (hit, n) in cover.items()}}
    for c, (n, blocks, st, hit) in cols.items():
        out["colors"][c] = {"n": n, "blocks": len(blocks), "share": n / total if total else 0,
                            "stated": st / n if n else None, "right": hit / n if n else None}
    for name, (n, st, hit, l, base) in calls.items():
        out["calls"][name] = {"n": n, "stated": st / n if n else None, "right": hit / n if n else None,
                              "ll": l / n if n else None, "d_ll": (base - l) / n if n else None}
    return out


def score_finished(db, live_path):
    """Mark finished intervals' readings as right or wrong, the way the live predictor scored
    them (the contract's official result once it has posted). Returns how many were scored."""
    now, done = time.time(), 0
    todo = [s for (s,) in db.execute("SELECT DISTINCT start FROM light WHERE up IS NULL") if s + BAR + 90 < now]
    if not todo:
        return 0
    con = read_live(live_path)
    for start in todo:
        got = con.execute("SELECT up, price FROM readings WHERE start=? AND up IS NOT NULL ORDER BY t DESC LIMIT 1",
                          (start,)).fetchone()
        if not got and now > start + BAR + 600:            # the live predictor never scored it: use the candles
            a = con.execute("SELECT o FROM candles WHERE t=?", (start,)).fetchone()
            b = con.execute("SELECT c FROM candles WHERE t=?", (start + BAR - 60,)).fetchone()
            got = (int(b[0] > a[0]), b[0]) if a and b else None
        if got:
            db.execute("UPDATE light SET up=?, close=? WHERE start=?", (got[0], got[1], start))
            done += 1
    con.close()
    db.commit()
    return done


def status(db):
    mins = depth_minutes(db)
    n, blocks = db.execute("SELECT COUNT(*), COUNT(DISTINCT start) FROM light WHERE up IS NOT NULL AND left <= ?",
                           (BAR - WARMUP,)).fetchone()
    return {"depth_rows": db.execute("SELECT COUNT(*) FROM depth").fetchone()[0], "depth_days": mins / 1440,
            "depth_need": MIN_DEPTH_DAYS, "depth_error": DEPTH_ERR[0], "readings": n, "blocks": blocks}


# --------------------------------------------------------- shared state

STATE = {"now": None, "board": None, "evidence": {}, "version": None, "versions": [], "status": {},
         "busy": False, "error": None, "train_error": None}
LOCK = threading.Lock()
RETRAIN = threading.Event()
DEFAULT_LINES = {"green": 0.90, "red": 0.60}


LAST = {}                                  # the live page's latest reading, kept for the chart


def chart_data(path):
    """What the chart draws: the price path, the book over this interval, and the light's colours."""
    L = dict(LAST)
    if not L:
        return {}
    books = [b for b in list(DEPTH) if b.get("profile")]
    sh = L.get("basis") or 0.0             # book prices are the exchange's; shift them like the live page's prices
    L["heat"] = [[round(b["t"], 1), round(b["mid"] + sh, 2), round(b["profile"]["lo"] + sh, 2),
                  [int(round(x * 10)) for x in b["profile"]["n"]]] for b in books if b["t"] >= L["start"] - 10]
    L["walls"] = {k: [[p + sh, z] for p, z in books[-1][k + "walls"]] for k in "ba"} if books else None
    L["bin"], L["warmup"] = BIN, WARMUP
    try:
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
        L["lights"] = db.execute("SELECT t, color FROM light WHERE start=? ORDER BY t", (L["start"],)).fetchall()
        db.close()
    except sqlite3.Error:
        L["lights"] = []
    return L


def publish(**kw):
    global STATE
    with LOCK:
        STATE = {**STATE, **kw}


def load_versions(db):
    rows = db.execute("SELECT id, report FROM versions ORDER BY id DESC LIMIT 12").fetchall()
    vs = [{**json.loads(r), "id": i} for i, r in rows]
    publish(version=vs[0] if vs else None, versions=[
        {"id": v["id"], "at": v["at"], "lines": v["lines"], "lines_from": v.get("lines_from"),
         "green": v["hold"]["green"], "red": v["hold"]["red"], "model": v["model"]["status"],
         "kept": v["model"].get("kept", []), "proven": v.get("proven", [])} for v in vs])
    return vs[0] if vs else None


def trainer(args):
    db = open_db(args.db)
    last = load_versions(db)
    if last and time.time() - last["at"] < args.retrain_hours * 3600:
        RETRAIN.wait(args.retrain_hours * 3600 - (time.time() - last["at"]))
        RETRAIN.clear()
    while True:
        publish(busy=True)
        try:
            print("Training the light...", flush=True)
            rep = train(args, db)
            ev = evidence(db)
            rep["proven"] = [k for k, e in ev.items() if e["proven"]]
            db.execute("INSERT INTO versions (at, report) VALUES (?, ?)", (rep["at"], json.dumps(rep)))
            db.commit()
            load_versions(db)
            publish(train_error=None, evidence=ev)
            g = rep["hold"]["green"]
            print(f"Light trained: green >= {rep['lines']['green']:.0%}, red < {rep['lines']['red']:.0%}; on unseen days green was "
                  f"right {g['right']:.1%} of {g['n']:,} readings; book signals: {rep['model']['status']}", flush=True)
        except Exception as e:
            publish(train_error=str(e)[:300])
            print(f"Light training failed: {e}", flush=True)
        publish(busy=False)
        RETRAIN.wait(args.retrain_hours * 3600)
        RETRAIN.clear()


def lab_test(s, args):
    """The lab's test-model probability for the current interval, if the lab is up and has one."""
    try:
        now = s.get(args.lab_url.rstrip("/") + "/testing/data", timeout=2).json().get("now") or {}
        return now.get("start"), (now.get("probs") or {}).get("test")
    except (requests.RequestException, ValueError):
        return None, None


def watch(args):
    db, s = open_db(args.db), requests.Session()
    fl, test, slow_at, rec_at, board_at, held = {}, (None, None), 0.0, 0.0, 0.0, (None, None)
    while True:
        tick = time.time()
        try:
            if tick - slow_at >= 10:
                slow_at = tick
                fl, test = flow_window(args.live_db, tick), lab_test(s, args)
                if score_finished(db, args.live_db) or tick - board_at > 300:
                    board_at = tick
                    publish(board=board(db), evidence=evidence(db))
                publish(status=status(db))
            ver = STATE["version"]
            lines = ver["lines"] if ver else DEFAULT_LINES
            proven = {k for k, e in STATE["evidence"].items() if e.get("proven")}
            try:
                d = s.get(args.live_url.rstrip("/") + "/data", timeout=3).json()
            except (requests.RequestException, ValueError):
                d = None
            if not d or d.get("p_up") is None or not d.get("start_price"):
                why = "The live predictor is not reachable" if not d else "No reading from the live predictor yet"
                publish(error=None, now={"t": tick, "color": "red", "reason": why, "lines": lines, "flags": {}, "active": [], "feats": {}})
            else:
                f, flags, stops = signals(d, fl, tick)
                path, last = [], 0.0
                for t, px in d.get("ticks") or []:
                    if t >= last + 2:
                        last = t
                        path.append([round(t, 1), px])
                LAST.clear()
                LAST.update(start=d["start"], end=d["end"], start_price=d["start_price"], price=d["price"], now=d["now"],
                            sigma=d.get("sigma"), k_now=d.get("k_now"), tilt=d.get("tilt") or 0.0, path=path,
                            basis=d.get("basis") or 0.0, start_source=d.get("start_source"))
                try:
                    pj = projection(DEPTH[-1] if DEPTH else None, d, tick)
                except (ValueError, ZeroDivisionError, IndexError):
                    pj = None
                LAST["proj"] = pj
                if not d.get("exact"):
                    stops.insert(0, "The live predictor does not have this interval's exact start price")
                if d.get("error") or tick - d["now"] > 15:
                    stops.insert(0, "The live price feed is stale")
                p_live = d["p_up"]
                p = p_live
                if ver and ver.get("beta"):
                    off = ND.inv_cdf(min(max(p_live, 1e-6), 1 - 1e-6))
                    p = live.phi(lab.score_of(off, f, ver["beta"]))
                hold, key = max(p, 1 - p), (d["start"], p > 0.5)
                if hold < lines["green"]:
                    held = (None, None)
                elif held[0] != key:
                    held = (key, tick)
                held_secs = tick - held[1] if held[1] else 0.0
                color, reason = light_state(hold, held_secs, stops, flags, lines, proven)
                left = d["end"] - d["now"]
                wait = max(0.0, left - (BAR - WARMUP))
                if wait > 0:
                    color, reason = "wait", "Too early to call: the first 5 minutes of an interval can still change too much"
                p_test = test[1] if test[0] == d["start"] else None
                if left > 0 and tick - rec_at >= 10:
                    db.execute("INSERT INTO light (t, start, left, color, hold, p, p_live, p_test, flags, feats, version, "
                               "p_book, cone_lo, cone_hi, book_lo, book_hi) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                               (d["now"], d["start"], left, color, hold, p, p_live, p_test, json.dumps(list(flags)),
                                json.dumps(f), ver["id"] if ver else None,
                                *((pj["p_book"], *pj["cone"], *pj["book"]) if pj else (None,) * 5)))
                    db.commit()
                    rec_at = tick
                publish(error=None, now={
                    "t": d["now"], "start": d["start"], "end": d["end"], "price": d["price"], "start_price": d["start_price"],
                    "p_live": p_live, "p_test": p_test, "p_book": pj and pj["p_book"], "p": p, "hold": hold,
                    "held": held_secs, "color": color, "wait": wait, "ranges": pj and {"cone": pj["cone"], "book": pj["book"]},
                    "reason": reason, "lines": lines, "flags": flags, "active": [k for k in flags if k in proven],
                    "stops": stops, "feats": f, "rating": None if wait > 0 else rating(d, left)})
        except Exception as e:
            publish(error=str(e)[:300])
        time.sleep(max(0.2, args.every - (time.time() - tick)))


# ---------------------------------------------------------------- page

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

    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        if path in ("", "/index.html"):
            self.send(302, b"", "text/plain", [("Location", "/testing-2")])
        elif path == "/testing-2":
            self.send(200, PAGE.encode(), "text/html; charset=utf-8")
        elif path == "/testing-2/data":
            with LOCK:
                body = json.dumps({**STATE, "flag_info": FLAGS, "live_port": ARGS.live_url.rsplit(":", 1)[-1].strip("/"),
                                   "light_port": ARGS.port, "hold_secs": HOLD_SECS,
                                   "prove": [PROVE_BLOCKS, PROVE_GAP, PROVE_T], "dists": DISTS}).encode()
            self.send(200, body, "application/json")
        elif path == "/testing-2/chart":
            self.send(200, json.dumps(chart_data(ARGS.db)).encode(), "application/json")
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path != "/testing-2/retrain":
            self.send_error(404)
            return
        RETRAIN.set()
        self.send(200, b"{}", "application/json")

    def log_message(self, *a):
        pass


ARGS = None


def main():
    global ARGS
    ap = argparse.ArgumentParser(description="Red / yellow / green light for the live BTC 15-minute call.")
    ap.add_argument("--db", default=os.path.join(HERE, "btc_light.db"), help="this script's database (default btc_light.db)")
    ap.add_argument("--live-db", default=os.path.join(HERE, "btc_15m_coinbase.db"), help="live database, read only")
    ap.add_argument("--live-url", default="http://127.0.0.1:8000", help="live predictor's page")
    ap.add_argument("--lab-url", default="http://127.0.0.1:8001", help="testing lab's page, for its test model's call")
    ap.add_argument("--port", type=int, default=8002, help="port for the /testing-2 page (default 8002)")
    ap.add_argument("--every", type=float, default=2.0, help="seconds between readings (default 2)")
    ap.add_argument("--depth-every", type=float, default=10.0, help="seconds between order-book snapshots (default 10)")
    ap.add_argument("--days", type=int, default=30, help="days of history to fit the lines on (default 30)")
    ap.add_argument("--retrain-hours", type=float, default=4, help="hours between training runs (default 4)")
    ARGS = args = ap.parse_args()

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"Light page: http://localhost:{args.port}/testing-2", flush=True)
    db = open_db(args.db)
    load_depth(db)
    publish(status=status(db), board=board(db), evidence=evidence(db))
    db.close()
    threading.Thread(target=depth_recorder, args=(args,), daemon=True).start()
    threading.Thread(target=trainer, args=(args,), daemon=True).start()
    try:
        watch(args)
    except KeyboardInterrupt:
        print("\nStopped.")


PAGE_CSS = lab.PAGE.split("<style>")[1].split("</style>")[0] + """
  /* Top of the page follows the main page's layout, so it can move there as it is. */
  [hidden] { display: none !important; }
  .tag.l { background: rgba(46,189,133,.14); color: var(--up); }
  .brand a { font-size: 13px; }
  .clock { display: flex; align-items: baseline; gap: 14px; }
  .clock .left { font-size: 22px; font-weight: 700; color: var(--text); }
  .clock .left small { font-family: var(--sans); font-size: 12px; font-weight: 400; color: var(--muted); margin-left: 6px; }
  .pad { padding: 16px; }
  .hero { display: grid; gap: 16px; grid-template-columns: minmax(250px, 1fr) minmax(280px, 1.25fr) minmax(280px, 1.1fr); }
  @media (max-width: 980px) { .hero { grid-template-columns: 1fr; } }
  .dir { font-size: 13px; font-weight: 600; letter-spacing: .1em; text-transform: uppercase; color: var(--muted); }
  .big { font-family: var(--mono); font-weight: 700; font-size: 68px; line-height: 1; letter-spacing: -.04em; margin: 6px 0 2px; }
  .big sup { font-size: 28px; font-weight: 500; vertical-align: top; position: relative; top: 7px; margin-left: 2px; letter-spacing: 0; }
  .big.off { color: var(--faint); }
  .bar { display: flex; height: 8px; margin: 12px 0 6px; background: var(--down); border-radius: 4px; overflow: hidden; }
  .bar i { display: block; background: var(--up); transition: width .4s ease; border-right: 2px solid var(--card); }
  .bar.off { background: #23272f; } .bar.off i { background: #23272f; }
  .barlabels { display: flex; justify-content: space-between; font-family: var(--mono); font-size: 12px; }
  .conf { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 12px; }
  .conf > div + div { border-left: 1px solid var(--line); padding-left: 16px; }
  .lbl { color: var(--faint); font-size: 11px; font-weight: 600; letter-spacing: .1em; text-transform: uppercase; margin-bottom: 6px; }
  .meter { display: flex; gap: 4px; margin-top: 10px; }
  .meter i { flex: 1; height: 8px; border-radius: 2px; background: #23272f; }
  .meter i.on { background: var(--text); }
  .note .warn { color: var(--down); }
  .lightrow { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  .lamps { display: flex; gap: 6px; padding: 6px 8px; background: #07080a; border: 1px solid var(--line); border-radius: 20px; }
  .lamp { width: 18px; height: 18px; border-radius: 50%; background: #1a1d22; }
  .lamp.on.red { background: var(--down); box-shadow: 0 0 14px rgba(246,70,93,.6); }
  .lamp.on.yellow { background: var(--warn); box-shadow: 0 0 14px rgba(232,184,74,.6); }
  .lamp.on.green { background: var(--up); box-shadow: 0 0 14px rgba(46,189,133,.6); }
  .rating { font-size: 30px; font-weight: 800; line-height: 1.1; }
  .note { margin: 8px 0 0; color: var(--muted); font-size: 13px; }
  .note b { color: var(--text); font-family: var(--mono); font-weight: 500; }
  .note:empty { display: none; }
  .c-green { color: var(--up); } .c-yellow { color: var(--warn); } .c-red { color: var(--down); } .c-wait { color: var(--muted); }
  .pill.use { background: rgba(232,184,74,.14); color: var(--warn); }
  .swatch { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; }
  .kv dt small { display: block; color: var(--faint); font-size: 12px; }

  .card > h2 .hov { text-transform: none; letter-spacing: 0; font-weight: 400; font-family: var(--mono); color: var(--text); text-align: right; }
  .stats { display: flex; flex-wrap: wrap; border-bottom: 1px solid var(--line); }
  .stats div { flex: 1 1 150px; padding: 10px 16px; border-right: 1px solid var(--line); }
  .stats div:last-child { border-right: 0; }
  .stats span { display: block; color: var(--muted); font-size: 12px; }
  .stats b { font-family: var(--mono); font-weight: 500; font-size: 15px; }
  .stats small { font-family: var(--mono); color: var(--faint); font-size: 12px; margin-left: 4px; }
  #cv { display: block; width: 100%; height: clamp(320px, 50vh, 560px); cursor: crosshair; }
  .toggles { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 18px; padding: 8px 16px 10px; border-top: 1px solid var(--line); }
  .toggles .grp { color: var(--faint); font-size: 11px; font-weight: 600; letter-spacing: .1em; text-transform: uppercase; }
  .toggles .gap { flex-basis: 100%; height: 0; }
  .tg { display: inline-flex; align-items: center; gap: 8px; padding: 3px 0; background: none; border: 0; border-radius: 3px;
        color: var(--muted); font-weight: 400; font-size: 12px; letter-spacing: 0; }
  .tg:hover:not(:disabled) { color: var(--text); background: none; }
  .tg i { display: inline-block; width: 18px; height: 8px; border-radius: 2px; }
  .tg[aria-pressed="false"] { color: var(--faint); text-decoration: line-through; }
  .tg[aria-pressed="false"] i { opacity: .2; }
  .tg.all { margin-left: auto; color: var(--faint); }

  .lower { display: grid; gap: 16px; grid-template-columns: minmax(0, 1.25fr) minmax(0, 1fr); align-items: start; }
  @media (max-width: 980px) { .lower { grid-template-columns: 1fr; } }
  .on-flag { color: var(--warn); }
  .lower th:first-child, .lower td:first-child { min-width: 96px; }
  #flagsmini div { padding: 7px 16px; }
  #flagsmini dd { white-space: nowrap; }
  #flagsmini dd small { display: inline; margin-right: 10px; }
  details.more > summary { cursor: pointer; list-style: none; display: flex; align-items: center; gap: 10px; padding: 12px 16px;
        background: var(--card); border: 1px solid var(--line); border-radius: 8px;
        font-size: 11px; font-weight: 600; letter-spacing: .1em; text-transform: uppercase; color: var(--muted); }
  details.more > summary::-webkit-details-marker { display: none; }
  details.more > summary::before { content: "+"; font-family: var(--mono); font-size: 14px; color: var(--text); width: 10px; }
  details.more[open] > summary::before { content: "–"; }
  details.more > summary small { text-transform: none; letter-spacing: 0; font-weight: 400; font-size: 12px; color: var(--faint); }
  details.more > summary:hover { border-color: #2c323c; color: var(--text); }
  .morebody { display: flex; flex-direction: column; gap: 16px; margin-top: 16px; }
  footer { color: var(--faint); font-size: 12px; }
"""

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 15m Light</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@400;500;600;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>__CSS__</style>
</head>
<body>
<div class="page">
  <header>
    <div class="brand">BTC 15m <span class="tag l">LIGHT</span> <a id="back" href="/">Main page</a> <a id="lab" href="/testing">Testing</a></div>
    <div class="clock"><span id="win">&ndash;</span><span class="left"><span id="count">--:--</span><small>left</small></span></div>
  </header>
  <div class="alert" id="err" hidden></div>

  <!-- 1. the answer: projection, light, orders. This row and the chart are the part meant for the main page. -->
  <div class="hero">
    <section class="card" aria-live="polite">
      <h2>Projection</h2>
      <div class="pad">
        <div class="dir" id="call">Waiting</div>
        <div class="big off" id="pct">--</div>
        <div class="bar off" id="bar" aria-hidden="true"><i id="barfill" style="width:50%"></i></div>
        <div class="barlabels"><span class="up" id="lblup">&#9650; Up</span><span class="down" id="lbldown">Down &#9660;</span></div>
      </div>
    </section>
    <section class="card" aria-live="polite">
      <h2>Confidence</h2>
      <div class="pad">
        <div class="conf">
          <div>
            <div class="lbl">Rating</div>
            <div class="rating" id="rating">&ndash;</div>
            <div class="meter" id="meter" aria-hidden="true"><i></i><i></i><i></i><i></i><i></i></div>
          </div>
          <div>
            <div class="lbl">Light</div>
            <div class="lightrow">
              <div class="lamps" role="img" id="lamps" aria-label="Light">
                <div class="lamp red" id="l_red"></div><div class="lamp yellow" id="l_yellow"></div><div class="lamp green" id="l_green"></div>
              </div>
              <div class="rating" id="word">&ndash;</div>
            </div>
          </div>
        </div>
        <p class="note" id="track"></p>
        <p class="note" id="why">Waiting for data</p>
      </div>
    </section>
    <section class="card">
      <h2>Orders</h2>
      <dl class="kv" id="book"></dl>
    </section>
  </div>

  <!-- 2. the picture -->
  <section class="card">
    <h2>This interval <span class="hov" id="hov"></span></h2>
    <div class="stats">
      <div><span>Start</span><b id="s_start">&ndash;</b></div>
      <div><span>Now</span><b id="s_now">&ndash;</b><small id="s_chg"></small></div>
      <div><span>Likely finish (9 in 10)</span><b id="s_range">&ndash;</b></div>
      <div><span>Same, bent by the orders</span><b id="s_book">&ndash;</b><small>experimental</small></div>
    </div>
    <canvas id="cv" role="img" aria-label="Price this interval with the projected range to the end, drawn over the resting buy and sell orders at each price"></canvas>
    <div class="toggles" id="toggles" aria-label="Show or hide parts of the chart"></div>
  </section>

  <!-- 3. the evidence, short form -->
  <div class="lower">
    <section class="card">
      <h2>Light's record, live</h2>
      <div class="meta" id="bdmeta">Nothing scored yet.</div>
      <div class="scroll"><table id="bd"></table></div>
    </section>
    <section class="card">
      <h2>Warning flags</h2>
      <dl class="kv" id="flagsmini"></dl>
    </section>
  </div>

  <!-- 4. everything else: stays on the testing page -->
  <details class="more" id="more">
    <summary>Testing detail <small>calls compared, flag evidence, order book, training</small></summary>
    <div class="morebody">
      <div class="grid2">
        <section class="card">
          <h2>Calls compared, live</h2>
          <div class="scroll"><table id="cmp"></table></div>
          <div class="foot" id="cmpfoot"></div>
        </section>
        <section class="card">
          <h2>Right now</h2>
          <dl class="kv" id="side"></dl>
        </section>
      </div>
      <section class="card">
        <h2>Flag evidence</h2>
        <div class="scroll"><table id="flags"></table></div>
        <div class="foot" id="flagfoot"></div>
      </section>
      <div class="grid2">
        <section class="card">
          <h2>Training <button id="retrain">Retrain now</button></h2>
          <div class="meta" id="trmeta">Not trained yet.</div>
          <div class="scroll"><table id="tr"></table></div>
          <div class="foot">The lines are fitted on older days and checked on the most recent days the fit never saw.</div>
        </section>
        <section class="card">
          <h2>Order book now</h2>
          <dl class="kv" id="bookfull"></dl>
        </section>
      </div>
      <section class="card">
        <h2>Training runs</h2>
        <div class="scroll"><table id="vs"></table></div>
      </section>
    </div>
  </details>
  <footer>The light says how far to trust the call, not which way the price will go. Green and red bands on the chart are buy and sell orders resting at each price; brighter means more BTC. Nothing is called in the first 5 minutes. Not financial advice.</footer>
</div>
<script>
const $ = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const pct = (x, d = 1) => x == null ? '&ndash;' : (x * 100).toFixed(d) + '%';
const num = (x, d = 1) => x == null ? '&ndash;' : x.toLocaleString(undefined, {minimumFractionDigits: d, maximumFractionDigits: d});
const clock = t => new Date(t * 1000).toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
const ago = t => { const m = (Date.now() / 1000 - t) / 60; return m < 60 ? `${Math.round(m)} min ago` : `${(m / 60).toFixed(1)} h ago`; };
const call = p => p == null ? '&ndash;' : `<span class="${p >= .5 ? 'up' : 'down'}">${p >= .5 ? 'UP ' + pct(p, 0) : 'DOWN ' + pct(1 - p, 0)}</span>`;
const WORD = {green: 'GREEN', yellow: 'YELLOW', red: 'RED', wait: 'WAITING'};
const mmss = s => Math.floor(s / 60) + ':' + String(Math.floor(s % 60)).padStart(2, '0');
const dot = c => `<span class="swatch" style="background:var(--${c === 'green' ? 'up' : c === 'red' ? 'down' : 'warn'})"></span>`;
const kv = rows => rows.map(([a, b, c]) => `<div><dt>${a}</dt><dd>${b}${c ? `<small>${c}</small>` : ''}</dd></div>`).join('');
let D = null;

function render() {
  if (!D) return;
  const own = location.port === String(D.light_port);
  $('back').href = own ? `${location.protocol}//${location.hostname}:${D.live_port}/` : '/';
  $('lab').href = own ? `${location.protocol}//${location.hostname}:${D.live_port}/testing` : '/testing';
  const err = D.error || D.train_error;
  $('err').hidden = !err; $('err').textContent = err ? (D.error ? 'Light: ' : 'Training: ') + err : '';
  const N = D.now, FI = D.flag_info, E = D.evidence || {}, B = D.board, V = D.version, S = D.status || {};
  const name = c => WORD[c][0] + WORD[c].slice(1).toLowerCase();

  if (N) {
    const early = N.color === 'wait', hide = v => early ? '&ndash;' : v, L = N.lines, p = N.p_live;
    if (N.end) $('win').textContent = `${clock(N.start)} – ${clock(N.end)}`;

    // Projection, as on the main page
    const show = !early && p != null, up = p >= .5;
    $('call').innerHTML = show ? (up ? '&#9650; Up' : '&#9660; Down') : early ? 'Too early to call' : 'Waiting';
    $('call').className = 'dir' + (show ? (up ? ' up' : ' down') : '');
    $('pct').innerHTML = show ? Math.round(Math.max(p, 1 - p) * 100) + '<sup>%</sup>' : '--';
    $('pct').className = 'big ' + (show ? (up ? 'up' : 'down') : 'off');
    $('bar').classList.toggle('off', !show); $('barfill').style.width = (show ? p * 100 : 50) + '%';
    $('lblup').innerHTML = '&#9650; Up' + (show ? ' ' + pct(p, 0) : ''); $('lbldown').innerHTML = 'Down' + (show ? ' ' + pct(1 - p, 0) : '') + ' &#9660;';

    // Light
    for (const c of ['red', 'yellow', 'green']) $('l_' + c).classList.toggle('on', N.color === c);
    $('lamps').setAttribute('aria-label', 'Light: ' + WORD[N.color]);
    $('word').textContent = early ? 'Waiting' : name(N.color); $('word').className = 'rating c-' + N.color;
    const g = B?.colors?.green, R = N.rating;
    $('why').innerHTML = `<b>Light:</b> ${esc(N.reason)}${early ? `. Calls start in ${mmss(N.wait)}.` : '.'} ` +
      (g?.n ? `Green has been right <b>${pct(g.right, 0)}</b> of the time over ${g.blocks} intervals.`
        : V ? `On unseen history, green was right <b>${pct(V.hold.green.right, 0)}</b> of the time.` : '');

    // Rating, by the main page's rule
    $('rating').textContent = early ? 'Too early' : R ? R.name : '–';
    [...$('meter').children].forEach((e, i) => e.classList.toggle('on', !early && R && i < R.bars));
    $('track').innerHTML = early || !R ? '' : R.n == null ? 'The main page has not measured its track record yet.'
      : R.n < 30 ? `Only ${R.n} past case${R.n === 1 ? '' : 's'} like this (${R.min} min in, saying ${R.says}).`
      : `Readings like this (${R.min} min in, saying ${R.says}) were right <b>${pct(R.right, 0)}</b> of the time in ${R.n.toLocaleString()} past cases` +
        (R.swing ? `; with ${R.swing} swings like now, <b>${pct(R.swing_right, 0)}</b>` : '') + '.' + (R.over ? ' <span class="warn">It has been overconfident here.</span>' : '');

    // Orders
    const F = N.feats || {}, bk = F.book, lead = N.price >= N.start_price ? 'buy' : 'sell';
    $('book').innerHTML = bk ? kv([
      ['Between price and start', F.cover == null ? '&ndash;' : `${num(F.cover)} BTC`,
        F.cover != null && F.pace ? `${lead} orders, about ${num(F.cover / F.pace, 1)} min of trading` : `${lead} orders protecting the lead`],
      ['Largest order in the way', F.wall == null ? 'none' : `${num(F.wall)} BTC`, F.wall == null ? '' : `$${num(F.walldist, 0)} away, ${num(F.wallx, 1)}x a usual large one`],
      ['Trades against the lead', F.sweepx == null ? '&ndash;' : `${num(F.sweepx, 1)}x usual`, 'last 30 seconds'],
      ['Call with the orders', hide(call(N.p_book)), 'experimental'],
    ]) : kv([['Order book', 'waiting', esc(S.depth_error || 'first snapshot not in yet')]]);

    // Detail: every call side by side, and the whole book
    $('side').innerHTML = kv([
      ['Main page', hide(call(p)), ''],
      ['Testing page (test model)', hide(call(N.p_test)), early ? '' : N.p_test == null ? 'no test model right now, so it equals the main page' : ''],
      ['With the orders (experimental)', hide(call(N.p_book)), early ? '' : N.p_book == null ? 'waiting for the order book' : 'main page&rsquo;s call, adjusted for what is resting between price and start'],
      ['Light&rsquo;s hold chance', N.hold == null ? '&ndash;' : hide(pct(N.hold, 0)),
        early ? 'hidden for the first 5 minutes' : V && Object.keys(V.beta || {}).length ? 'main page&rsquo;s call adjusted by the book signals' : 'same as the main page until book signals pass'],
      ['Green line / red line', `${pct(L.green, 0)} / ${pct(L.red, 0)}`, early ? '' : `over the green line for ${Math.round(N.held || 0)} s (needs ${D.hold_secs} s)`],
    ]);
    const rows = [];
    if (bk) for (const [i, d] of [[1, 25], [3, 100], [5, 500]]) rows.push([`Resting within $${d}`, `${num(bk.bids[i])} buy / ${num(bk.asks[i])} sell`, 'BTC']);
    rows.push(['Trading pace', F.pace == null ? '&ndash;' : `${num(F.pace, 2)} BTC / min`, 'market orders, last 5 minutes']);
    rows.push(['Coinbase vs Kraken / Bitstamp', F.gapdev == null ? '&ndash;' : `${F.gapdev >= 0 ? '+' : '−'}$${num(Math.abs(F.gapdev), 0)}`, 'off its usual gap']);
    rows.push(['Order-book snapshots saved', `${(S.depth_rows ?? 0).toLocaleString()}`,
      S.depth_error ? `last attempt failed: ${esc(S.depth_error)}` : `${(S.depth_days ?? 0).toFixed(2)} of ${S.depth_need} days needed before book signals are fitted`]);
    rows.push(['Light readings scored', `${(S.readings ?? 0).toLocaleString()}`, `${S.blocks ?? 0} intervals`]);
    $('bookfull').innerHTML = kv(rows);
  }

  if (FI) {
    // Short list: what is on now and whether it counts yet
    $('flagsmini').innerHTML = Object.entries(FI).map(([k, [nm]]) => {
      const e = E[k] || {}, on = N?.flags?.[k];
      return `<div><dt>${esc(nm)}${on ? `<small>${esc(on)}</small>` : ''}</dt>` +
        `<dd><small>${e.proven ? 'in use' : `watching, ${e.blocks ?? 0} of ${(D.prove || [40])[0]}`}</small>${on ? '<span class="on-flag">on</span>' : '<span class="dim">off</span>'}</dd></div>`;
    }).join('');
    $('flags').innerHTML = '<tr><th>Flag</th><th>Now</th><th>Status</th><th>Intervals seen</th><th>Said</th><th>Right</th><th>Gap</th></tr>' +
      Object.entries(FI).map(([k, [nm, action, desc]]) => {
        const e = E[k] || {}, on = N?.flags?.[k];
        return `<tr><td>${esc(nm)}<small>${esc(desc)} Once proven: ${action === 'red' ? 'turns the light red' : 'blocks green'}.</small></td>` +
          `<td>${on ? '<span class="on-flag">on</span>' : '<span class="dim">off</span>'}</td>` +
          `<td>${e.proven ? '<span class="pill use">in use</span>' : '<span class="pill no">watching</span>'}</td>` +
          `<td>${e.blocks ?? 0}</td><td>${pct(e.stated)}</td><td>${pct(e.right)}</td>` +
          `<td>${e.gap == null ? '&ndash;' : `<span class="${e.gap > 0 ? 'down' : ''}">${(e.gap * 100).toFixed(1)} pts</span>`}</td></tr>`;
      }).join('');
    const [pb, pg] = D.prove || [40, .05, 2];
    $('flagfoot').innerHTML = `<b>Said</b>: the average hold chance shown while the flag was on. <b>Right</b>: how often those calls stood. A flag starts to count once it has been seen in ${pb} intervals and its calls were right at least ${(pg * 100).toFixed(0)} points less often than said, by more than chance would explain.`;
  }

  if (B) {
    $('bdmeta').innerHTML = B.total ? `<b>${S.blocks ?? 0}</b> intervals scored` +
      (B.green_blocks ? `. Green showed in <b>${B.green_blocks}</b> of them and the call was wrong in <b>${B.green_wrong}</b>.` : '.')
      : 'Nothing scored yet. The first results appear after the current interval ends.';
    $('bd').innerHTML = B.total ? '<tr><th>Light</th><th>Share of time</th><th>Said</th><th>Right</th></tr>' +
      ['green', 'yellow', 'red'].map(c => { const x = B.colors[c];
        return `<tr><td>${dot(c)}${name(c)}</td><td>${pct(x.share, 0)}</td><td>${pct(x.stated, 0)}</td><td>${pct(x.right, 0)}</td></tr>`; }).join('') : '';
    const gain = x => x.d_ll == null || !x.n ? '' : `<span class="${x.d_ll > 0 ? 'up' : x.d_ll < 0 ? 'down' : ''}">${x.d_ll > 0 ? '+' : ''}${x.d_ll.toFixed(4)}</span>`;
    const row = (nm, x, vs = '') => `<tr><td>${nm}</td><td>${(x.n || 0).toLocaleString()}</td><td>${pct(x.stated)}</td><td>${pct(x.right)}</td><td>${vs}</td></tr>`;
    $('cmp').innerHTML = B.total ? '<tr><th>Call</th><th>Readings</th><th>Said</th><th>Right</th><th>Log loss vs main</th></tr>' +
      row('Main page', B.calls.live) + (B.calls.test.n ? row('Testing page', B.calls.test, gain(B.calls.test)) : '') +
      (B.calls.book?.n ? row('Adjusted by the orders (experimental)', B.calls.book, gain(B.calls.book)) : '')
      : '<tr><td class="dim">Nothing scored yet.</td></tr>';
    const cv = B.cover || {};
    $('cmpfoot').innerHTML = (cv.book?.n ? `Final price landed inside the 9-in-10 range: main page <b>${pct(cv.cone.inside)}</b>, bent by the orders <b>${pct(cv.book.inside)}</b> (90% is the target). ` : '') +
      '<b>Log loss vs main</b>: positive means better than the main page on the same readings. Treat it as noise until a few hundred intervals are in.';
  }

  $('retrain').disabled = D.busy; $('retrain').textContent = D.busy ? 'Training…' : 'Retrain now';
  if (V) {
    const M = V.model, dd = V.depth_days ?? 0;
    const book = M.status === 'collecting' ? `collecting order-book history (<b>${dd.toFixed(1)}</b> of ${S.depth_need ?? 14} days)`
      : M.status === 'in use' ? `in use (<b>${esc(M.kept.join(', '))}</b>, log loss better by ${M.d_ll.toFixed(4)} on unseen readings)`
      : `tested on ${M.n_hold.toLocaleString()} unseen readings, not better than the main page (${M.d_ll >= 0 ? '+' : ''}${M.d_ll.toFixed(4)})`;
    $('trmeta').innerHTML = `Green at <b>${pct(V.lines.green, 0)}</b>, red under <b>${pct(V.lines.red, 0)}</b>, fitted on <b>${V.train_days.toFixed(1)}</b> days and checked on the most recent <b>${V.hold_days.toFixed(1)}</b>, ${ago(V.at)}.<br>Book signals: ${book}`;
    $('tr').innerHTML = '<tr><th>Light</th><th>Share of time</th><th>Said</th><th>Right, fitted days</th><th>Right, unseen days</th></tr>' +
      ['green', 'yellow', 'red'].map(c => `<tr><td>${dot(c)}${name(c)}</td><td>${pct(V.hold[c].share, 0)}</td>` +
        `<td>${pct(V.hold[c].stated)}</td><td>${pct(V.train[c].right)}</td><td>${pct(V.hold[c].right)}</td></tr>`).join('');
  }
  const VS = D.versions || [];
  $('vs').innerHTML = VS.length ? '<tr><th>Run</th><th>Green line</th><th>Red line</th><th>Green right, unseen</th><th>Green share</th><th>Red right, unseen</th><th>Book signals</th><th>Flags in use</th></tr>' +
    VS.map(v => `<tr><td>#${v.id} · ${ago(v.at)}</td><td>${pct(v.lines.green, 0)}</td><td>${pct(v.lines.red, 0)}</td><td>${pct(v.green.right)}</td>` +
      `<td>${pct(v.green.share, 0)}</td><td>${pct(v.red.right)}</td><td>${esc(v.model === 'in use' ? v.kept.join(', ') : v.model)}</td>` +
      `<td>${v.proven.length ? esc(v.proven.map(k => D.flag_info[k][0]).join(', ')) : 'none yet'}</td></tr>`).join('')
    : '<tr><td class="dim">No training run yet.</td></tr>';
}
try { $('more').open = localStorage.getItem('light_more') === '1'; } catch (e) { /* stays closed */ }
$('more').addEventListener('toggle', () => { try { localStorage.setItem('light_more', $('more').open ? '1' : '0'); } catch (e) {} });

// ---- chart: the main page's chart (range, example paths) drawn together with the order book
let C = null, GEO = null, Wp = null, wKey = null;
const UP = '46,189,133', DN = '246,70,93', WARN = '232,184,74', PAPER = '#eef0f3', MUTED = '#868d9a';
const MONO = '11px "JetBrains Mono", ui-monospace, monospace';
const money = (p, d = 2) => p.toLocaleString('en-US', {minimumFractionDigits: d, maximumFractionDigits: d});
const LAYERS = [
  ['price', 'Price', `background:${PAPER};height:2px`, 0],
  ['start', 'Start', `background:none;border-top:1px dashed ${MUTED};height:0`, 0],
  ['cone', 'Likely range (50% / 90%)', `background:linear-gradient(rgba(${UP},.45) 50%,rgba(${DN},.45) 50%);height:12px`, 0],
  ['paths', 'Example paths', `background:${MUTED};height:1px`, 0],
  ['light', 'Light colour', `background:linear-gradient(90deg,rgb(${DN}) 33%,rgb(${WARN}) 33% 66%,rgb(${UP}) 66%);height:4px`, 0],
  ['grid', 'Grid', 'background:rgba(255,255,255,.25);height:1px', 0],
  ['heat', 'So far', `background:linear-gradient(90deg,rgba(${UP},.8),rgba(${DN},.8))`, 1],
  ['ahead', 'Resting now', `background:linear-gradient(90deg,rgba(${UP},.4),rgba(${DN},.4))`, 1],
  ['bars', 'Size at each price', `background:linear-gradient(rgba(${DN},.8) 50%,rgba(${UP},.8) 50%);height:12px`, 1],
  ['walls', 'Largest', `background:none;border:1px solid rgb(${UP});height:10px`, 1],
  ['bookrange', 'Range bent by the orders', `background:rgb(${WARN});height:2px`, 1],
];
const SHOW = {}; LAYERS.forEach(([k]) => SHOW[k] = true);
try { Object.assign(SHOW, JSON.parse(localStorage.getItem('light_layers') || '{}')); } catch (e) { /* storage blocked: all on */ }
function toggles() {
  const btn = ([k, n, sw]) => `<button type="button" class="tg" data-k="${k}" aria-pressed="${!!SHOW[k]}"><i style="${sw}"></i>${n}</button>`;
  $('toggles').innerHTML = LAYERS.filter(l => !l[3]).map(btn).join('') + '<span class="gap"></span><span class="grp">Orders</span>' +
    LAYERS.filter(l => l[3]).map(btn).join('') + (LAYERS.every(([k]) => SHOW[k]) ? '' : '<button type="button" class="tg all" data-k="*">Show all</button>');
}
$('toggles').addEventListener('click', e => {
  const b = e.target.closest('button'); if (!b) return;
  if (b.dataset.k === '*') LAYERS.forEach(([k]) => SHOW[k] = true); else SHOW[b.dataset.k] = !SHOW[b.dataset.k];
  try { localStorage.setItem('light_layers', JSON.stringify(SHOW)); } catch (e) { /* not remembered, still works */ }
  toggles(); draw();
});
toggles();

function buildPaths(seed) {              // same recipe and seed as the main page, so the example paths match it
  let a = seed >>> 0;
  const r = () => { a = a + 0x6D2B79F5 | 0; let t = Math.imul(a ^ a >>> 15, 1 | a); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; };
  const gs = () => Math.sqrt(-2 * Math.log(r() || 1e-9)) * Math.cos(2 * Math.PI * r());
  Wp = [];
  for (let k = 0; k < 10; k++) { const w = [0]; for (let i = 1; i <= 180; i++) w.push(w[i - 1] + gs() * Math.sqrt(5)); Wp.push(w); }
}
const wAt = (w, s) => { s = Math.max(0, Math.min(900, s)); const i = Math.min(179, Math.floor(s / 5)), f = s / 5 - i; return w[i] + (w[i + 1] - w[i]) * f; };

function draw() {
  const cv = $('cv'), w = cv.clientWidth, h = cv.clientHeight, dpr = window.devicePixelRatio || 1;
  if (!w) return;
  if (cv.width !== Math.round(w * dpr) || cv.height !== Math.round(h * dpr)) { cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr); }
  const g = cv.getContext('2d');
  g.setTransform(dpr, 0, 0, dpr, 0, 0); g.clearRect(0, 0, w, h);
  g.font = MONO; g.textBaseline = 'middle';
  if (!C || !C.start || !C.sigma) { g.fillStyle = '#565c67'; g.fillText('Waiting for the live predictor', 12, 20); GEO = null; return; }
  const N = D?.now, path = C.path.slice(), t0 = C.start;
  let now = C.now, P = C.price;
  if (N && N.start === t0 && N.t > now) { now = N.t; P = N.price; path.push([now, P]); }
  now = Math.max(t0, Math.min(now, C.end));
  const left = C.end - now, S = C.start_price, bin = C.bin, early = now - t0 < (C.warmup || 0), PJ = early ? null : C.proj;
  const sg = C.sigma / (C.k_now || 1), mu = (C.tilt || 0) * sg * Math.sqrt(left / 60);
  const band = (z, sec) => P * Math.exp((left > 0 ? mu * sec / left : 0) + z * sg * Math.sqrt(Math.max(0, sec) / 60));

  // Vertical range: everything seen so far, the start price, and the far end of whichever ranges are showing.
  let lo = Math.min(S, P, band(-1.645, left)), hi = Math.max(S, P, band(1.645, left));   // sets the scale even while projections are hidden
  for (const [, p] of path) { if (p < lo) lo = p; if (p > hi) hi = p; }
  if (PJ && SHOW.bookrange && left > 0) { lo = Math.min(lo, ...PJ.dn90); hi = Math.max(hi, ...PJ.up90); }
  const pad = (hi - lo) * 0.14 || P * 0.0004; lo -= pad; hi += pad;
  const narrow = w < 600, gut = SHOW.bars ? (narrow ? 36 : 84) : 0, L = 12, R = (narrow ? 86 : 128) + gut, T = 18, B = 30;
  const pw = w - L - R, ph = h - T - B, x1 = L + pw, ax = x1 + gut;
  const X = t => L + (t - t0) / 900 * pw, Y = p => T + (hi - p) / (hi - lo) * ph;
  const yS = Y(S), xN = X(now), bh = Math.max(1, ph * bin / (hi - lo)), heat = C.heat || [], last = heat[heat.length - 1];

  // Grid and axis labels
  g.lineWidth = 1;
  const raw = (hi - lo) / 6, mag = Math.pow(10, Math.floor(Math.log10(raw))), step = [1, 2, 5, 10].map(m => m * mag).find(v => v >= raw);
  g.textAlign = 'left';
  for (let v = Math.ceil(lo / step) * step; v < hi; v += step) {
    const y = Math.round(Y(v)) + 0.5;
    if (SHOW.grid) { g.strokeStyle = 'rgba(255,255,255,.06)'; g.beginPath(); g.moveTo(L, y); g.lineTo(x1, y); g.stroke(); }
    g.fillStyle = MUTED;
    if ((!SHOW.start || Math.abs(y - yS) > 12) && Math.abs(y - Y(P)) > 12) g.fillText(money(v, step < 1 ? 2 : 0), ax + 10, y);
  }
  g.textBaseline = 'top';
  for (let m = 0; m <= 15; m += 5) {
    const x = Math.round(X(t0 + m * 60)) + 0.5;
    if (SHOW.grid) { g.strokeStyle = m % 15 ? 'rgba(255,255,255,.06)' : 'rgba(255,255,255,.14)'; g.beginPath(); g.moveTo(x, T); g.lineTo(x, T + ph); g.stroke(); }
    g.fillStyle = MUTED; g.textAlign = m === 0 ? 'left' : m === 15 ? 'right' : 'center';
    g.fillText(clock(t0 + m * 60).replace(/\s?[AP]M/i, ''), x, T + ph + 10);
  }
  g.textBaseline = 'middle';

  // Resting orders. "Bright" is the 97th percentile of what is in view now, so one huge order does not wash out the rest.
  let ref = 10;
  if (last) {
    const inView = last[3].filter((v, k) => v > 0 && last[2] + k * bin + bin > lo && last[2] + k * bin < hi).sort((a, b) => a - b);
    if (inView.length) ref = Math.max(10, inView[Math.floor(inView.length * 0.97)]);
  }
  const bands = (snap, xa, xb, dim) => {
    const [, mid, base, sizes] = snap;
    for (let k = 0; k < sizes.length; k++) {
      const v = sizes[k]; if (!v) continue;
      const p = base + k * bin; if (p + bin < lo || p > hi) continue;
      g.fillStyle = `rgba(${p + bin / 2 < mid ? UP : DN},${(Math.min(1, v / ref) ** 0.7 * 0.6 * dim).toFixed(3)})`;
      g.fillRect(xa, Y(p + bin), xb - xa, bh + 0.5);
    }
  };
  g.save(); g.beginPath(); g.rect(L, T, pw + gut, ph); g.clip();
  if (SHOW.heat) for (let i = 0; i < heat.length; i++) {
    const xa = Math.max(L, X(heat[i][0])), xb = Math.min(xN, i + 1 < heat.length ? X(heat[i + 1][0]) : xN);
    if (xb > xa) bands(heat[i], xa, xb + 0.5, 1);
  }
  if (last && SHOW.ahead && left > 0) bands(last, xN, x1, 0.4);
  if (last && SHOW.bars) {
    let top = 1;
    last[3].forEach((v, k) => { const p = last[2] + k * bin; if (p + bin > lo && p < hi && v > top) top = v; });
    last[3].forEach((v, k) => {
      const p = last[2] + k * bin; if (!v || p + bin < lo || p > hi) return;
      g.fillStyle = `rgba(${p + bin / 2 < last[1] ? UP : DN},.75)`;
      g.fillRect(x1 + 4, Y(p + bin) + 0.5, Math.max(1, (gut - 8) * v / top), Math.max(1, bh - 1));
    });
  }
  g.restore();

  if (early) {                                                       // no projections yet: mark where they begin
    const xw = Math.round(X(t0 + C.warmup)) + 0.5;
    g.setLineDash([2, 4]); g.strokeStyle = MUTED; g.lineWidth = 1; g.beginPath(); g.moveTo(xw, T); g.lineTo(xw, T + ph); g.stroke(); g.setLineDash([]);
    g.fillStyle = MUTED; g.textAlign = 'left'; g.fillText('projections start', xw + 6, T + 8);
  }
  if (left > 0 && !early) {
    // Likely range, tinted by which side of the start price it is on (as on the main page)
    if (SHOW.cone) {
      const n = 48, cone = z => { g.beginPath();
        for (let i = 0; i <= n; i++) { const s = left * i / n; g.lineTo(X(now + s), Y(band(z, s))); }
        for (let i = n; i >= 0; i--) { const s = left * i / n; g.lineTo(X(now + s), Y(band(-z, s))); }
        g.closePath(); g.fill(); };
      for (const [color, ya, yb] of [[UP, T, yS], [DN, yS, T + ph]]) {
        if (yb <= ya) continue;
        g.save(); g.beginPath(); g.rect(xN, Math.max(T, ya), x1 - xN, Math.min(T + ph, yb) - Math.max(T, ya)); g.clip();
        g.fillStyle = `rgba(${color},.12)`; cone(1.645); g.fillStyle = `rgba(${color},.16)`; cone(0.674); g.restore();
      }
    }
    // Example paths
    if (SHOW.paths) {
      if (wKey !== t0) { buildPaths(t0); wKey = t0; }
      const sNow = now - t0, sgS = sg / Math.sqrt(60);
      g.save(); g.beginPath(); g.rect(xN, T, x1 - xN, ph); g.clip(); g.lineWidth = 1;
      for (const wk of Wp) {
        const base = wAt(wk, sNow), end = P * Math.exp(mu + sgS * (wk[180] - base));
        g.strokeStyle = `rgba(${end > S ? UP : DN},.3)`; g.beginPath(); g.moveTo(xN, Y(P));
        for (let s = Math.ceil(sNow / 5) * 5; s <= 900; s += 5) g.lineTo(X(t0 + s), Y(P * Math.exp(mu * (s - sNow) / left + sgS * (wAt(wk, s) - base))));
        g.stroke();
      }
      g.restore();
    }
    // The same range, bent by the resting orders
    if (SHOW.bookrange && PJ) {
      g.save(); g.beginPath(); g.rect(L, T, pw, ph); g.clip(); g.lineJoin = 'round';
      for (const [key, a, lw] of [['up90', 1, 1.75], ['dn90', 1, 1.75], ['up50', .55, 1], ['dn50', .55, 1]]) {
        g.strokeStyle = `rgba(${WARN},${a})`; g.lineWidth = lw; g.beginPath();
        PJ.s.forEach((s, i) => { const x = X(PJ.t + s), y = Y(PJ[key][i]); i ? g.lineTo(x, y) : g.moveTo(x, y); });
        g.stroke();
      }
      g.restore();
    }
  }

  // Start price line
  if (SHOW.start) {
    g.setLineDash([5, 5]); g.strokeStyle = MUTED; g.lineWidth = 1;
    g.beginPath(); g.moveTo(L, Math.round(yS) + 0.5); g.lineTo(x1, Math.round(yS) + 0.5); g.stroke(); g.setLineDash([]);
    g.fillStyle = MUTED; g.textAlign = 'left'; g.fillText((narrow ? '' : 'start ') + money(S), ax + 10, yS);
  }
  // Price so far, with a dark underlay so it reads over bright bands
  if (SHOW.price) {
    g.lineJoin = 'round';
    for (const [col, lw] of [['#090a0c', 4.5], [PAPER, 2]]) {
      g.strokeStyle = col; g.lineWidth = lw; g.beginPath();
      path.forEach(([t, p], i) => i ? g.lineTo(X(t), Y(p)) : g.moveTo(X(t), Y(p)));
      g.lineTo(xN, Y(P)); g.stroke();
    }
  }
  // The light's colour over time
  if (SHOW.light) {
    const Ls = C.lights || [];
    for (let i = 0; i < Ls.length; i++) {
      const xa = X(Ls[i][0]), xb = i + 1 < Ls.length ? X(Ls[i + 1][0]) : xN;
      g.fillStyle = Ls[i][1] === 'wait' ? 'rgba(255,255,255,.12)' : `rgb(${Ls[i][1] === 'green' ? UP : Ls[i][1] === 'red' ? DN : WARN})`;
      g.fillRect(xa, T + ph + 2, Math.max(1, xb - xa + 0.5), 4);
    }
  }
  // Largest resting orders in view
  if (SHOW.walls && C.walls) {
    const walls = [...C.walls.b.map(x => [...x, UP]), ...C.walls.a.map(x => [...x, DN])], used = [];
    g.textAlign = 'right';
    for (const [p, size, col] of walls.sort((a, b) => b[1] - a[1])) {
      const y = Y(p);
      if (p <= lo || p >= hi || used.length >= 4 || used.some(u => Math.abs(u - y) < 15)) continue;
      used.push(y);
      const txt = size.toFixed(1) + ' BTC', tw = g.measureText(txt).width + 8;
      g.fillStyle = 'rgba(9,10,12,.85)'; g.fillRect(x1 - tw - 2, y - 8, tw, 16);
      g.fillStyle = `rgb(${col})`; g.fillText(txt, x1 - 6, y);
    }
  }
  // Now marker and price tag
  g.strokeStyle = 'rgba(238,240,243,.22)'; g.lineWidth = 1; g.beginPath(); g.moveTo(Math.round(xN) + 0.5, T); g.lineTo(Math.round(xN) + 0.5, T + ph); g.stroke();
  const col = P >= S ? UP : DN;
  g.fillStyle = `rgba(${col},.25)`; g.beginPath(); g.arc(xN, Y(P), 8, 0, 7); g.fill();
  g.fillStyle = `rgb(${col})`; g.beginPath(); g.arc(xN, Y(P), 3.5, 0, 7); g.fill();
  const yP = SHOW.start && Math.abs(Y(P) - yS) < 13 ? yS + (Y(P) >= yS ? 13 : -13) : Y(P), tagW = R - gut - 13;
  g.fillStyle = `rgb(${col})`; g.beginPath(); g.roundRect ? g.roundRect(ax + 5, yP - 9, tagW, 18, 3) : g.rect(ax + 5, yP - 9, tagW, 18); g.fill();
  g.fillStyle = '#090a0c'; g.font = '700 ' + MONO; g.textAlign = 'left'; g.fillText(money(P), ax + 10, yP + 0.5);

  GEO = {lo, hi, y0: T, y1: T + ph, last, bin};
  const rg = r => `${money(r[0], 0)} – ${money(r[1], 0)}`, chg = (P / S - 1) * 100, soon = `in ${mmss(Math.max(0, (C.warmup || 0) - (now - t0)))}`;
  $('s_start').textContent = money(S); $('s_now').textContent = money(P); $('s_chg').textContent = (chg >= 0 ? '+' : '') + chg.toFixed(3) + '%';
  $('s_range').textContent = early ? soon : left > 0 ? rg([band(-1.645, left), band(1.645, left)]) : '–';
  $('s_book').textContent = early ? soon : left > 0 && PJ ? rg(PJ.book) : '–';
}
$('cv').addEventListener('mousemove', e => {
  if (!GEO) return;
  const r = $('cv').getBoundingClientRect(), y = e.clientY - r.top;
  if (y < GEO.y0 || y > GEO.y1) { $('hov').textContent = ''; return; }
  const p = GEO.hi - (y - GEO.y0) / (GEO.y1 - GEO.y0) * (GEO.hi - GEO.lo), Lb = GEO.last;
  let txt = '$' + money(p, 0);
  if (Lb) {
    const k = Math.floor((p - Lb[2]) / GEO.bin), v = Lb[3][k];
    if (v != null) txt += ` · ${(v / 10).toFixed(1)} BTC of ${Lb[2] + k * GEO.bin + GEO.bin / 2 < Lb[1] ? 'buy' : 'sell'} orders resting in this $${GEO.bin} band`;
  }
  $('hov').textContent = txt;
});
$('cv').addEventListener('mouseleave', () => { $('hov').textContent = ''; });
window.addEventListener('resize', draw);
async function pollChart() {
  try { C = await (await fetch('/testing-2/chart', {cache: 'no-store'})).json(); draw(); } catch (e) {}
}

function tick() {
  if (D?.now?.end) {
    const left = Math.max(0, D.now.end - Date.now() / 1000);
    $('count').textContent = mmss(left);
  }
}
async function poll() {
  try { D = await (await fetch('/testing-2/data', {cache: 'no-store'})).json(); render(); draw(); }
  catch (e) { $('err').hidden = false; $('err').textContent = 'Light script not reachable.'; }
}
$('retrain').onclick = async () => { $('retrain').disabled = true; await fetch('/testing-2/retrain', {method: 'POST'}); poll(); };
poll(); pollChart(); setInterval(poll, 2000); setInterval(pollChart, 5000); setInterval(tick, 250);
</script>
</body>
</html>
""".replace("__CSS__", PAGE_CSS)


if __name__ == "__main__":
    main()
