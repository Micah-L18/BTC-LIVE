# BTC-LIVE

A live, self-tuning guess about Bitcoin's current 15-minute interval.

Bitcoin's price is split into 15-minute blocks that start at :00, :15, :30 and :45. Every 2 seconds this program asks one question:

> **Will this 15-minute block end higher or lower than where it started, and how sure are we?**

It shows the answer as a percentage (for example, "72% UP") in the terminal and on a live chart at `http://localhost:8000`.

**The start price matches Robinhood.** Each block starts at the target price of Robinhood's 15-minute BTC up/down contract (Kalshi's `KXBTC15M` market), fetched when the block opens. That target is the average of an index (CF Benchmarks' BRTI) over the 60 seconds before the block, so it is usually a few dollars away from Coinbase's price at the bell, and sometimes much more. Coinbase's price is shifted by the gap so the lead is measured on the same footing, and each block is scored with the contract's own result. `--start exchange` goes back to Coinbase's opening price. History used for self-tuning still comes from Coinbase's 1-minute candles, which is a close stand-in, not an exact match.

The chart page shows no call, percentage or projected range for the first 5 minutes of a block, because too much can still change that early. The readings are still worked out and saved.

> This is a statistics project, not financial advice.

---

## The idea in one paragraph

Suppose BTC opened the block at $100,000 and it's now $100,300, with 3 minutes left. Can it fall $300 in 3 minutes? That depends on how much BTC has been moving lately. If it usually moves about $50 a minute, a $300 lead with 3 minutes left is pretty safe. If it's been jumping $500 a minute, that lead could vanish easily. **This program measures the lead, measures how jumpy BTC has been, and works out the odds that the lead holds until the bell.**

---

## How it makes a call

At every reading, the program looks at three things:

| What | Plain meaning |
|---|---|
| **The lead** | How far the price is now above (or below) where the block started |
| **Time left** | How many minutes until the block ends |
| **Jumpiness** (volatility) | How big a typical 1-minute move has been recently |

Then it combines them:

```
                         how far ahead we are
score  =  ─────────────────────────────────────────────────
           how far BTC typically moves in the time remaining
```

- **Big lead, little time, calm market** gives a big score, which means very confident.
- **Small lead, lots of time, wild market** gives a score near zero, which is roughly a coin flip.

The score then goes through a bell curve, which turns it into a probability between 0% and 100%. This is the same math behind coin-flip random walks and option pricing. It assumes the price wanders randomly from here on, and asks how likely it is to wander back past the starting line.

The real formula, for the curious:

```
P(up) = Φ( k · ln(now / start) / (σ · √minutes_left) + tilt )
```

`Φ` is the bell curve, `σ` is jumpiness, `k` is a confidence dial, and `tilt` is an optional nudge (both explained below).

---

## How it teaches itself

The program keeps a local database of every 1-minute BTC price, every call it makes, and how each block actually ended. Once a day (or when you press **Retrain now** on the chart page), it uses that history to adjust itself.

### 1. It replays history

It goes back through past days (30 by default, up to 90) and rebuilds every 15-minute block, minute by minute: "at minute 7, the price was this far ahead, and BTC was this jumpy... did it end up or down?" That gives it tens of thousands of practice examples.

### 2. It tries different settings

- **How far back to measure jumpiness:** the last 30 minutes, 1 hour, 2 hours, 4 hours or 8 hours. A short window reacts fast; a long window is steadier.
- **Confidence dials (`k`):** three of them, one each for early, middle and late in the block. If the raw formula turns out to be a bit overconfident early on, the early dial is turned down a little, and so on.
- **Extra clues (the "tilt"):** it checks whether *how* the price got here matters, not just *where* it is now:

  | Clue | Question it asks |
  |---|---|
  | momentum | Has it been moving fast in one direction over the last few minutes? |
  | steady | Has it spent most of the block on this side of the start? |
  | crossings | Has it flip-flopped across the starting price a lot? |
  | giveback | Has it given back much of its best lead? |
  | trend | Which way have the last five blocks been heading? |
  | streak | How many blocks in a row finished this way? |
  | burst | Is it suddenly much jumpier in the last 5 minutes? |
  | chop | Is it zig-zagging minute to minute? |
  | range | Has this block's high-to-low range been unusually wide? |
  | swing75 | Were the last five blocks unusually wild? |

  **A clue is only used if it clearly shows up in the history.** Most of them turn out to be noise and get dropped automatically.

### 3. It tests itself fairly

The most recent 20% of history is **held back** and not used for tuning. After building a new version on the older 80%, it's tested on that unseen recent data. That's like studying old exams and then taking a new one.

### 4. It only upgrades if the new version is actually better

The new version replaces the current one only if it scores clearly better on the held-back data. Otherwise nothing changes.

### What does "better" mean?

The goal isn't just "right more often." The goal is **honest percentages**. When it says 70%, it should be right about 70% of the time. When it says 90%, about 90% of the time. The chart page shows a confidence table so you can check this yourself.

---

## What it has learned so far

- The plain formula already works well. Tuning mostly makes the percentages a little more honest, slightly less confident early in a block and slightly more confident near the end.
- Clues like momentum, trend and streaks have shown **almost no predictive power**. In other words, past direction doesn't say much about future direction over 15 minutes, which is what you'd expect from a busy market.
- The one clue that keeps showing up is **swing75**: when the last hour or so has been unusually wild, a lead is less safe than it looks. So far it hasn't beaten the current version by enough to be adopted.

So the honest summary is: **it's a well-calibrated "will the lead hold?" calculator, not a crystal ball for which way BTC will go next.**

---

## Running it

```bash
pip install requests
python btc_15.py
```

The first run downloads price history, which takes about a minute. After that it only fetches what's new. A chart opens at `http://localhost:8000`. Stop with Ctrl+C.

Useful options:

```bash
python btc_15.py --source binance        # use Binance instead of Coinbase
python btc_15.py --every 1               # read the price every second
python btc_15.py --history-days 90       # tune on more history
python btc_15.py --retrain-hours 12      # re-tune more often
python btc_15.py --market polymarket     # also record Polymarket's odds (or kalshi)
python btc_15.py --no-web                # terminal only, no chart page
```

Delete the `.db` file to start fresh.

### Built to run for days

Errors are retried with growing pauses, it slows down if the exchange says it's asking too often, and outages are logged. On a Mac, keep the computer awake with `caffeinate -i python btc_15.py`.

### What else it records

Every 10 seconds it also saves extra data for testing future ideas: the best bid/ask prices, how much was bought vs sold, BTC's price on Kraken and Bitstamp, and (with `--market`) betting-market odds. **None of this affects the calls yet.**

---

## The testing lab (`btc_lab.py`)

A separate sandbox for trying new ideas **without touching the live predictor**. Run it next to the main script:

```bash
python btc_lab.py      # page at http://localhost:8001/testing (also reachable at :8000/testing)
```

It tests extra clues on top of the live model, including time-of-day patterns, volume bursts, buy/sell flow, the 1-hour and 4-hour trend, round-number price levels ($500 steps) and the previous block's high and low. Each one is tested two ways:

1. **On history**, with the same "study old data, test on unseen recent data" rule as above.
2. **Live, side by side** with the real predictor, scored as each block ends. This is the real test: an idea can look good on history by luck, but live data can't be fooled.

It also records Polymarket and Kalshi odds so you can see how the predictor compares to the betting market. An idea that wins in the lab still has to be added to `btc_15.py` and pass its self-test before it affects real calls.

---

## The traffic light (`btc_light.py`)

A third script, with its own database (`btc_light.db`) and its own page, that answers a different question from the percentage: **how much should the current call be trusted right now?**

```bash
python btc_light.py    # page at http://localhost:8002/testing-2 (also reachable at :8000/testing-2)
```

It needs `btc_15.py` running, and shows the testing lab's call beside it when `btc_lab.py` is running too. It only reads from them; it never changes a call.

**The light also shows on the main page**, in the Confidence card, while this script is running (it says "Off" when it is not). While `btc_lab.py` is running, the main page's projection is the lab's test model (the main model plus the signals that passed the backtest); otherwise it is the main model. Either way, the readings `btc_15.py` saves and scores are still its own model's, so the lab and the light keep comparing against an unchanged baseline.

The row of buttons under the main page's header switches each part of the page, and each layer of the chart, on or off. The choice is remembered in that browser.

| Light | Meaning |
|---|---|
| **Red** | The feed is down or stale, the exchanges disagree, the chance the call holds is under the red line, or a proven "stop" warning is on |
| **Green** | The chance the call holds has stayed over the green line for a minute, and no proven warning is on |
| **Yellow** | Everything else |

For the first 5 minutes of each block there is no light, call or projection on this page, because too much can still change. Those minutes are recorded but never scored.

### The chart

The page's chart is the main page's chart (price, start price, likely range, example paths) drawn over the order book: green and red bands show buy and sell orders resting at each price, brighter meaning more BTC. Every part can be switched on and off with the buttons under it.

It also draws an experimental **range bent by the orders**: the main page's likely range, pulled in where resting orders are thick and let out where they are thin, with a matching order-adjusted call. Resting orders can be pulled before the price reaches them, so this is scored live against the main page before anything relies on it.

### What it records

Every 10 seconds it saves a summary of Coinbase's full order book: how much BTC is resting within $10 to $1,000 of the price on each side, the five largest single orders per side, and any of those that shrank by half since the last look. Past order books can't be downloaded, so this history only grows from the day the script starts.

### The warning flags

| Flag | What it looks for |
|---|---|
| Choppy | The price has crossed the start several times this block |
| Wild market | Recent swings are well above the usual |
| Thin book | Little is resting between the price and the start, compared with how much is trading |
| Large order ahead | An unusually large order sits close by, between the price and the start |
| Large order pulled | A large order protecting the lead vanished without being traded |
| Large order eaten | A large order protecting the lead is being traded away |
| Sweep against the lead | A burst of market orders against the lead |

### How it gets better

- **The green and red lines are fitted, not fixed.** Every few hours they are refitted on history and checked on recent days the fit never saw.
- **Flags have to earn their place.** Every flag is logged from day one but only "watched". It starts to change the colour once the record shows calls made while it was on were clearly worse than their percentages claimed, and it stops again if the record stops showing that.
- **Order-book signals join after 14 days.** Once enough book history exists, the signals are fitted on top of the live model and used for the light only if they beat it on unseen readings.
- **Every reading is scored.** The page shows how often green, yellow and red were actually right, beside the main page's and the testing page's calls on the same moments.

