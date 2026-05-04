# Decision Layer — Signal Methodology

This document explains how the pipeline transforms daily market data into an actionable
**BUY / SELL / HOLD** signal for every Tadawul-listed symbol, with a formal certainty
factor (−1.0 to +1.0) and a traceable explanation for every decision.

It is written as a system-design document, not a technical spec. The goal is to show
**why each component exists, why it matters, and how all pieces connect** — so that
a reviewer, evaluator, or domain expert can follow the full reasoning without needing
to read SQL.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Conceptual Model: Propose → Protect → Validate](#conceptual-model-propose--protect--validate)
3. [System Architecture](#system-architecture)
4. [Why a Decision Layer?](#why-a-decision-layer)
5. [Part 1 — The Six Gold Layer Models](#part-1--the-six-gold-layer-models)
   - 1.1 `gold_technical_rating` — Propose
   - 1.2 `gold_volatility_index` — Protect (Risk Gate)
   - 1.3 `gold_anomaly_flags` — Protect (Outlier Gate)
   - 1.4 `gold_52w_levels` — Validate (Position Context)
   - 1.5 `gold_sector_performance` — Validate (Market Context)
   - 1.6 `gold_intraday_vwap` — Execution Timing
6. [Part 2 — The Decision Table (Knowledge Base)](#part-2--the-decision-table-knowledge-base)
   - 2.1 Why a Decision Table Instead of Pure ML?
   - 2.2 Certainty Factors: Quantifying Judgment
   - 2.3 The 36 Production Rules
   - 2.4 Specificity-Based Rule Matching
   - 2.5 Rule Selection Flow
7. [Part 3 — Anomaly SELL Override](#part-3--anomaly-sell-override)
8. [Part 4 — How a Signal Is Formed: Step by Step](#part-4--how-a-signal-is-formed-step-by-step)
9. [Part 5 — Realistic End-to-End Examples](#part-5--realistic-end-to-end-examples)
10. [Part 6 — Explanation Facility (XAI)](#part-6--explanation-facility-xai)
11. [Part 7 — Output Columns Reference](#part-7--output-columns-reference)
12. [Part 8 — Query Examples](#part-8--query-examples)
13. [Part 9 — Maintaining the Knowledge Base](#part-9--maintaining-the-knowledge-base)
14. [Limitations & Caveats](#limitations--caveats)

---

## Executive Summary

The decision layer is a **rule-based expert system** that synthesizes six market
models into one row per `(symbol, date)` containing: a directional signal, a
certainty factor, and a human-readable explanation. It does not use machine learning.
Instead, it encodes domain expertise as 36 explicit production rules in a decision
table — a file any domain expert can edit without touching SQL.

The process has four conceptual stages:

| Stage | What it does | Gold models involved |
|---|---|---|
| **Propose** | Establishes a directional bias from technical indicators | `gold_technical_rating` |
| **Protect** | Gates or blocks the proposal when market conditions are risky | `gold_volatility_index`, `gold_anomaly_flags` |
| **Validate** | Adjusts conviction based on context and price extremes | `gold_52w_levels`, `gold_sector_performance` |
| **Execute** | Provides entry-level timing context (output only, not a decision key) | `gold_intraday_vwap` |

Every signal is accompanied by `why_signal`, `why_not_buy`, and `reasoning_trace` —
embodying Explainable AI (XAI) principles so that any stakeholder can trace the
decision back to the exact rule that fired.

---

## Conceptual Model: Propose → Protect → Validate

Before diving into implementation, understand the three-layer reasoning that governs
every signal. This model makes the decision logic transparent, teachable, and auditable.

```
┌──────────────────────────────────────────────────────────────────────┐
│                        PROPOSE  (What to do)                         │
│                                                                      │
│  gold_technical_rating → rating: Strong Buy / Buy / Neutral /        │
│                                  Sell / Strong Sell                  │
│                                                                      │
│  Eight indicators vote +1/0/−1. The net score maps to a categorical  │
│  label. This is the base directional bias before any risk check.     │
└─────────────────────────────┬────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        PROTECT  (Should we act?)                     │
│                                                                      │
│  gold_volatility_index → vol_level (high / extreme blocks BUY)       │
│  gold_anomaly_flags    → has_price_anomaly (overrides to HOLD/SELL)  │
│                                                                      │
│  Risk gates that can cancel or reverse the proposal. High vol means  │
│  entries are unsafe. Anomalies signal non-normal behaviour that may  │
│  invalidate the technical read. If a protect condition fires, the     │
│  signal is downgraded or blocked — regardless of the proposal.       │
└─────────────────────────────┬────────────────────────────────────────┘
                              │  (if protect passes or is overridden)
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        VALIDATE  (How strongly?)                     │
│                                                                      │
│  gold_sector_performance → sector_ok (headwind / tailwind)           │
│  gold_52w_levels        → at_52w_pos (near support / resistance)     │
│                                                                      │
│  These factors do not create a signal. They adjust the certainty     │
│  factor up or down. A strong sector or a 52-week low increases BUY   │
│  conviction; a weak sector or 52-week high decreases it or blocks.   │
└─────────────────────────────┬────────────────────────────────────────┘
                              │
                              ▼
                   Final SIGNAL + CF + explanation
```

**How this maps to the decision table:** Every one of the 36 rules is labelled with
its role in this model. Protect rules always take precedence over Validate rules when
both apply to the same (symbol, date) — by design, safety overrides opportunity.

| Role | Decision table condition | State IDs |
|---|---|---|
| **Propose** | `rating_group` | All rows |
| **Protect** | `vol_level = high` or `extreme` | S01, S02, S08, S09, S15, S16, S22, S23, S29, S30 |
| **Protect** | `has_anomaly = true` | S03, S10, S17, S24, S31 |
| **Validate** | `sector_ok = false` | S04, S11, S18, S25, S32 |
| **Validate** | `at_52w_pos = near_low` or `near_high` | S05, S06, S12, S13, S19, S20, S26, S27, S33, S34 |
| **Default** | No special conditions | S07, S14, S21, S28, S35 |

---

## System Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER  (6 feature models)                  │
│  Each model runs daily and produces a table of (symbol, date, features) │
├────────────────────────────────────────────────────────────────────────┤
│  gold_technical_rating   → signal_score, rating, RSI, SMAs, BB        │
│  gold_volatility_index   → annualized_vol, vol_level                   │
│  gold_anomaly_flags      → has_price_anomaly, has_volume_anomaly       │
│  gold_52w_levels         → at_52w_high, at_52w_low, pct_from_high/low │
│  gold_sector_performance → sector_advance_ratio, sector_ok             │
│  gold_intraday_vwap      → session_vwap                                │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │  All six joined in one SQL model
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│              KNOWLEDGE BASE: dbt/seeds/decision_table.csv              │
│  36 production rules — each row: IF conditions THEN signal, CF         │
│  Example: IF rating='Strong Buy' AND anomaly=false AND sector_ok=true  │
│           AND vol='normal' AND at_52w='near_low' THEN BUY, CF=+0.92   │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │  Specificity-ordered lookup
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   decision_signals  (single unified output table)       │
│  • signal (BUY / SELL / HOLD)                                          │
│  • signal_cf (−1.0 to +1.0) and confidence (HIGH / MEDIUM / LOW)      │
│  • state_id (which rule fired)                                         │
│  • why_signal, why_not_buy, why_not_sell, reasoning_trace  (XAI)      │
│  • All gold layer context columns (for auditing and explanation)       │
└────────────────────────────────────────────────────────────────────────┘
```

**Knowledge representation techniques used (Lecture 4):**

| Technique | Where | § |
|---|---|---|
| **Decision table** | `decision_table.csv` — 36 IF-THEN rules with CF | §4.7 Formal Logic |
| **Decision tree** | `signal_score → rating` — 5-branch CASE in SQL | §4.7 Formal Logic |
| **Production rules** | Each row of the decision table is a named production rule | §4.1 |
| **Certainty Factors** | `signal_cf` column — formal uncertainty (−1.0 to +1.0) | §4.10 |

---

## Why a Decision Layer?

The six gold models each provide a narrow slice of information: trend, volatility,
anomalies, 52-week proximity, sector strength, VWAP. None alone is sufficient for a
good trading decision.

The decision layer **integrates** these slices using an explicit knowledge base. This
is **knowledge-based systems (KBS) in production**: every signal traces to a specific
rule, every certainty factor is pre-assigned by an expert, and the logic can be
updated without retraining models. A pure ML classifier would require thousands of
labelled examples, and its decisions would be unauditable — the opposite of what a
KBS demands.

---

## Part 1 — The Six Gold Layer Models

Each gold model exists because it captures one distinct kind of market evidence. The
decision layer reads their ready-made outputs — it does not recompute anything.

---

### 1.1 `gold_technical_rating` — Propose

**Role:** **PROPOSE** — provides the base directional bias before any risk or context
check is applied.

#### Why it exists
A single price number does not reveal whether the market is trending upward,
weakening, overbought, or at a statistical extreme. Technical indicators condense
price history into a consistent directional summary.

#### Why it is used
The decision table needs a coarse but meaningful measure of direction. Rather than
inspecting eight raw indicators one by one, it only needs the rating group: `Strong
Buy`, `Buy`, `Neutral`, `Sell`, or `Strong Sell`.

#### Why this matters
This model is the system's **first filter for directional bias**. It tells the decision
layer whether the stock is attractive, uncertain, or weak — before any risk gate is
applied. Everything else modifies this base proposal.

#### Technical voting logic

Each of 8 indicators casts a vote: +1 (bullish), 0 (neutral), −1 (bearish).

| Rule | Indicator | Buy vote (+1) | Sell vote (−1) |
|---|---|---|---|
| R01 | Price vs SMA10 | close > SMA10 | close < SMA10 |
| R02 | Price vs SMA20 | close > SMA20 | close < SMA20 |
| R03 | Price vs SMA50 | close > SMA50 | close < SMA50 |
| R04 | Price vs SMA200 | close > SMA200 | close < SMA200 |
| R05 | MA cross | SMA10 > SMA20 (golden cross) | SMA10 < SMA20 (death cross) |
| R06 | RSI(14) | RSI < 30 — oversold | RSI > 70 — overbought |
| R07 | Bollinger Bands | close < BB\_lower | close > BB\_upper |
| R08 | MACD proxy | SMA12 > SMA26 | SMA12 < SMA26 |

#### Score-to-rating mapping (decision tree, §4.7)

```
signal_score = Σ votes   (range: −8 to +8)

signal_score ≥ +5  →  Strong Buy
signal_score ≥ +3  →  Buy
signal_score ≤ −5  →  Strong Sell
signal_score ≤ −3  →  Sell
otherwise          →  Neutral
```

This 5-branch mapping is a **decision tree** (§4.7 Formal Logic): it compresses the
continuous score into a categorical label used as the primary key in the decision table.

#### Key columns used downstream

| Column | Purpose in decision layer |
|---|---|
| `rating` | **Primary decision-table key** — one of 5 categories |
| `signal_score` | Used in the anomaly SELL override condition |
| `buy_signals`, `sell_signals` | Explanation output ("6/8 indicators bullish") |
| `rsi14`, `sma50`, `sma200`, `bb_lower`, `bb_upper` | Context for human-readable explanations |

---

### 1.2 `gold_volatility_index` — Protect (Risk Gate)

**Role:** **PROTECT** — gates or blocks the proposal when risk is too high.

#### Why it exists
A stock can look attractive technically but still be too dangerous to enter if its
recent price swings are extreme. Volatility is **risk**, not direction.

#### Why it is used
Volatility acts as a **risk gate**. It does not tell you whether the stock will go up
or down. It tells you whether the environment is calm enough to trust a directional
signal. High volatility means wider spreads, more slippage, and higher probability of
stop-loss hits from normal noise.

#### Why this matters
This model protects the system from overconfident entries during chaotic conditions.
A stock with 80% annualised volatility can gap 5% overnight. Entering a position based
on technical indicators in that environment is like trying to park on a trampoline.
The volatility gate exists precisely to prevent this.

#### Definition

$$r_t = \ln\!\left(\frac{\text{close}_t}{\text{close}_{t-1}}\right) \qquad \text{annualized\_vol} = \sigma_{20d} \times \sqrt{252}$$

where $\sigma_{20d}$ is the rolling 20-day standard deviation of log-returns.

#### Volatility classification (decision key: `vol_level`)

| `annualized_vol` | `vol_level` | Approximate daily swing | Protection effect |
|---|---|---|---|
| > 80% | `extreme` | > 5% per day | Blocks BUY entirely; SELL CF slightly reduced (panic reversal risk) |
| > 50% | `high` | > 3% per day | Reduces BUY conviction; often forces HOLD |
| 20%–50% | `normal` | 1.2%–3% per day | Standard environment — allows full BUY/SELL |
| ≤ 20% | `low` | < 1.2% per day | Highest confidence for directional trades |

---

### 1.3 `gold_anomaly_flags` — Protect (Outlier Gate)

**Role:** **PROTECT** — overrides the proposal when price or volume behaviour is
statistically abnormal.

#### Why it exists
Technical indicators are calibrated on normal market behaviour. An anomalous day
(crash, spike, news event, data error) violates the assumptions underlying those
indicators. Acting on a technical signal during an anomaly is like navigating with a
compass near a magnet.

#### Why it is used
Anomalies represent events where price action is **not informationally reliable**. A
sudden 10% drop may be a genuine breakdown — or it may be a flash crash that reverts
within hours. Rather than trying to distinguish the two in real time, the system
treats `has_price_anomaly = true` as a **caution flag**: it blocks BUY, and if
combined with a negative score, it triggers SELL.

#### Why this matters
Without anomaly detection, the system would happily issue a BUY signal immediately
after a stock spikes up on unusual volume — exactly the moment most likely to be
followed by mean reversion. The anomaly gate prevents this.

#### Two independent detectors

**Volume Z-score (30-day rolling):**
$$z = \frac{V_t - \bar{V}_{30d}}{\sigma_{V,30d}} \qquad \text{flagged if } |z| > 2.5$$

**Price IQR / Tukey fence (90-day trailing log-returns):**
$$\text{lower} = Q_1 - 1.5 \times IQR \qquad \text{upper} = Q_3 + 1.5 \times IQR$$
Flagged when the daily log-return falls outside `[lower, upper]`.

#### Key columns used downstream

| Column | Role |
|---|---|
| `has_price_anomaly` | **Decision key** — blocks BUY; triggers SELL override with negative score |
| `has_volume_anomaly` | Passed through to output for explanation and auditing |

---

### 1.4 `gold_52w_levels` — Validate (Position Context)

**Role:** **VALIDATE** — adjusts conviction based on support and resistance proximity.

#### Why it exists
Price extremes are **psychological anchors**. The 52-week high and low represent the
range within which a stock has traded over the past year. Proximity to these levels
carries predictive meaning that raw technical indicators do not capture.

#### Why it is used
The 52-week position fine-tunes conviction **after** the proposal and protection
stages pass. It answers the question: *"Even if we should buy, is this a particularly
good or bad price level to enter?"*

#### Why this matters
Near a 52-week low, mean-reversion traders expect a bounce — this is among the most
reliable setups in technical analysis. The decision table reflects this: a Strong Buy
near the 52-week low gets CF=+0.92 (the highest in the table), while the same rating
near the 52-week high gets CF=+0.72. Conversely, for SELL signals, a stock near its
52-week low has potential support that reduces sell conviction (S26, CF=−0.52), while
a breakdown from a 52-week high is more likely to persist (S27, CF=−0.82).

#### Definition

$$\text{high}_{52w} = \max(\text{high}_{t-251},\ldots,\text{high}_t) \qquad \text{low}_{52w} = \min(\text{low}_{t-251},\ldots,\text{low}_t)$$

Proximity flags (within 2% of extreme): `at_52w_high`, `at_52w_low`.

#### Decision key mapping (`at_52w_pos`)

| Condition | `at_52w_pos` | Validation effect on BUY | Validation effect on SELL |
|---|---|---|---|
| `at_52w_low = true` | `near_low` | **Boosts CF** — mean reversion opportunity | **Reduces CF** — potential support/bounce |
| `at_52w_high = true` | `near_high` | **Reduces CF** — resistance zone | **Boosts CF** — confirmed breakdown from peak |
| Neither | `neutral` | No adjustment | No adjustment |

---

### 1.5 `gold_sector_performance` — Validate (Market Context)

**Role:** **VALIDATE** — confirms or contradicts the proposal based on broad sector
strength.

#### Why it exists
Even the best stock can struggle when its whole sector is declining. A rising tide
lifts most boats; a falling tide drops them regardless of individual quality.

#### Why it is used
Sector performance is the **macro context check**. If a stock shows a Buy signal but
80% of its sector is declining today, something is likely wrong — either the stock is
special, or the technical signal is noise. The decision table conservatively blocks
BUY when `sector_ok = false`, requiring the sector to confirm the individual signal.

#### Why this matters
Without sector validation, the system would issue BUY signals into sector-wide
selloffs. With it, a Buy-rated stock that is fighting against a declining sector is
downgraded to HOLD — a more defensible and empirically sounder position.

#### Definition

$$\text{advance\_ratio} = \frac{\text{\# stocks in sector with positive daily return}}{\text{\# total stocks in sector}}$$

$$\text{sector\_ok} = (\text{advance\_ratio} \ge 0.5)$$

**Effect on decision table:**
- `sector_ok = false` → BUY is blocked (S04, S11, S18); SELL is strengthened (S25, CF=−0.75)
- `sector_ok = true` → no constraint added; validate rules (S05–S07 etc.) apply normally

---

### 1.6 `gold_intraday_vwap` — Execution Timing

**Role:** **Not a decision key.** Included in the output for informational purposes only.

#### Why it exists
VWAP (Volume-Weighted Average Price) represents the average price at which the
market has transacted throughout the session. It is the reference price used by
institutional order desks to evaluate execution quality.

#### Why it is not a decision key
The decision table answers *"what to do"* — direction and conviction. VWAP answers
*"when to execute"* — entry timing. These are different questions. A BUY signal means
the system judges the stock worth buying; VWAP tells a human trader whether to buy
now or wait for a dip toward VWAP for a better average cost.

#### Why this matters
Including VWAP in the output allows downstream users to enrich the signal without
baking timing logic into the decision layer itself — preserving the separation between
*deciding* and *executing*.

#### Definition

$$\text{VWAP} = \frac{\sum_t \text{price}_t \times \text{volume}_t}{\sum_t \text{volume}_t}$$

`session_vwap` is `NULL` for days with no tick data (batch-only historical loads).
During market hours (Sun–Thu 10:00–15:00 Riyadh time), the Kafka tick stream provides
updates via Spark Structured Streaming.

---

## Part 2 — The Decision Table (Knowledge Base)

### 2.1 Why a Decision Table Instead of Pure ML?

| Aspect | Decision Table (KBS) | Pure ML Classifier |
|---|---|---|
| **Explainability** | Every signal traces to a specific named rule | Black box — per-instance reasoning is invisible |
| **Editable by domain expert** | Edit CSV, run `dbt seed` — no code | Requires retraining, relabelling, ML engineering |
| **Certainty factors** | Explicitly assigned from expert judgment | Requires probabilistic calibration from data |
| **Performance with sparse history** | Works with any amount of data | Needs thousands of labelled examples |
| **Guaranteed behaviour** | No unintended interactions — rules are independent | May learn spurious correlations in training data |
| **Tie-breaking** | Explicit specificity ordering — fully deterministic | Implicit and hard to debug |

**Trade-off:** The decision table cannot learn complex non-linear interactions beyond
the 5-dimensional condition space. But for this domain — combining a handful of
categorical market features — a well-designed decision table is **interpretable,
editable, and sufficient**.

---

### 2.2 Certainty Factors: Quantifying Judgment (§4.10)

A **certainty factor (CF)** is a number between −1.0 and +1.0 that encodes both the
**direction** and the **strength of belief** in a signal. This follows Lecture 4
§4.10 (Certainty Factors), where belief and disbelief are represented as independent
signed values that can be combined.

```
  −1.0         −0.65      −0.40          0         +0.40      +0.65          +1.0
    │             │          │            │            │          │             │
    └─ HIGH SELL ─┘  MED SELL   ──── LOW / HOLD ────   MED BUY    └─ HIGH BUY ─┘
  (maximum conviction)            (weak or no signal)          (maximum conviction)
```

| \|CF\| range | `confidence` | What it means in practice |
|---|---|---|
| ≥ 0.65 | **HIGH** | Strong conviction — suitable for position sizing |
| 0.40–0.64 | **MEDIUM** | Notable signal but with meaningful uncertainty |
| < 0.40 | **LOW** | Weak conviction — lean toward holding; avoid new positions |

**Why CF instead of binary BUY/SELL/HOLD?**
CF allows **ranking** within a signal type. Two BUY signals — CF=0.92 and CF=0.55 —
are both BUY, but the former is more actionable. CF also provides a smooth transition:
a rule that almost blocks BUY can express +0.18 (slight positive lean) rather than
forcing a flat 0.00 HOLD, preserving nuance without inventing extra signal categories.

**How CF values were assigned:** They encode the propose-protect-validate framework:

- **+0.92 (S05, Strong Buy + near_low):** Full proposal (Strong Buy) + all protections pass + strong validation (near 52W low). Maximum BUY.
- **+0.22 (S01, Strong Buy + high vol):** Good proposal but volatility gate fires. Positive lean (technicals still strong) but BUY blocked.
- **−0.12 (S02, Strong Buy + extreme vol):** Extreme volatility turns the CF negative — entering here could result in a gap loss.
- **−0.95 (S31, Strong Sell + anomaly):** Strongest SELL: maximum negative proposal + protect confirmation. No context can reverse this.
- **−0.52 (S26, Sell + near_low):** SELL signal but near a 52-week low that might provide support. Reduced conviction.

---

### 2.3 The 36 Production Rules

The table is organised as **5 rating groups × 7 scenarios + 1 fallback**.
Every group covers the same 7 scenarios, ensuring symmetric coverage:

| Scenario # | Condition | Role | State IDs |
|---|---|---|---|
| 1 | `vol_level = high` | **Protect** | S01, S08, S15, S22, S29 |
| 2 | `vol_level = extreme` | **Protect** | S02, S09, S16, S23, S30 |
| 3 | `has_anomaly = true` | **Protect** | S03, S10, S17, S24, S31 |
| 4 | `sector_ok = false` | **Validate** | S04, S11, S18, S25, S32 |
| 5 | `at_52w_pos = near_low` | **Validate** | S05, S12, S19, S26, S33 |
| 6 | `at_52w_pos = near_high` | **Validate** | S06, S13, S20, S27, S34 |
| 7 | Default (no special conditions) | — | S07, S14, S21, S28, S35 |

**Fallback S36:** All conditions `any` → HOLD, CF=0.00. Guarantees every row matches.

**Full rule table (with role labels — see `decision_table.csv` for exact values):**

| State | Rating | Anomaly | Sector | Vol | 52W | Signal | CF | Conf | Role |
|---|---|---|---|---|---|---|---|---|---|
| S01 | Strong Buy | false | true | high | any | HOLD | +0.22 | LOW | Protect (vol) |
| S02 | Strong Buy | false | true | extreme | any | HOLD | −0.12 | LOW | Protect (vol) |
| S03 | Strong Buy | true | any | any | any | HOLD | −0.20 | LOW | Protect (anomaly) |
| S04 | Strong Buy | false | false | any | any | HOLD | +0.12 | LOW | Validate (sector) |
| S05 | Strong Buy | false | true | any | near\_low | **BUY** | **+0.92** | HIGH | Validate (52W) |
| S06 | Strong Buy | false | true | any | near\_high | **BUY** | +0.72 | HIGH | Validate (52W) |
| S07 | Strong Buy | false | true | any | any | **BUY** | +0.88 | HIGH | Default |
| S08 | Buy | false | true | high | any | HOLD | +0.18 | LOW | Protect (vol) |
| S09 | Buy | false | true | extreme | any | HOLD | −0.12 | LOW | Protect (vol) |
| S10 | Buy | true | any | any | any | HOLD | −0.28 | LOW | Protect (anomaly) |
| S11 | Buy | false | false | any | any | HOLD | 0.00 | LOW | Validate (sector) |
| S12 | Buy | false | true | any | near\_low | **BUY** | +0.80 | HIGH | Validate (52W) |
| S13 | Buy | false | true | any | near\_high | **BUY** | +0.55 | MEDIUM | Validate (52W) |
| S14 | Buy | false | true | any | any | **BUY** | +0.70 | HIGH | Default |
| S15 | Neutral | false | true | high | any | HOLD | −0.10 | LOW | Protect (vol) |
| S16 | Neutral | false | true | extreme | any | HOLD | −0.18 | LOW | Protect (vol) |
| S17 | Neutral | true | any | any | any | HOLD | −0.20 | LOW | Protect (anomaly) |
| S18 | Neutral | false | false | any | any | HOLD | −0.08 | LOW | Validate (sector) |
| S19 | Neutral | false | true | any | near\_low | HOLD | +0.12 | LOW | Validate (52W) |
| S20 | Neutral | false | true | any | near\_high | HOLD | −0.08 | LOW | Validate (52W) |
| S21 | Neutral | any | any | any | any | HOLD | 0.00 | LOW | Default |
| S22 | Sell | false | true | high | any | **SELL** | −0.68 | HIGH | Protect (vol) |
| S23 | Sell | false | true | extreme | any | **SELL** | −0.60 | MEDIUM | Protect (vol) |
| S24 | Sell | true | any | any | any | **SELL** | −0.85 | HIGH | Protect (anomaly) |
| S25 | Sell | false | false | any | any | **SELL** | −0.75 | HIGH | Validate (sector) |
| S26 | Sell | false | true | any | near\_low | **SELL** | −0.52 | MEDIUM | Validate (52W) |
| S27 | Sell | false | true | any | near\_high | **SELL** | −0.82 | HIGH | Validate (52W) |
| S28 | Sell | any | any | any | any | **SELL** | −0.70 | HIGH | Default |
| S29 | Strong Sell | false | true | high | any | **SELL** | −0.80 | HIGH | Protect (vol) |
| S30 | Strong Sell | false | true | extreme | any | **SELL** | −0.72 | HIGH | Protect (vol) |
| S31 | Strong Sell | true | any | any | any | **SELL** | **−0.95** | HIGH | Protect (anomaly) |
| S32 | Strong Sell | false | false | any | any | **SELL** | −0.88 | HIGH | Validate (sector) |
| S33 | Strong Sell | false | true | any | near\_low | **SELL** | −0.68 | HIGH | Validate (52W) |
| S34 | Strong Sell | false | true | any | near\_high | **SELL** | **−0.92** | HIGH | Validate (52W) |
| S35 | Strong Sell | any | any | any | any | **SELL** | −0.90 | HIGH | Default |
| S36 | any | any | any | any | any | HOLD | 0.00 | LOW | Fallback |

---

### 2.4 Specificity-Based Rule Matching

**The problem:** Multiple rules may match the same `(symbol, date)`. For example, a
stock with `Strong Buy, no anomaly, sector_ok, high vol, near_low` matches both:
- S01 (vol=high gate) — conditions: rating, anomaly=false, sector=true, vol=high, 52W=any
- S05 (near_low differentiator) — conditions: rating, anomaly=false, sector=true, vol=any, 52W=near_low

Both have **4 non-`any` conditions**. Which rule should win?

**Resolution:** Select the row with the highest `specificity_score`. On a tie, the
**lower `state_id`** wins.

```
specificity_score = count(condition ≠ 'any')
                  = (rating_group ≠ 'any') + (has_anomaly ≠ 'any')
                  + (sector_ok ≠ 'any') + (vol_level ≠ 'any') + (at_52w_pos ≠ 'any')
```

**Why lower state_id wins on a tie:** Within each rating group, gate rows (S01–S04,
S08–S11, etc.) have lower state_ids than differentiator rows (S05–S07, S12–S14, etc.).
This means **volatility and anomaly gates take precedence over 52-week proximity**.
That is intentional: **risk management (protect) overrides opportunity (validate)**.

**Tie-breaking examples:**

| Input conditions | Matching rules | Specificities | Winner | Reason |
|---|---|---|---|---|
| Strong Buy + high vol + near_low | S01 (vol=high), S05 (near_low) | 4, 4 | **S01** | Lower ID → protect wins over validate |
| Strong Buy + normal vol + near_low | S05 only (S01 vol=high doesn't match) | 4 | **S05** | Only match |
| Strong Buy + anomaly + near_low | S03 (anomaly=true), S05 (anomaly=false → no match) | 2 | **S03** | Only match; S05 requires anomaly=false |
| Strong Buy + no anomaly + sector_fail + near_low | S04 (sector=false), S05 (sector=true → no match) | 3 | **S04** | Only match; S05 requires sector_ok=true |

**Implicit priority hierarchy:**

```
1. Protect rules (vol=high/extreme, anomaly=true)  — specificity 4, lower state_id
2. Validate rules (sector=false, near_low, near_high) — specificity 3-4, higher state_id
3. Default rules (no special conditions)           — specificity 1-3
4. Fallback (S36)                                  — specificity 0
```

---

### 2.5 Rule Selection Flow

```
              ┌──────────────────────────────────────────────┐
              │          INPUT: 5 decision keys              │
              │  rating, has_anomaly, sector_ok,             │
              │  vol_level, at_52w_pos                       │
              └──────────────────────┬───────────────────────┘
                                     │
                                     ▼
              ┌──────────────────────────────────────────────┐
              │   Find all matching rules in decision_table  │
              │   (non-'any' conditions must equal input)    │
              └──────────────────────┬───────────────────────┘
                                     │
                                     ▼
              ┌──────────────────────────────────────────────┐
              │   Compute specificity_score for each match   │
              │   = count of non-'any' conditions            │
              └──────────────────────┬───────────────────────┘
                                     │
                                     ▼
              ┌──────────────────────────────────────────────┐
              │   Select winner                              │
              │   highest specificity_score first            │
              │   tie → lowest state_id (gate beats diff.)  │
              └──────────────────────┬───────────────────────┘
                                     │
                                     ▼
              ┌──────────────────────────────────────────────┐
              │   Output: signal, signal_cf, state_id        │
              └──────────────────────────────────────────────┘
```

---

## Part 3 — Anomaly SELL Override

**Why this exists:** The decision table handles anomalies primarily as a **BUY
blocker** — rows S03, S10, S17, S24, S31 produce HOLD or SELL when `has_anomaly=true`.
However, the original system logic defines a specific SELL trigger that the table
alone cannot express: a **Neutral-rated stock with a price anomaly and a negative
signal_score** should produce a SELL, not a HOLD.

Row S17 (Neutral + anomaly) gives HOLD with CF=−0.20, which is conservative but
technically correct for the anomaly-alone case. However, when the anomaly co-occurs
with a negative score (signal_score < 0), the combination represents a genuine
breakdown that warrants a SELL.

**The override rule:**

```
IF  has_price_anomaly = TRUE
AND rating = 'Neutral'   (signal_score in range −2 to 0)
AND signal_score < 0
THEN  signal = SELL,  signal_cf = −0.50,  confidence = MEDIUM
```

**Why CF=−0.50?** This is a medium-conviction SELL. It is weaker than a Sell-rated
stock with anomaly (S24, CF=−0.85) because the technical rating is Neutral, not
confirmed Sell. But the combination of a negative score plus a price anomaly (an
abnormal downward move) suggests a genuine breakdown, not noise. CF=−0.50 sits in
the MEDIUM zone — strong enough to act on, not so strong as to be confused with a
structurally bearish stock.

**Example:** A stock with rating=Neutral (score=−2), vol=normal, no sector issues,
but a 9% drop flagged as a price-IQR anomaly. The table would give HOLD (S17,
CF=−0.20). The override fires → SELL, CF=−0.50, confidence=MEDIUM.

---

## Part 4 — How a Signal Is Formed: Step by Step

The full inference for one `(symbol, date)` proceeds in five steps:

**Step 1 — Gather gold layer outputs**

The model joins all six gold tables on `(symbol, date)`. Gold models handle their
own incremental computation; the decision model reads the result directly.

**Step 2 — Classify continuous values into discrete decision keys**

| Raw column | Decision key | Mapping |
|---|---|---|
| `signal_score` | `rating` | ≥+5 → Strong Buy, ≥+3 → Buy, ≤−5 → Strong Sell, ≤−3 → Sell, else → Neutral |
| `annualized_vol` | `vol_level` | >0.80 → extreme, >0.50 → high, >0.20 → normal, else → low |
| `at_52w_high`, `at_52w_low` | `at_52w_pos` | at_low → near_low, at_high → near_high, else → neutral |
| `sector_advance_ratio` | `sector_ok` | ≥ 0.50 → true, else → false |
| `has_price_anomaly` | `has_anomaly` | as-is (boolean) |

**Step 3 — Look up the decision table (specificity-ordered)**

Match all 36 rows against the five decision keys. Select the row with the highest
`specificity_score`; break ties by lowest `state_id`. This row determines the initial
`signal`, `signal_cf`, and `confidence`.

**Step 4 — Apply anomaly SELL override if condition is met**

Check: `has_price_anomaly = TRUE AND rating = 'Neutral' AND signal_score < 0`.
If true, override the table result with `SELL, CF=−0.50, MEDIUM`.

**Step 5 — Assemble explanation columns and write output**

Generate `why_signal`, `why_not_buy`, `why_not_sell`, `reasoning_trace` from the
matched state and the actual condition values. Write one row per `(symbol, date)`.

---

## Part 5 — Realistic End-to-End Examples

### Example A: Strong Buy + Near 52-Week Low → BUY (CF=0.92)

**Propose:** Strong Buy (score=+6)
**Protect:** No anomaly, vol=normal (25%) → all gates pass
**Validate:** sector_ok=true (65% advancing), at_52w_pos=near_low (1.5% above 52W low)

| Step | Action | Result |
|---|---|---|
| 1 | Classify inputs | rating=Strong Buy, vol=normal, 52W=near_low, anomaly=false, sector_ok=true |
| 2 | Match decision table | S05 matches (specificity=4); S07 also matches (specificity=3) → S05 wins |
| 3 | Read rule | S05: BUY, CF=+0.92, HIGH |
| 4 | Override check | Not triggered |
| 5 | Output | BUY, CF=+0.92, HIGH, state_id=S05 |

```
signal = BUY   signal_cf = 0.92   confidence = HIGH   state_id = S05

why_signal:
  "BUY (CF=0.92, HIGH): state=S05 — Best setup: strong technicals +
   clean conditions + near 52W low (mean reversion)"

why_not_buy: NULL   (signal IS buy)

reasoning_trace:
  "1. Inputs classified: rating=Strong Buy, score=6/8, vol=normal,
   52W=near_low, anomaly=false, sector_ok=true.
   2. Decision table matched: state=S05 (specificity=4).
   3. Signal=BUY, CF=0.92 (HIGH)."
```

---

### Example B: Buy Rating Blocked by High Volatility → HOLD (CF=0.18)

**Propose:** Buy (score=+3)
**Protect:** vol=high (62% annualised) → **volatility gate fires**, blocks full BUY
**Validate:** sector_ok=true, 52W=neutral — but protect already won

| Step | Action | Result |
|---|---|---|
| 1 | Classify inputs | rating=Buy, vol=high, 52W=neutral, anomaly=false, sector_ok=true |
| 2 | Match decision table | S08 (vol=high, spec=4) and S14 (default, spec=3) both match → S08 wins |
| 3 | Read rule | S08: HOLD, CF=+0.18, LOW |
| 4 | Override check | Not triggered (no anomaly) |
| 5 | Output | HOLD, CF=+0.18, LOW, state_id=S08 |

```
signal = HOLD   signal_cf = 0.18   confidence = LOW   state_id = S08

why_signal:
  "HOLD (CF=0.18, LOW): state=S08 — High volatility (62% ann.) blocks BUY —
   position sizing unsafe, but technicals are positive."

why_not_buy:
  "high volatility (62.4% ann.) — position risk"

reasoning_trace:
  "1. Inputs: rating=Buy, score=3/8, vol=high, 52W=neutral,
   anomaly=false, sector_ok=true.
   2. Decision table matched: state=S08 (high vol gate, spec=4).
   3. Signal=HOLD, CF=0.18 (LOW)."
```

---

### Example C: Neutral + Price Anomaly + Negative Score → SELL Override (CF=−0.50)

**Propose:** Neutral (score=−2) — no strong directional bias
**Protect:** has_price_anomaly=true → table gives HOLD (S17, CF=−0.20)
**Override condition met:** Neutral + anomaly + negative score → SELL

| Step | Action | Result |
|---|---|---|
| 1 | Classify inputs | rating=Neutral, score=−2, vol=normal, anomaly=true, sector_ok=false |
| 2 | Match decision table | S17 (anomaly, spec=2) → HOLD, CF=−0.20 |
| 3 | Check override | Neutral AND anomaly AND score < 0 → TRUE |
| 4 | Apply override | SELL, CF=−0.50, MEDIUM, state_id=NULL |
| 5 | Output | SELL, CF=−0.50, MEDIUM |

```
signal = SELL   signal_cf = -0.50   confidence = MEDIUM   state_id = NULL

why_signal:
  "SELL (CF=-0.50, MEDIUM): Anomaly SELL override — Neutral rating with
   negative score (−2/8) + price anomaly = breakdown confirmation"

why_not_buy:
  "Rating is 'Neutral' (score=-2/8) — not eligible for BUY; price anomaly detected"

reasoning_trace:
  "1. Inputs: rating=Neutral, score=-2/8, vol=normal, 52W=neutral,
   anomaly=true, sector_ok=false.
   2. Decision table matched: state=S17 (anomaly gate) → HOLD, CF=-0.20.
   3. Anomaly SELL override condition met → final Signal=SELL, CF=-0.50 (MEDIUM).
   [anomaly SELL override applied]"
```

---

## Part 6 — Explanation Facility (XAI)

The system implements explainable AI (XAI) as defined in Lecture 4 §4.8–4.9: every
output row carries human-readable fields that trace the decision from input to output.
A reviewer can understand any signal without reading SQL.

### `why_signal` — Dynamic Justification

A template-based string explaining why the **specific** signal was chosen. It always
references the matched rule (`state_id`) or override condition.

| Scenario | Example |
|---|---|
| S05 (Strong Buy + near_low) | `"BUY (CF=0.92, HIGH): state=S05 — Best setup: strong technicals + clean conditions + near 52W low"` |
| S08 (Buy + high vol) | `"HOLD (CF=0.18, LOW): state=S08 — High volatility (62% ann.) blocks BUY"` |
| S24 (Sell + anomaly) | `"SELL (CF=-0.85, HIGH): state=S24 — Sell rating + price anomaly = compounded bearish"` |
| Override | `"SELL (CF=-0.50, MEDIUM): Anomaly SELL override — breakdown confirmation"` |

### `why_not_buy` — Tracing the Blocker (NULL when signal = BUY)

Reports the **first blocking condition** in protect-before-validate order:

1. Rating not eligible (Neutral, Sell, Strong Sell)
2. Price anomaly detected
3. Sector advancing < 50%
4. High or extreme volatility
5. Otherwise: decision table state

This ordering reflects the conceptual model: protect conditions are reported before
validate conditions, matching the priority in which they fire.

### `why_not_sell` — NULL when signal = SELL

```
"Rating is 'Buy' and no triggering anomaly"
"SELL not triggered: rating is 'Neutral' and no triggering anomaly"
```

### `reasoning_trace` — Numbered How-Trace (§4.9 Tracing)

A numbered sequence showing every inference step from input classification to final
signal. Step 1 shows classified inputs; Step 2 shows the matched rule; Step 3 shows
the final output; override is noted when applied.

These four fields together satisfy all four explanation types from §4.8:
- **Why** → `why_signal`
- **Why Not** → `why_not_buy`, `why_not_sell`
- **How** → `reasoning_trace`
- **Journalistic** → the compact `explanation` string (who/what/when/why)

---

## Part 7 — Output Columns Reference

| Column | Type | Description |
|---|---|---|
| `symbol` | VARCHAR | 4-digit Tadawul code |
| `company_name` | VARCHAR | Full company name |
| `sector` | VARCHAR | Sector classification |
| `date` | DATE | Trading date |
| `close` | DOUBLE | Closing price (SAR) |
| `signal` | VARCHAR | **BUY / SELL / HOLD** |
| `signal_cf` | DOUBLE | Certainty factor −1.0 to +1.0 (§4.10) |
| `confidence` | VARCHAR | **HIGH** (≥0.65) / **MEDIUM** (≥0.40) / **LOW** (<0.40) |
| `state_id` | VARCHAR | Matched decision table row (e.g. `S05`); NULL on override |
| `why_signal` | VARCHAR | Dynamic explanation of this signal |
| `why_not_buy` | VARCHAR | First blocking condition — NULL when signal = BUY |
| `why_not_sell` | VARCHAR | Why SELL not triggered — NULL when signal = SELL |
| `reasoning_trace` | VARCHAR | Numbered step-by-step inference trace |
| `rating` | VARCHAR | Strong Buy / Buy / Neutral / Sell / Strong Sell |
| `signal_score` | INT | −8 to +8 net indicator vote |
| `buy_signals` | INT | Count of buy votes (0–8) |
| `sell_signals` | INT | Count of sell votes (0–8) |
| `rsi14` | DOUBLE | 14-day RSI |
| `sma50` / `sma200` | DOUBLE | 50-day and 200-day SMAs |
| `bb_lower` / `bb_upper` | DOUBLE | Bollinger Band boundaries (SMA20 ± 2σ) |
| `annualized_vol` | DOUBLE | 20-day volatility × √252 |
| `vol_level` | VARCHAR | low / normal / high / extreme |
| `high_52w` / `low_52w` | DOUBLE | 52-week high and low |
| `pct_from_high` / `pct_from_low` | DOUBLE | % distance from 52-week extremes |
| `at_52w_high` / `at_52w_low` | BOOLEAN | Within 2% of 52-week extreme |
| `at_52w_pos` | VARCHAR | near\_low / neutral / near\_high |
| `has_price_anomaly` | BOOLEAN | IQR outlier on daily log-return |
| `has_volume_anomaly` | BOOLEAN | Z-score > 2.5 on daily volume |
| `sector_advance_ratio` | DOUBLE | Fraction of sector advancing (0–1) |
| `sector_ok` | BOOLEAN | sector\_advance\_ratio ≥ 0.50 |
| `session_vwap` | DOUBLE | Intraday VWAP from tick stream (NULL if no ticks) |

---

## Part 8 — Query Examples

```sql
-- Today's BUY signals ranked by certainty
SELECT symbol, company_name, sector, close, signal_cf, state_id, why_signal
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
  AND signal = 'BUY'
ORDER BY signal_cf DESC;

-- Which decision table rule fired for each symbol today?
SELECT symbol, rating, vol_level, at_52w_pos,
       has_price_anomaly, sector_ok, state_id, signal, signal_cf
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
ORDER BY signal_cf DESC;

-- Buy/Strong Buy stocks that got blocked — which gate fired?
SELECT symbol, rating, signal_score, vol_level, at_52w_pos,
       has_price_anomaly, sector_ok, state_id, signal_cf, why_not_buy
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
  AND signal = 'HOLD'
  AND rating IN ('Buy', 'Strong Buy')
ORDER BY signal_cf DESC;

-- Distribution of rules fired today — which states are most common?
SELECT state_id, signal, confidence, COUNT(*) AS symbols_matched
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
GROUP BY state_id, signal, confidence
ORDER BY symbols_matched DESC;

-- Full history for a specific symbol — what changed over time?
SELECT date, signal, state_id, signal_cf, confidence, reasoning_trace
FROM iceberg.decision.decision_signals
WHERE symbol = '2222'
ORDER BY date DESC LIMIT 30;
```

---

## Part 9 — Maintaining the Knowledge Base

To change a signal, adjust a CF value, or add a new rule:

1. **Edit** `dbt/seeds/decision_table.csv`
2. **Run:**
```bash
docker exec dbt dbt seed --select decision_table --project-dir /usr/dbt --profiles-dir /root/.dbt
docker exec dbt dbt run  --select decision_signals  --project-dir /usr/dbt --profiles-dir /root/.dbt
```

No SQL knowledge required. The domain expert edits the CSV only.

**CF assignment guide:**

| Scenario | Recommended `signal_cf` |
|---|---|
| Perfect BUY: strong rating + low vol + near 52W low | +0.90 to +0.95 |
| Standard BUY: good conditions, neutral 52W position | +0.70 to +0.88 |
| Moderate BUY: near resistance or slightly elevated vol | +0.50 to +0.65 |
| BUY blocked but positive technical lean | +0.10 to +0.28 |
| Gate fires, CF turns slightly negative | −0.10 to −0.30 |
| Standard SELL | −0.68 to −0.75 |
| SELL with potential support (near 52W low) | −0.50 to −0.60 |
| SELL confirmed by anomaly or sector decline | −0.80 to −0.88 |
| Maximum SELL: Strong Sell + anomaly | −0.90 to −0.95 |

**Adding a new rule:** Add a row with a new `state_id` (S37+) and set
`specificity_score` to the count of non-`any` conditions. State IDs S01–S36 are
reserved — new rows with higher IDs will have lower priority than existing rows of
equal specificity.

---

## Limitations & Caveats

- **Not financial advice.** Signals are generated by deterministic SQL rules on historical prices. They do not account for fundamentals, news, earnings, or geopolitical events.

- **Shallow knowledge (§6.6).** The system encodes empirical price patterns using IF-THEN rules. It has no causal model of *why* prices move. Deep knowledge — sector dynamics, macroeconomic causality, earnings cycles — is outside the current scope.

- **Simulation data.** Outside Tadawul market hours (Sun–Thu 10:00–15:00 Riyadh time), the Kafka producer generates random-walk ticks. Signals derived from simulated ticks are for pipeline testing only.

- **VWAP gaps.** `session_vwap` is NULL for days with no intraday tick data (batch-only historical loads).

- **CBR requires accumulated history.** `decision_validation` returns empty or sparse results until the Airflow CBR DAG has run for several weeks to accumulate WIN/LOSS outcomes in `decision_case_outcomes`.

- **No look-ahead bias.** All indicators use only information available at market close on the signal date. The system is designed for real-time deployment without data leakage.
