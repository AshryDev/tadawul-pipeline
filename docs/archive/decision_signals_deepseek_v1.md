# Decision Layer — Signal Methodology

> **What this document is:** A complete, explainable decision system that combines six technical models with a curated knowledge base of 36 production rules to produce **BUY / SELL / HOLD** signals with formal certainty factors (−1.0 to +1.0) for every Tadawul symbol on every trading day.

---

## Table of Contents

1. [Conceptual Model: Propose → Protect → Validate](#conceptual-model-propose--protect--validate)
2. [System Architecture](#system-architecture)
3. [Why a Decision Layer?](#why-a-decision-layer)
4. [Part 1 — The Six Gold Layer Models (Inputs)](#part-1--the-six-gold-layer-models-inputs)
   - 1.1 `gold_technical_rating` — The Propose Component
   - 1.2 `gold_volatility_index` — Protect (Risk Gate)
   - 1.3 `gold_anomaly_flags` — Protect (Outlier Gate)
   - 1.4 `gold_52w_levels` — Validate (Position Context)
   - 1.5 `gold_sector_performance` — Validate (Market Context)
   - 1.6 `gold_intraday_vwap` — Execution Context (Optional)
5. [Part 2 — The Decision Table (Knowledge Base)](#part-2--the-decision-table-knowledge-base)
   - 2.1 Why a Decision Table Instead of Pure ML?
   - 2.2 Certainty Factors: Quantifying Judgment
   - 2.3 The 36 Production Rules
   - 2.4 Specificity‑Based Rule Matching
   - 2.5 Visualising the Rule Selection Flow
6. [Part 3 — Anomaly SELL Override (A Special Rule)](#part-3--anomaly-sell-override-a-special-rule)
7. [Part 4 — How a Signal Is Formed: Step-by-Step](#part-4--how-a-signal-is-formed-step-by-step)
   - Step 1: Gather Gold Layer Outputs
   - Step 2: Classify Continuous Values into Discrete Decision Keys
   - Step 3: Look Up the Decision Table (Specificity-Ordered Matching)
   - Step 4: Apply Anomaly SELL Override (if Condition Matches)
   - Step 5: Assemble Explanation and Output
8. [Part 5 — Realistic End-to-End Examples](#part-5--realistic-end-to-end-examples)
   - Example A: Strong Buy + Near 52‑Week Low → BUY (CF=0.92)
   - Example B: Buy Rating Blocked by High Volatility → HOLD (CF=0.18)
   - Example C: Neutral Rating + Price Anomaly + Negative Score → SELL (CF=-0.50)
9. [Part 6 — The Explanation Facility (Explainable AI)](#part-6--the-explanation-facility-explainable-ai)
   - `why_signal` — Dynamic Justification
   - `why_not_buy` / `why_not_sell` — Tracing Blockers
   - `reasoning_trace` — Numbered How‑Trace
10. [Part 7 — Output Columns Reference](#part-7--output-columns-reference)
11. [Part 8 — Query Examples](#part-8--query-examples)
12. [Part 9 — Maintaining the Knowledge Base](#part-9--maintaining-the-knowledge-base)
13. [Limitations & Caveats](#limitations--caveats)

---

## Conceptual Model: Propose → Protect → Validate

Before diving into implementation, understand the **three‑layer reasoning** that governs every signal. This model makes the decision logic transparent, teachable, and auditable.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PROPOSE (What to do)                        │
│  gold_technical_rating → rating (Strong Buy → Strong Sell)          │
│                                                                     │
│  The technical indicators provide a base directional bias.          │
│  This is the "raw signal" before risk and context are applied.      │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         PROTECT (Should we act?)                    │
│  gold_volatility_index → vol_level (high/extreme blocks BUY)        │
│  gold_anomaly_flags    → has_anomaly (overrides to HOLD/SELL)       │
│                                                                     │
│  Risk gates that can cancel or reverse the proposal.                │
│  High volatility means entries are unsafe; anomalies suggest        │
│  non‑normal behaviour that may invalidate technical signals.        │
│  If a protect condition fires, the signal is downgraded or blocked. │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │ (if protect passes or is overridden)
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        VALIDATE (How strongly?)                     │
│  gold_sector_performance → sector_ok (headwind/tailwind)            │
│  gold_52w_levels        → at_52w_pos (support/resistance)           │
│                                                                     │
│  These factors do not create a signal on their own, but they        │
│  adjust the certainty factor (CF) up or down.                       │
│  A strong sector or a 52‑week low increases BUY conviction;         │
│  a weak sector or 52‑week high decreases it (or blocks BUY          │
│  in some lower‑strength proposals).                                 │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  ▼
                        final SIGNAL + CF
```

**How this maps to the decision table:**  
The table’s 36 rules are organised exactly along these three lines:

| Role | Decision table condition | Example states |
|------|------------------------|----------------|
| **Propose** | `rating_group` (Strong Buy, Buy, Neutral, Sell, Strong Sell) | S01‑S07 (Strong Buy), S08‑S14 (Buy), etc. |
| **Protect** | `vol_level = high/extreme` | S01, S02, S08, S09, S15, S16, S22, S23, S29, S30 |
| **Protect** | `has_anomaly = true` | S03, S10, S17, S24, S31 |
| **Validate** | `sector_ok = false` | S04, S11, S18, S25, S32 |
| **Validate** | `at_52w_pos = near_low` or `near_high` | S05, S06, S12, S13, S19, S20, S26, S27, S33, S34 |
| **Default** | (no special conditions) | S07, S14, S21, S28, S35 |

**Why this matters:** The conceptual model explains *why* a rule exists. A reviewer can see immediately that volatility gates are **protect** rules — they exist to prevent unsafe entries, not because volatility predicts direction. Sector and 52‑week levels are **validate** rules — they fine‑tune conviction based on context.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER (6 feature models)                       │
│  Each model runs daily and produces a table of (symbol, date, features)    │
├─────────────────────────────────────────────────────────────────────────────┤
│  gold_technical_rating  → signal_score, rating, RSI, SMAs, BB              │
│  gold_volatility_index  → annualized_vol, vol_level                        │
│  gold_anomaly_flags     → has_price_anomaly, has_volume_anomaly            │
│  gold_52w_levels        → at_52w_high, at_52w_low, pct_from_high/low       │
│  gold_sector_performance→ sector_advance_ratio, sector_ok                  │
│  gold_intraday_vwap     → session_vwap                                     │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      │ All six read directly in SQL JOIN
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    KNOWLEDGE BASE: dbt/seeds/decision_table.csv            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  36 production rules (IF condition → THEN signal, CF)              │   │
│  │  Example: IF rating='Strong Buy' AND has_anomaly=false AND         │   │
│  │            sector_ok=true AND vol='normal' AND at_52w='near_low'   │   │
│  │          THEN BUY, CF=+0.92                                        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │ Specificity‑ordered lookup (most specific rule wins)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    decision_signals (single unified output)                │
│  • signal (BUY/SELL/HOLD)                                                  │
│  • signal_cf (−1.0 to +1.0) & confidence (HIGH/MEDIUM/LOW)                │
│  • state_id (which rule fired)                                            │
│  • why_signal, why_not_buy, reasoning_trace (XAI)                         │
│  • All gold layer context columns (for auditing)                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Knowledge representation techniques used (Lecture 4):**

| Technique | Where | Purpose |
|-----------|-------|---------|
| **Decision table** | `decision_table.csv` — 36 IF‑THEN rules with CF | Explicit, editable knowledge base |
| **Decision tree** | `signal_score → rating` — 5‑branch CASE in SQL | Compresses 8 indicator votes into a categorical label |
| **Production rules** | Each row of the decision table | Modular, independent rules |
| **Certainty Factors** | `signal_cf` column (−1.0 to +1.0) | Quantifies expert uncertainty |

---

## Why a Decision Layer?

The six gold layer models each provide a narrow slice of information: technical rating, volatility, anomalies, 52‑week proximity, sector strength, and VWAP. None alone is sufficient for a trading decision. The decision layer **integrates** these slices using an explicit knowledge base — a decision table — that encodes domain expertise about how these factors interact.

**Why this matters:** A pure machine learning approach would require thousands of labeled examples and would be a black box. With the decision table, every signal is **traceable** to a specific rule, every certainty factor is **pre‑assigned by an expert**, and you can **edit** the logic without retraining models. This is knowledge‑based systems (KBS) in production.

---

## Part 1 — The Six Gold Layer Models (Inputs)

Each gold model is a separate dbt model that transforms raw or silver data into a feature. The decision layer **does not recompute** anything — it reads the ready‑made outputs.

### 1.1 `gold_technical_rating` — The Propose Component

**Role in conceptual model:** **PROPOSE** — provides the base directional bias.

**What it does:** Aggregates 8 technical indicators (price vs SMAs, moving average cross, RSI, Bollinger Bands, MACD proxy). Each indicator votes **+1 (buy)**, **0 (neutral)**, or **−1 (sell)**. The net score (−8 to +8) is mapped to a rating.

| Indicator | Buy vote (+1) | Sell vote (−1) | Why this matters |
|-----------|---------------|----------------|------------------|
| Price vs SMA10 | close > SMA10 | close < SMA10 | Short‑term trend alignment |
| Price vs SMA20 | close > SMA20 | close < SMA20 | Medium‑term trend alignment |
| Price vs SMA50 | close > SMA50 | close < SMA50 | Intermediate trend (often used by institutional traders) |
| Price vs SMA200 | close > SMA200 | close < SMA200 | Long‑term bull/bear market filter |
| MA cross (SMA10 > SMA20) | golden cross | death cross | Momentum shift signal |
| RSI(14) | RSI < 30 (oversold) | RSI > 70 (overbought) | Mean reversion opportunity / exhaustion |
| Bollinger Bands | close < lower band | close > upper band | Extreme price relative to volatility |
| MACD proxy (SMA12 > SMA26) | bullish cross | bearish cross | Trend strength / direction |

**Score → Rating mapping:**

```
signal_score:  -8 -7 -6 -5 -4 -3 -2 -1  0 +1 +2 +3 +4 +5 +6 +7 +8
                └─── Strong Sell ──┘└─ Sell ─┘└─ Neutral ─┘└─ Buy ─┘└─Strong Buy─┘
                      ≤ -5            ≤ -3        else        ≥ +3       ≥ +5
```

**Why this model exists as the “propose” component:** Technical ratings are a condensed summary of price action. They are the **primary directional input** — a Strong Buy rating already implies most indicators are bullish. The decision layer then modifies that base conviction based on risk factors (protect) and context (validate).

**Key columns used in decision:**

| Column | Role |
|--------|------|
| `signal_score` | Numerical intensity (used in anomaly SELL override) |
| `rating` | **Primary decision key** — one of 5 categorical values |
| `buy_signals`, `sell_signals` | Used in explanations (e.g., “6/8 indicators bullish”) |
| `rsi14`, `sma50`, `sma200`, `bb_lower`, `bb_upper` | Context for human explanations |

---

### 1.2 `gold_volatility_index` — Protect (Risk Gate)

**Role in conceptual model:** **PROTECT** — gates or blocks the proposal when risk is too high.

**What it does:** Computes 20‑day rolling volatility of daily log‑returns, then annualises it.

$$r_t = \ln(\text{close}_t / \text{close}_{t-1}), \quad \sigma_{20d} = \text{std dev}(r_{t-19}\ldots r_t), \quad \text{annualised vol} = \sigma_{20d} \times \sqrt{252}$$

**Why this matters for protection:** Volatility is **risk**. High volatility means larger daily swings — entries are riskier because a stop loss might be hit by normal noise. Conversely, low volatility provides a calm environment where technical signals are more reliable. The decision table uses volatility as a **gate**: if volatility is high or extreme, even a Strong Buy rating may be downgraded to HOLD. This is pure risk management, not a directional predictor.

**Volatility classification (decision key):**

| `annualized_vol` | `vol_level` | Daily swing (approx) | Protection effect |
|------------------|-------------|----------------------|-------------------|
| > 80% | `extreme` | > 5% | Blocks BUY entirely, may even turn to SELL |
| > 50% | `high` | > 3% | Reduces BUY conviction, often forces HOLD |
| 20% – 50% | `normal` | 1.2% – 3% | Standard environment — allows BUY/SELL normally |
| ≤ 20% | `low` | < 1.2% | Highest confidence for directional trades |

**Why this matters (real‑world example):** A stock with 80% annualised volatility can gap 5% overnight. Entering a position based on a technical signal is like trying to park on a trampoline. The decision table’s volatility gates protect against this.

---

### 1.3 `gold_anomaly_flags` — Protect (Outlier Gate)

**Role in conceptual model:** **PROTECT** — overrides the proposal when price behaviour is abnormal.

**Two independent anomaly detectors:**

1. **Volume Z‑score:**  
   $$z = \frac{V_t - \bar{V}_{30d}}{\sigma_{V,30d}}$$  
   Flagged when `|z| > 2.5` (unusually high or low volume).

2. **Price IQR (Tukey fence on 90‑day log‑returns):**  
   $$\text{lower} = Q_1 - 1.5 \times IQR,\quad \text{upper} = Q_3 + 1.5 \times IQR$$  
   Flagged when daily log‑return lies outside `[lower, upper]` (extreme price move).

**Why this matters for protection:** Anomalies represent **non‑normal market conditions**. A price anomaly (e.g., a 10% drop on no news) could be a flash crash or a data error — but if accompanied by a negative technical score, it may indicate a genuine breakdown. The decision table treats `has_price_anomaly = true` as a **strong caution flag**, often overriding a BUY signal into HOLD or SELL. Volume anomalies suggest unusual participation (smart money accumulation or distribution) but are used only for explanation.

**Key columns used:**

| Column | Role |
|--------|------|
| `has_price_anomaly` | Decision key (blocks BUY, amplifies SELL) |
| `has_volume_anomaly` | Explanation only |

---

### 1.4 `gold_52w_levels` — Validate (Position Context)

**Role in conceptual model:** **VALIDATE** — adjusts conviction based on support/resistance proximity.

**What it does:** Computes rolling 52‑week (252 trading days) high and low from daily highs and lows. Then flags when the closing price is within 2% of either extreme.

$$ \text{at\_52w\_high} = (\text{close} \ge 0.98 \times \text{high}_{52w}) $$  
$$ \text{at\_52w\_low} = (\text{close} \le 1.02 \times \text{low}_{52w}) $$

**Why this matters for validation:** 52‑week highs and lows are **psychological anchors**. Near a 52‑week low, mean reversion traders expect a bounce — this **increases BUY conviction** (see S05, CF=+0.92). Near a 52‑week high, resistance may cap upside, so BUY conviction is slightly reduced (S06, CF=+0.72). Conversely, for SELL signals, being near a 52‑week high increases breakdown risk (S27, CF=−0.82), while near a 52‑week low reduces SELL conviction (S26, CF=−0.52).

**Decision key mapping:**

| Condition | `at_52w_pos` | Interpretation | Validation effect |
|-----------|--------------|----------------|-------------------|
| `at_52w_low = true` | `near_low` | Potential bounce zone — bullish tilt | Increases BUY CF, decreases SELL CF |
| `at_52w_high = true` | `near_high` | Resistance zone — bearish tilt | Decreases BUY CF, increases SELL CF |
| otherwise | `neutral` | No extreme proximity | No adjustment |

---

### 1.5 `gold_sector_performance` — Validate (Market Context)

**Role in conceptual model:** **VALIDATE** — confirms or contradicts the proposal based on broad market strength.

**What it does:** For each sector and day, computes the **advance ratio**:

$$ \text{advance\_ratio} = \frac{\text{# stocks with positive daily return in sector}}{\text{total # stocks in sector}} $$

**Why this matters for validation:** Even the best company can be dragged down by a weak sector. Conversely, a rising tide lifts most boats. The decision table uses `sector_ok = (advance_ratio ≥ 0.5)` as a **validating condition**: if less than half the sector is advancing, BUY signals are blocked (S04, S11, S18) or conviction is reduced. For SELL signals, a weak sector (`sector_ok = false`) actually **strengthens** the SELL conviction (S25, CF=−0.75 vs S22, CF=−0.68) — this is validation: the sector confirms the bearish bias.

**Example:** You see a stock with a Buy rating. If 80% of its sector is up, the signal is more reliable (validation passes). If only 30% are up, the stock is fighting headwinds — the decision table will block the BUY (or downgrade it to HOLD).

---

### 1.6 `gold_intraday_vwap` — Execution Context (Optional)

**Role in conceptual model:** **Not a decision key** — provided for informational purposes.

**What it does:** Computes session Volume‑Weighted Average Price from tick data:

$$ \text{VWAP} = \frac{\sum_t \text{price}_t \times \text{volume}_t}{\sum_t \text{volume}_t} $$

**Why this matters:** VWAP represents the **average execution price** for large orders. It is not used as a decision key (no rule depends on it) but is **passed through** to the output for downstream systems. A human trader may use it to decide whether to buy at market or wait for a pullback to VWAP.

**Data availability:** `NULL` on days with no tick data (e.g., batch‑only historical loads). During market hours (Sun–Thu 10:00–15:00 Riyadh time), the Kafka tick stream provides updates.

---

## Part 2 — The Decision Table (Knowledge Base)

### 2.1 Why a Decision Table Instead of Pure ML?

**Design decision:** The system uses an **explicit decision table** (36 rows) rather than training a classifier (e.g., random forest, XGBoost).

**Reasons:**

| Aspect | Decision Table (KBS) | Pure ML |
|--------|----------------------|---------|
| **Explainability** | Every signal traces to a specific rule (e.g., S05) | Black box — feature importance is global, not per‑instance |
| **Editable by domain expert** | Edit CSV, re‑run dbt seed | Requires retraining, data labelling, and ML engineering |
| **Certainty factors** | Explicitly assigned based on expert judgment | Would require probabilistic calibration |
| **Performance with sparse data** | Works perfectly with little historical data | Needs thousands of labelled examples |
| **Guaranteed behaviour** | No unintended interactions (rules are independent) | Model may learn spurious correlations |
| **Tie‑breaking logic** | Clear specificity ordering | Implicit, hard to debug |

**Trade‑off:** The decision table cannot learn complex non‑linear interactions beyond the 5‑dimensional space. But for this domain — combining a handful of categorical features — a well‑designed decision table is **interpretable, editable, and sufficient**.

### 2.2 Certainty Factors: Quantifying Judgment

**What is a certainty factor (CF)?** A number between −1.0 and +1.0 that represents the **strength of belief** in a signal, combining both direction and confidence. This is based on Lecture 4 §4.10 (Certainty Factors).

**CF scale and interpretation:**

```
-1.0          -0.65      -0.40          0          +0.40      +0.65          +1.0
  │             │          │            │            │          │             │
  └─ Strong Sell─┘    └─ Medium ─┘    HOLD/    └─ Medium ─┘    └─ Strong Buy─┘
  (maximum conviction)   Sell         Low           Buy      (maximum conviction)
                                     conviction
```

| CF absolute value | `confidence` | Meaning |
|-------------------|--------------|---------|
| ≥ 0.65 | **HIGH** | Strong signal; high conviction; suitable for position sizing |
| 0.40 – 0.64 | **MEDIUM** | Notable signal but with some uncertainty; consider risk management |
| < 0.40 | **LOW** | Weak conviction; likely HOLD; avoid trading |

**Why CF instead of binary BUY/SELL/HOLD?** CF allows **ranking** signals (e.g., two BUY signals: CF=0.92 is stronger than CF=0.55). It also provides a smooth transition — a rule that nearly blocks BUY might have a small positive CF (e.g., +0.18) rather than forcing HOLD with 0.00.

### 2.3 The 36 Production Rules

The decision table covers **5 rating groups** × **7 scenarios** + 1 fallback. Each rule has the form:

```
IF rating_group = X
AND has_anomaly = Y   (true / false / any)
AND sector_ok = Z     (true / false / any)
AND vol_level = V     (low / normal / high / extreme / any)
AND at_52w_pos = P    (near_low / neutral / near_high / any)
THEN signal = S, signal_cf = CF
```

**The 7 scenarios per rating group (with state ID ranges):**

| Row # | Scenario | State IDs | Role | Purpose |
|--------|----------|-----------|------|---------|
| 1 | High volatility (vol = `high`) | S01 (Strong Buy), S08 (Buy), S15 (Neutral), S22 (Sell), S29 (Strong Sell) | **Protect** | If vol is high (but not extreme), BUY conviction is reduced; SELL conviction may be slightly reduced too. |
| 2 | Extreme volatility (vol = `extreme`) | S02, S09, S16, S23, S30 | **Protect** | Extreme vol blocks BUY entirely; SELL may be weakened because panic moves reverse quickly. |
| 3 | Price anomaly (`has_anomaly = true`) | S03, S10, S17, S24, S31 | **Protect** | Anomaly overrides rating — usually forces HOLD or SELL; BUY is not allowed. |
| 4 | Sector headwind (`sector_ok = false`) | S04, S11, S18, S25, S32 | **Validate** | Weak sector blocks BUY (or lowers CF); for SELL, weak sector strengthens conviction. |
| 5 | Near 52‑week low | S05, S12, S19, S26, S33 | **Validate** | Mean reversion opportunity: increases BUY CF, decreases SELL CF. |
| 6 | Near 52‑week high | S06, S13, S20, S27, S34 | **Validate** | Resistance: slightly reduces BUY CF, increases SELL CF. |
| 7 | Default (no special conditions) | S07, S14, S21, S28, S35 | — | Baseline signal when none of the above gates/differentiators apply. |

**Fallback (S36):** When no rule matches (should never happen with `any` catch‑all), HOLD, CF=0.00.

**Full table (abridged — see `decision_table.csv` for exact values):**

| State | Rating | Anomaly | Sector | Vol | 52W | Signal | CF | Role |
|-------|--------|---------|--------|-----|-----|--------|----|------|
| S01 | Strong Buy | false | true | high | any | HOLD | +0.22 | Protect (vol) |
| S02 | Strong Buy | false | true | extreme | any | HOLD | −0.12 | Protect (vol) |
| S03 | Strong Buy | true | any | any | any | HOLD | −0.20 | Protect (anomaly) |
| S04 | Strong Buy | false | false | any | any | HOLD | +0.12 | Validate (sector) |
| S05 | Strong Buy | false | true | any | near_low | **BUY** | **+0.92** | Validate (52W) |
| S06 | Strong Buy | false | true | any | near_high | **BUY** | +0.72 | Validate (52W) |
| S07 | Strong Buy | false | true | any | neutral | **BUY** | +0.88 | Default |
| ... | ... | ... | ... | ... | ... | ... | ... | ... |
| S24 | Sell | true | any | any | any | **SELL** | **−0.85** | Protect (anomaly) |
| ... | ... | ... | ... | ... | ... | ... | ... | ... |
| S36 | any | any | any | any | any | HOLD | 0.00 | Fallback |

**Why these specific CF values?** They encode expert judgment based on the propose‑protect‑validate framework:

- **+0.92 (S05):** Strong Buy (propose) → protect passes (no anomaly, good sector, low/normal vol) → validate adds near 52‑week low (bullish) → maximum BUY.
- **−0.95 (S31):** Strong Sell (propose) → protect anomaly true → validate irrelevant → maximum SELL.
- **+0.22 (S01):** Strong Buy (propose) → protect high vol blocks full BUY but leaves a weak positive lean because technicals are still strong.

### 2.4 Specificity‑Based Rule Matching

**The problem:** Multiple rules may match the same (rating, anomaly, sector, vol, 52W) combination. For example:
- `Strong Buy, no anomaly, sector_ok, high vol, near_low` matches:
  - S01 (high vol gate) — conditions: rating, anomaly=false, sector_ok=true, vol=high, 52W=any
  - S05 (near_low differentiator) — conditions: rating, anomaly=false, sector_ok=true, vol=any, 52W=near_low

Both have **specificity** = 4 (four non‑`any` conditions). Which one should win?

**Resolution:** Compute `specificity_score` = count of non‑`any` conditions.  
Then pick the **highest specificity**. If tie, pick the **lower `state_id`**.

**Why lower state_id wins in a tie?** Within each rating group, gate rows (S01–S04) have lower state IDs than differentiator rows (S05–S07). This means **volatility or anomaly gates take precedence** over 52‑week proximity. That is intentional: risk management (protect) overrides opportunity (validate).

**Specificity calculation examples:**

| Rule | Rating | Anomaly | Sector | Vol | 52W | Specificity |
|------|--------|---------|--------|-----|-----|-------------|
| S01 | Strong Buy | false | true | high | any | 4 |
| S05 | Strong Buy | false | true | any | near_low | 4 |
| S07 | Strong Buy | false | true | any | any | 3 |
| S03 | Strong Buy | true | any | any | any | 2 |
| S36 | any | any | any | any | any | 0 |

**Tie‑breaking examples:**

| Input | Matching states | Specificities | Winner | Why |
|-------|----------------|---------------|--------|-----|
| Strong Buy + no anomaly + sector_ok + high vol + near_low | S01 (vol=high), S05 (52W=near_low) | 4, 4 | S01 | lower state_id → protect (vol) wins over validate (52W) |
| Strong Buy + no anomaly + sector_ok + normal vol + near_low | S05 only (S01 vol=high doesn’t match) | 4 | S05 | only match |
| Strong Buy + anomaly + near_low | S03 (anomaly=true) → matches; S05 requires anomaly=false → no match | 2 | S03 | protect (anomaly) wins |

### 2.5 Visualising the Rule Selection Flow

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │              INPUT (5 decision keys)                        │
                    │  rating, has_anomaly, sector_ok, vol_level, at_52w_pos     │
                    └─────────────────────────────────┬───────────────────────────┘
                                                      │
                                                      ▼
                    ┌─────────────────────────────────────────────────────────────┐
                    │           STEP 1: Find all matching rules                  │
                    │  (non-'any' conditions must equal input)                   │
                    └─────────────────────────────────┬───────────────────────────┘
                                                      │
                                                      ▼
                    ┌─────────────────────────────────────────────────────────────┐
                    │           STEP 2: Compute specificity_score                │
                    │  = count of non-'any' conditions                           │
                    └─────────────────────────────────┬───────────────────────────┘
                                                      │
                                                      ▼
                    ┌─────────────────────────────────────────────────────────────┐
                    │           STEP 3: Select winner                            │
                    │  highest specificity; tie → lowest state_id                │
                    └─────────────────────────────────┬───────────────────────────┘
                                                      │
                                                      ▼
                    ┌─────────────────────────────────────────────────────────────┐
                    │              OUTPUT: signal, CF, state_id                  │
                    └─────────────────────────────────────────────────────────────┘
```

**Priority hierarchy (implicit in specificity + state_id ordering):**

1. **Most specific rules** (specificity 5) — rare.
2. **Protect rules** (volatility gates: vol=high/extreme; anomaly gates) — specificity 4, low state_id.
3. **Validate rules** (sector_ok=false, near_low, near_high) — also specificity 4, but higher state_id.
4. **Default rules** (specificity 3 or less).
5. **Fallback** (specificity 0).

This ensures **safety first** (protect), then **opportunity** (validate).

---

## Part 3 — Anomaly SELL Override (A Special Rule)

**Why this exists:** The decision table handles anomaly primarily as a **blocker for BUY** (e.g., S03, S10, S17 give HOLD or weak negative CF when anomaly is true). However, the original system logic defined a specific SELL trigger: **Neutral rating + price anomaly + negative signal_score** should produce a SELL, not HOLD.

This scenario is **not fully captured** in the decision table because the neutral group’s anomaly rule (S17) outputs HOLD, CF=−0.20. The override rule fires **after** the table lookup to correct this.

**The rule:**

```
IF  has_price_anomaly = TRUE
AND rating = 'Neutral'
AND signal_score < 0
THEN  signal = SELL,  signal_cf = −0.50,  confidence = MEDIUM
```

**Why CF=−0.50?** This is a medium‑conviction SELL. It is not as strong as a Sell‑rated stock with anomaly (S24, CF=−0.85) because the technical rating is Neutral, not Sell. But the combination of a negative score and a price anomaly suggests a genuine breakdown.

**Example:** A stock with rating Neutral (score = −1), no other red flags, but a 8% drop flagged as price anomaly → override fires → SELL, CF=−0.50.

---

## Part 4 — How a Signal Is Formed: Step-by-Step

This is the end‑to‑end inference flow for one (symbol, date).

### Step 1: Gather Gold Layer Outputs

Join the six gold models on `symbol` and `date`:

```sql
SELECT
  t.symbol, t.date, t.close,
  gr.rating, gr.signal_score, gr.buy_signals, gr.sell_signals, gr.rsi14,
  gv.annualized_vol,
  ga.has_price_anomaly, ga.has_volume_anomaly,
  g52.at_52w_high, g52.at_52w_low,
  gs.advance_ratio,
  gvw.session_vwap
FROM ...
```

### Step 2: Classify Continuous Values into Discrete Decision Keys

| Raw column | Decision key | Mapping logic |
|------------|--------------|----------------|
| `annualized_vol` | `vol_level` | >0.8 → 'extreme', >0.5 → 'high', >0.2 → 'normal', else 'low' |
| `at_52w_high`, `at_52w_low` | `at_52w_pos` | true/true? (shouldn’t happen) → neutral; `at_52w_low`=true → 'near_low'; `at_52w_high`=true → 'near_high'; else 'neutral' |
| `advance_ratio` | `sector_ok` | ≥0.5 → true, else false |
| `rating` | `rating_group` (already categorical) | as is |
| `has_price_anomaly` | `has_anomaly` | as is (boolean) |

### Step 3: Look Up the Decision Table (Specificity-Ordered Matching)

Using the five decision keys, query the seed table `decision_table`:

```sql
SELECT state_id, signal, signal_cf, specificity_score
FROM decision_table
WHERE (rating_group = input.rating OR rating_group = 'any')
  AND (has_anomaly = input.has_anomaly OR has_anomaly = 'any')
  AND (sector_ok = input.sector_ok OR sector_ok = 'any')
  AND (vol_level = input.vol_level OR vol_level = 'any')
  AND (at_52w_pos = input.at_52w_pos OR at_52w_pos = 'any')
ORDER BY specificity_score DESC, state_id ASC
LIMIT 1
```

### Step 4: Apply Anomaly SELL Override (if Condition Matches)

After the table lookup, check:

```
IF  has_price_anomaly = TRUE
AND rating = 'Neutral'
AND signal_score < 0
THEN  override: signal = 'SELL', signal_cf = -0.50, confidence = 'MEDIUM', state_id = NULL
```

**Why state_id = NULL?** This is not a decision table row; it is a hard‑coded override rule.

### Step 5: Assemble Explanation and Output

- `why_signal` — generate a human‑readable string based on the final signal and conditions.
- `why_not_buy` — if signal ≠ BUY, determine the first blocking condition (priority: anomaly → sector → volatility → rating).
- `why_not_sell` — if signal ≠ SELL, determine blocking condition (if any).
- `reasoning_trace` — numbered list of steps from input classification to final signal.

---

## Part 5 — Realistic End-to-End Examples

### Example A: Strong Buy with Near 52‑Week Low → BUY (CF=0.92)

**Propose:** Strong Buy (score +6)  
**Protect:** No anomaly, vol=normal (25%) → passes  
**Validate:** Sector_ok=true (65% advancing), near 52‑week low (1.5% above) → boosts CF

**Step‑by‑step:**

1. **Inputs classified:** rating=Strong Buy, score=+6/8, vol=normal, 52W=near_low, anomaly=false, sector_ok=true.
2. **Decision table matched:** state=S05 (specificity=4) → BUY, CF=+0.92, confidence=HIGH.
3. **Override not triggered.**
4. **Final signal:** BUY.

**Output:**
```
signal = BUY
signal_cf = 0.92
confidence = HIGH
state_id = S05
why_signal = "BUY (CF=0.92, HIGH): state=S05 — Best setup: Strong Buy rating + clean conditions + near 52W low (mean reversion bounce expected)"
why_not_buy = NULL
reasoning_trace = "1. Inputs classified: rating=Strong Buy, score=6/8, vol=normal, 52W=near_low, anomaly=false, sector_ok=true. 2. Decision table matched: state=S05 (specificity=4). 3. Signal=BUY, CF=0.92 (HIGH)."
```

---

### Example B: Buy Rating Blocked by High Volatility → HOLD (CF=0.18)

**Propose:** Buy (score +3)  
**Protect:** High volatility (62%) → **gate fires**, blocks full BUY  
**Validate:** Sector_ok=true, 52W=neutral — but protect already won.

**Step‑by‑step:**

1. **Inputs:** rating=Buy, score=+3/8, vol=high, 52W=neutral, anomaly=false, sector_ok=true.
2. **Decision table matched:** S08 (high vol gate, specificity=4) → HOLD, CF=+0.18, confidence=LOW.
3. **Override not triggered.**
4. **Final signal:** HOLD.

**Output:**
```
signal = HOLD
signal_cf = 0.18
confidence = LOW
state_id = S08
why_signal = "HOLD (CF=0.18, LOW): state=S08 — High volatility (62% ann.) blocks BUY — position sizing unsafe, but technicals are positive."
why_not_buy = "high volatility (62.4% ann.) — position risk"
reasoning_trace = "1. Inputs: rating=Buy, score=3/8, vol=high, 52W=neutral, anomaly=false, sector_ok=true. 2. Decision table matched: state=S08 (high vol gate, spec=4). 3. Signal=HOLD, CF=0.18 (LOW)."
```

---

### Example C: Neutral Rating + Price Anomaly + Negative Score → SELL (CF=-0.50)

**Propose:** Neutral (score −2) — no strong directional bias.  
**Protect:** Price anomaly = true → table gives HOLD (S17, CF=−0.20).  
**Override condition:** Neutral + anomaly + negative score → triggers SELL override.

**Step‑by‑step:**

1. **Inputs:** rating=Neutral, score=−2/8, vol=normal, 52W=neutral, anomaly=true, sector_ok=false.
2. **Decision table matched:** S17 (anomaly gate, specificity=2) → HOLD, CF=−0.20.
3. **Anomaly SELL override condition met** (Neutral + anomaly + negative score) → final signal=SELL, CF=−0.50, confidence=MEDIUM, state_id=NULL.
4. **Final signal:** SELL.

**Output:**
```
signal = SELL
signal_cf = -0.50
confidence = MEDIUM
state_id = NULL
why_signal = "SELL (CF=-0.50, MEDIUM): Anomaly SELL override — Neutral rating with negative score (-2/8) + price anomaly = breakdown confirmation"
why_not_buy = "Rating is 'Neutral' (score=-2/8) — not eligible for BUY; price anomaly detected"
reasoning_trace = "1. Inputs: rating=Neutral, score=-2/8, vol=normal, 52W=neutral, anomaly=true, sector_ok=false. 2. Decision table matched: state=S17 (anomaly gate) → HOLD, CF=-0.20. 3. Anomaly SELL override condition met → final signal=SELL, CF=-0.50 (MEDIUM)."
```

---

## Part 6 — The Explanation Facility (Explainable AI)

The system implements **explainable AI (XAI)** principles from Lecture 4 (§4.8–4.9): dynamic justification, tracing, and how‑traces. Each output row is accompanied by human‑readable explanations.

### `why_signal` — Dynamic Justification

A short, template‑based string that explains **why** the specific signal was chosen. It adapts to the matched state and override conditions. The explanation references the conceptual model where helpful.

**Template examples:**

| Scenario | Example output |
|----------|----------------|
| S05 (Strong Buy + near 52W low) | `"BUY (CF=0.92, HIGH): state=S05 — Best setup: strong technicals + clean conditions + near 52W low"` |
| S08 (Buy + high volatility) | `"HOLD (CF=0.18, LOW): state=S08 — High volatility blocks BUY — position sizing unsafe"` |
| S24 (Sell + anomaly) | `"SELL (CF=-0.85, HIGH): state=S24 — Sell rating + price anomaly = compounded bearish confirmation"` |
| Override (Neutral + anomaly + negative score) | `"SELL (CF=-0.50, MEDIUM): Anomaly SELL override — Neutral rating with negative score + price anomaly = breakdown confirmation"` |

### `why_not_buy` — First Blocking Condition (Priority Order)

If the final signal is **not BUY**, this column explains the **highest‑priority condition** that prevented a BUY. Priority order (from highest to lowest) follows the protect‑validate hierarchy:

1. **Price anomaly** (`has_price_anomaly = true`) — overrides everything.
2. **Rating not eligible** (Neutral, Sell, Strong Sell) — BUY requires Buy or Strong Buy.
3. **Sector headwind** (`sector_ok = false`) — even with good rating, weak sector blocks BUY.
4. **High or extreme volatility** — risk gate.
5. **Other** (e.g., near 52‑week high reduces confidence, but does not fully block — so not listed here).

**Examples:**

| Final signal | Condition | `why_not_buy` |
|--------------|-----------|----------------|
| HOLD (from S10) | Buy rating + anomaly | `"Price-IQR anomaly detected — spike entry risk (state S10)"` |
| HOLD (from S04) | Strong Buy + sector false | `"Sector advancing 43% < 50% threshold"` |
| HOLD (from S01) | Strong Buy + high vol | `"high volatility (62.4% ann.) — position risk"` |
| SELL (override) | Neutral rating | `"Rating is 'Neutral' (score=-2/8) — not eligible for BUY; price anomaly detected"` |

### `why_not_sell` — Why SELL Was Not Triggered (When signal ≠ SELL)

Similar to `why_not_buy` but for sell signals. Priority:
1. Price anomaly? (if anomaly alone would trigger SELL? Actually anomaly alone doesn't trigger SELL except through override; so for HOLD/BUY, we explain why a SELL didn't happen)
2. Rating not bearish (Buy, Strong Buy)
3. Sector strength
4. Low volatility (calm environment)
5. Near 52‑week low (support)

**Example:** A Strong Buy signal → `why_not_sell = "Rating is 'Strong Buy' — strongly bullish"`

### `reasoning_trace` — Numbered How‑Trace

A step‑by‑step reconstruction of the inference chain, useful for debugging and auditing.

**Format:**
```
1. Inputs classified: rating=X, score=Y/8, vol=Z, 52W=W, anomaly=A, sector_ok=B.
2. Decision table matched: state=Sxx (specificity=N).
3. [If override] Anomaly SELL override condition met → final signal=SELL, CF=... 
   [Else] Signal=..., CF=..., confidence=... .
```

**Why this supports XAI:** A human (or auditor) can **replay** the decision exactly as the system made it. This satisfies the “How” question (Lecture 4 §4.9) and is essential for regulatory or compliance review.

---

## Part 7 — Output Columns Reference

Full list of columns produced by `decision_signals`:

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | VARCHAR | Tadawul stock code |
| `company_name` | VARCHAR | Full name |
| `sector` | VARCHAR | Sector name |
| `date` | DATE | Trading date |
| `close` | DOUBLE | Closing price (SAR) |
| `signal` | VARCHAR | **BUY / SELL / HOLD** |
| `signal_cf` | DOUBLE | Certainty factor −1.0 to +1.0 |
| `confidence` | VARCHAR | HIGH (≥0.65) / MEDIUM (≥0.40) / LOW (<0.40) |
| `state_id` | VARCHAR | Matched decision table row (e.g., S05) — NULL for override |
| `why_signal` | VARCHAR | Dynamic explanation of this signal |
| `why_not_buy` | VARCHAR | First blocking condition (NULL if signal = BUY) |
| `why_not_sell` | VARCHAR | Why SELL not triggered (NULL if signal = SELL) |
| `reasoning_trace` | VARCHAR | Numbered inference trace |
| `rating` | VARCHAR | Strong Buy / Buy / Neutral / Sell / Strong Sell |
| `signal_score` | INT | Net indicator vote (−8 to +8) |
| `buy_signals` | INT | Number of buy votes (0–8) |
| `sell_signals` | INT | Number of sell votes (0–8) |
| `rsi14` | DOUBLE | 14-day RSI |
| `sma50`, `sma200` | DOUBLE | Simple moving averages |
| `bb_lower`, `bb_upper` | DOUBLE | Bollinger Band boundaries |
| `annualized_vol` | DOUBLE | 20‑day volatility annualised |
| `vol_level` | VARCHAR | low / normal / high / extreme |
| `high_52w`, `low_52w` | DOUBLE | 52‑week extremes |
| `pct_from_high`, `pct_from_low` | DOUBLE | % distance from extremes |
| `at_52w_high`, `at_52w_low` | BOOLEAN | Within 2% of extreme |
| `at_52w_pos` | VARCHAR | near_low / neutral / near_high |
| `has_price_anomaly` | BOOLEAN | IQR outlier on daily return |
| `has_volume_anomaly` | BOOLEAN | Volume Z‑score > 2.5 |
| `sector_advance_ratio` | DOUBLE | 0–1 fraction of sector advancing |
| `sector_ok` | BOOLEAN | advance_ratio ≥ 0.5 |
| `session_vwap` | DOUBLE | Intraday VWAP (may be NULL) |

---

## Part 8 — Query Examples

```sql
-- Today's strongest BUY signals
SELECT symbol, company_name, close, signal_cf, state_id, why_signal
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
  AND signal = 'BUY'
ORDER BY signal_cf DESC;

-- Which decision table row matched each symbol? (audit)
SELECT symbol, rating, vol_level, at_52w_pos, has_price_anomaly, sector_ok, state_id, signal, signal_cf
FROM iceberg.decision.decision_signals
WHERE date = CURRENT_DATE
ORDER BY signal_cf DESC;

-- Why were some Buy-rated stocks blocked?
SELECT symbol, rating, signal_score, vol_level, at_52w_pos, has_price_anomaly, sector_ok, state_id, why_not_buy
FROM iceberg.decision.decision_signals
WHERE date = CURRENT_DATE
  AND signal = 'HOLD'
  AND rating IN ('Buy', 'Strong Buy')
ORDER BY signal_cf DESC;

-- Distribution of matched states today
SELECT state_id, signal, confidence, COUNT(*) AS symbols
FROM iceberg.decision.decision_signals
WHERE date = CURRENT_DATE
GROUP BY state_id, signal, confidence
ORDER BY symbols DESC;

-- Full trace for a specific symbol over time
SELECT date, signal, state_id, signal_cf, confidence, reasoning_trace
FROM iceberg.decision.decision_signals
WHERE symbol = '2222'
ORDER BY date DESC LIMIT 10;
```

---

## Part 9 — Maintaining the Knowledge Base

**To adjust a signal, change a CF value, or add a new rule:**

1. **Edit** `dbt/seeds/decision_table.csv` directly (any spreadsheet or text editor).
2. **Run** the dbt commands to reload the seed and rebuild the model:
   ```bash
   docker exec dbt dbt seed --select decision_table --project-dir /usr/dbt --profiles-dir /root/.dbt
   docker exec dbt dbt run --select decision_signals --project-dir /usr/dbt --profiles-dir /root/.dbt
   ```

No SQL knowledge required — the domain expert edits the CSV only.

**CF assignment guidelines (based on propose‑protect‑validate):**

| Scenario | Recommended `signal_cf` |
|----------|------------------------|
| Perfect BUY: Strong Buy + low vol + near 52W low (propose strong, protect passes, validate positive) | +0.90 to +0.95 |
| Standard BUY: good conditions, neutral 52W (propose good, protect passes, validate neutral) | +0.70 to +0.88 |
| Moderate BUY: near resistance or slightly elevated vol (validate negative or protect soft) | +0.50 to +0.65 |
| BUY blocked by gate — positive lean (protect active but technicals strong) | +0.10 to +0.28 |
| BUY blocked by gate — neutral (protect active, technicals moderate) | 0.00 |
| BUY blocked by anomaly — negative lean (protect anomaly with weak technicals) | −0.12 to −0.30 |
| Standard SELL (propose sell, protect passes, validate neutral) | −0.68 to −0.75 |
| SELL near support (near 52W low) — lower conviction (validate negative) | −0.50 to −0.60 |
| SELL confirmed by anomaly or sector (protect or validate positive for sell) | −0.80 to −0.88 |
| Maximum SELL: Strong Sell + anomaly (propose strong, protect confirms) | −0.90 to −0.95 |

**Adding a new rule:** Add a row with a new `state_id` (e.g., `S37`). Compute `specificity_score` as the count of non‑`any` conditions. Ensure the new `state_id` does not conflict with existing ordering if tie‑breaking matters. Reserved range S01–S36 should not be reused; use S37+.

---

## Limitations & Caveats

- **Not financial advice.** The signals are deterministic rules on historical prices. They do not constitute trading recommendations.
- **Shallow knowledge (Lecture 6 §6.6).** The system has no causal model — earnings surprises, news sentiment, macroeconomic events, or geopolitical risks are not modelled.
- **Simulation data.** The Kafka producer uses a random‑walk simulation outside market hours (Sun–Thu 10:00–15:00 Riyadh). Signals from simulated ticks are for pipeline testing only; do not trade on them.
- **VWAP gaps.** `session_vwap` = NULL on days without tick data (e.g., batch‑only historical loads).
- **CBR requires history.** `decision_case_outcomes` and `decision_validation` are empty until the Airflow CBR DAG has accumulated 30+ days of WIN/LOSS outcomes from forward returns.
- **No look‑ahead bias.** All indicators use only information available at market close on the signal date. Rolling windows are strictly trailing.
- **Certainty factors are subjective.** The CF values reflect the judgment of the system designer. They can and should be calibrated over time using the validation table (`decision_validation`).