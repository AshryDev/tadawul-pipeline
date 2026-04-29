# Decision Layer — Signal Methodology

This document explains exactly how the pipeline produces a **BUY / SELL / HOLD** signal for every Tadawul-listed stock on every trading day, including the equations used, the certainty factor combination, metarule evaluation, and explanation facility.

---

## Architecture Overview

The decision layer is composed of six dbt models and one Airflow DAG, all feeding into each other in a defined order:

```
dbt/seeds/knowledge_rules.csv   ← Named Knowledge Base (edit to update rules)
         │
         ▼
decision_cf_engine              ← Certainty Factor combination (§4.10)
         │
gold_anomaly_flags  ──────────┐
gold_volatility_index ────────► decision_metarule_flags  ← Metarule evaluation (§4.2)
                               │
gold_technical_rating ─────────┤
gold_52w_levels ───────────────┤
gold_volatility_index ─────────► decision_signals  ← Signal + Explanation facility (§4.8–4.9)
gold_anomaly_flags ────────────┤
gold_sector_performance ───────┘

         │
         │  [Airflow DAG: decision_cbr_outcomes — daily]
         │  reads decision_signals + silver_ohlcv, writes outcomes
         ▼
decision_case_outcomes          ← CBR case library (§7.8–7.10 Retain)
         │
         ├─► decision_cbr_lookup    ← CBR Retrieve + Reuse
         └─► decision_validation    ← Accuracy, Reliability, Sensitivity (§4.11–4.12)
```

---

## Part 1 — Gold Layer Inputs

### Step 1 — Technical Rating (8 indicators)

`gold_technical_rating` computes a **vote** of +1 (buy), 0 (neutral), or −1 (sell) for each of eight indicators. The votes are summed into a `signal_score` ranging from −8 to +8.

#### Moving Averages

All SMAs are simple (un-weighted) averages of the closing price over the trailing N days.

$$\text{SMA}_N = \frac{1}{N} \sum_{i=0}^{N-1} \text{close}_{t-i}$$

| # | Indicator | Buy vote (+1) | Sell vote (−1) |
|---|---|---|---|
| 1 | Price vs SMA10 | close > SMA10 | close < SMA10 |
| 2 | Price vs SMA20 | close > SMA20 | close < SMA20 |
| 3 | Price vs SMA50 | close > SMA50 | close < SMA50 |
| 4 | Price vs SMA200 | close > SMA200 | close < SMA200 |
| 5 | SMA10 / SMA20 cross | SMA10 > SMA20 (golden cross) | SMA10 < SMA20 (death cross) |

#### RSI(14)

$$\text{RS} = \frac{\text{Avg gain}_{14d}}{\text{Avg loss}_{14d}} \qquad \text{RSI} = 100 - \frac{100}{1 + \text{RS}}$$

| # | Indicator | Buy vote | Sell vote |
|---|---|---|---|
| 6 | RSI(14) | RSI < 30 — oversold | RSI > 70 — overbought |

#### Bollinger Bands

$$\text{BB}_{\text{upper}} = \text{SMA}_{20} + 2\,\sigma_{20} \qquad \text{BB}_{\text{lower}} = \text{SMA}_{20} - 2\,\sigma_{20}$$

| # | Indicator | Buy vote | Sell vote |
|---|---|---|---|
| 7 | Bollinger Band position | close < BB_lower | close > BB_upper |

#### MACD Proxy (SMA12 / SMA26)

| # | Indicator | Buy vote | Sell vote |
|---|---|---|---|
| 8 | MACD proxy | SMA12 > SMA26 | SMA12 < SMA26 |

#### Score → Rating Label

```
signal_score = Σ votes  (range: −8 to +8)

 5 to  8  →  Strong Buy
 3 to  4  →  Buy
−2 to  2  →  Neutral
−4 to −3  →  Sell
−8 to −5  →  Strong Sell
```

---

### Step 2 — Anomaly Detection

`gold_anomaly_flags` flags two anomalies per stock per day, pivoted in `decision_signals` into `has_price_anomaly` and `has_volume_anomaly`.

#### Volume Z-score

$$z = \frac{V_t - \bar{V}_{30d}}{\sigma_{V,30d}}$$

**Flags** when $|z| > 2.5$ (abnormal trading activity).

#### Price IQR (daily log-return)

$$r_t = \ln\!\left(\frac{\text{close}_t}{\text{close}_{t-1}}\right)$$

$$\text{lower} = Q_1 - 1.5 \times (Q_3 - Q_1) \qquad \text{upper} = Q_3 + 1.5 \times (Q_3 - Q_1)$$

**Flags** when $r_t < \text{lower}$ or $r_t > \text{upper}$ (Tukey outlier, trailing 90-day window).

---

### Step 3 — 52-Week Level Proximity

$$\text{high}_{52w} = \max(\text{high}_{t-251}, \ldots, \text{high}_t) \qquad \text{low}_{52w} = \min(\text{low}_{t-251}, \ldots, \text{low}_t)$$

$$\text{pct\_from\_high} = \frac{\text{close} - \text{high}_{52w}}{\text{high}_{52w}} \quad \text{pct\_from\_low} = \frac{\text{close} - \text{low}_{52w}}{\text{low}_{52w}}$$

Proximity flags (within 2%): `at_52w_high` and `at_52w_low`. These appear in the explanation string and provide context but do not override the signal.

---

### Step 4 — Volatility

$$\text{annualized\_vol} = \sigma_{20d} \times \sqrt{252}$$

where $\sigma_{20d}$ is the rolling 20-day standard deviation of log-returns. High volatility (> 50% annualised) appears in the explanation. Values > 80% trigger the G03 gate (see Step 7).

---

### Step 5 — Sector Confirmation

$$\text{advance\_ratio} = \frac{\text{\# stocks with positive daily return}}{\text{\# stocks in sector}}$$

A value ≥ 0.5 means the sector is broadly advancing. Values < 0.5 trigger the G02 gate.

---

## Part 2 — Decision Layer Processing

### Step 6 — Named Knowledge Base

`dbt/seeds/knowledge_rules.csv` is the explicit Knowledge Base. Each row is a named rule with a certainty factor (CF). This is the **Knowledge Acquisition interface** — domain experts edit this CSV and run `dbt seed` to update the system without touching SQL.

| rule_id | rule_name | category | CF | What it asserts |
|---|---|---|---|---|
| R01 | price_above_sma10 | inference | 0.30 | close > SMA10 → buy vote |
| R02 | price_above_sma20 | inference | 0.40 | close > SMA20 → buy vote |
| R03 | price_above_sma50 | inference | 0.50 | close > SMA50 → buy vote |
| R04 | price_above_sma200 | inference | 0.60 | close > SMA200 → buy vote (most reliable) |
| R05 | golden_cross | inference | 0.50 | SMA10 > SMA20 → buy vote |
| R06 | rsi_oversold | inference | 0.70 | RSI < 30 → buy vote (highest CF) |
| R07 | bollinger_lower_touch | inference | 0.60 | close < BB_lower → buy vote |
| R08 | macd_bullish_proxy | inference | 0.40 | SMA12 > SMA26 → buy vote |
| G01 | no_price_anomaly_gate | knowledge | 1.00 | price anomaly → BLOCK_BUY |
| G02 | sector_advance_gate | knowledge | 0.80 | advance_ratio < 50% → BLOCK_BUY |
| G03 | high_volatility_gate | knowledge | 0.90 | ann. vol > 80% → BLOCK_BUY |
| M01 | market_anomaly_metarule | metarule | 0.90 | market anomaly rate > 30% → TIGHTEN_BUY (require score ≥ 6) |
| M02 | consecutive_down_metarule | metarule | 0.80 | 3 consecutive down days → BLOCK_BUY |

CFs are assigned based on empirical reliability from technical analysis literature (Murphy, Wilder, Bollinger). Higher CF = stronger belief in the rule's validity.

---

### Step 7 — Certainty Factor Combination

`decision_cf_engine` applies the CF combination formula iteratively across all 8 inference rules, producing a single `combined_cf` score per (symbol, date).

Each rule contributes a **signed CF**: `vote × certainty_factor`. A BUY vote gives `+CF`, a SELL vote gives `−CF`, and a neutral vote gives `0`.

The combination formula (applied once per pair, iterated 7 times for 8 rules):

$$\text{CF}(A, B) = \begin{cases} A + B(1-A) & \text{if } A \geq 0 \text{ and } B \geq 0 \quad \text{(reinforcing positive)} \\ A + B(1+A) & \text{if } A \leq 0 \text{ and } B \leq 0 \quad \text{(reinforcing negative)} \\ \dfrac{A + B}{1 - \min(|A|, |B|)} & \text{otherwise} \quad \text{(conflicting evidence)} \end{cases}$$

**Worked example** — all 8 indicators vote BUY:

| Step | Rule added | CF before | CF after |
|---|---|---|---|
| 1 | R01 (cf=+0.30) | — | +0.30 |
| 2 | R02 (cf=+0.40) | +0.30 | 0.30 + 0.40×(1−0.30) = **+0.58** |
| 3 | R03 (cf=+0.50) | +0.58 | 0.58 + 0.50×(1−0.58) = **+0.79** |
| 4 | R04 (cf=+0.60) | +0.79 | 0.79 + 0.60×(1−0.79) = **+0.916** |
| 5 | R05 (cf=+0.50) | +0.916 | 0.916 + 0.50×(1−0.916) = **+0.958** |
| 6 | R06 (cf=+0.70) | +0.958 | 0.958 + 0.70×(1−0.958) = **+0.987** |
| 7 | R07 (cf=+0.60) | +0.987 | 0.987 + 0.60×(1−0.987) = **+0.995** |
| 8 | R08 (cf=+0.40) | +0.995 | 0.995 + 0.40×(1−0.995) = **+0.997** |

The `combined_cf` ranges from −1.0 (maximum sell conviction) to +1.0 (maximum buy conviction).

**Confidence mapping:**

```
|combined_cf| ≥ 0.65  →  HIGH
|combined_cf| ≥ 0.40  →  MEDIUM
|combined_cf| <  0.40 →  LOW
```

---

### Step 8 — Metarule Evaluation

`decision_metarule_flags` evaluates three higher-order rules that can override or tighten the inference engine output.

#### M01 — Market-Wide Anomaly (Tighten)

$$\text{market\_anomaly\_rate} = \frac{\text{\# symbols with price-IQR anomaly today}}{\text{\# total symbols}}$$

If rate > 0.30 (> 30% of the market is anomalous), the BUY threshold is raised from `signal_score ≥ 3` to `signal_score ≥ 6`. Rationale: a broad anomaly day indicates market regime instability — only very high-conviction BUY signals should proceed.

#### M02 — Consecutive Down-Day (Block)

$$\text{down\_days\_3d} = \sum_{i=0}^{2} \mathbb{1}[r_{t-i} < -0.02]$$

If a symbol has had 3 consecutive trading days with log-return < −2%, sustained selling pressure is present. BUY is blocked regardless of technical rating.

#### G03 — Extreme Volatility (Block)

If `annualized_vol > 0.80` (80%), the position risk is too high to enter a BUY trade.

**Metarule output columns** (in `decision_metarule_flags` and passed to `decision_signals`):

| Column | Meaning |
|---|---|
| `m01_tighten_buy` | BOOLEAN — M01 fired |
| `m02_consecutive_down` | BOOLEAN — M02 fired |
| `g03_extreme_volatility` | BOOLEAN — G03 fired |
| `active_metarules` | Space-separated list of fired IDs, e.g. `"M01 G03"` |
| `required_score_for_buy` | 3 normally; 6 when M01 fires |

---

### Step 9 — Final Signal Logic

`decision_signals` combines all inputs in the following order of precedence:

```
IF  technical rating ∈ {Buy, Strong Buy}
AND has_price_anomaly = FALSE             ← gate G01: no anomalous spike
AND sector_advance_ratio ≥ 0.50          ← gate G02: sector is net-positive
AND signal_score ≥ required_score_for_buy ← M01 metarule: 3 normally, 6 if market anomalous
AND g03_extreme_volatility = FALSE        ← gate G03: volatility ≤ 80%
AND m02_consecutive_down = FALSE          ← M02 metarule: no 3-day down streak
→  BUY

ELSE IF  technical rating ∈ {Sell, Strong Sell}
      OR (has_price_anomaly = TRUE AND signal_score < 0)
→  SELL

ELSE
→  HOLD
```

**Gate / metarule rationale:**

| Rule | Why |
|---|---|
| G01 no price anomaly | An IQR upside outlier is often a one-day spike before mean reversion. Buying into it is high-risk. |
| G02 sector advance ≥ 50% | Individual signals carry lower conviction when the whole sector is declining (macro headwind). |
| M01 tighten on market anomaly | If > 30% of the market is anomalous, only the strongest setups (score ≥ 6) should proceed. |
| G03 extreme volatility | Annualised vol > 80% means daily price swings of ~5%. Position sizing becomes unsafe. |
| M02 consecutive down days | 3 consecutive −2% days indicate a trend, not noise. Buying into a falling knife contradicts mean-reversion logic. |
| Anomaly + negative score → SELL | A price breakdown (anomaly) coinciding with technical weakness (negative score) is a compounded bearish signal. |

---

### Step 10 — Explanation Facility

`decision_signals` exposes four explanation columns per row.

#### `why_signal` — Why this signal was given

| Signal | Example output |
|---|---|
| BUY | `"BUY: Buy rating (CF=0.72, score=+4/8) + no price anomaly + sector advance=62%"` |
| SELL | `"SELL: Strong Sell rating (score=-6/8)"` |
| SELL (anomaly path) | `"SELL: price-IQR anomaly with negative signal score (-2)"` |
| HOLD | `"HOLD: Buy rating (CF=0.51, score=+3/8) — insufficient conditions for BUY or SELL"` |

#### `why_not_buy` — Why BUY was not triggered (NULL when signal = BUY)

The column cascades through gates in order and reports the **first** one that failed:

```
1. Rating not Buy/Strong Buy  → "BUY not triggered: rating is 'Neutral' (score=1/8, requires Buy or Strong Buy)"
2. Price anomaly              → "BUY blocked by gate G01: price-IQR anomaly detected"
3. Sector advance < 50%       → "BUY blocked by gate G02: sector advance 43% < 50% threshold"
4. M01 tightened, score low   → "BUY blocked by metarule M01: market-wide anomaly rate >30% requires score≥6, current score=4"
5. Extreme volatility         → "BUY blocked by gate G03: extreme volatility (83.2% ann.)"
6. Down streak                → "BUY blocked by metarule M02: 3 consecutive down days (sustained selling pressure)"
```

#### `why_not_sell` — Why SELL was not triggered (NULL when signal = SELL)

```
"SELL not triggered: rating is 'Neutral' and no price anomaly"
"SELL not triggered: price anomaly present but signal_score=1 is non-negative (anomaly SELL requires score < 0)"
```

#### `reasoning_trace` — Step-by-step inference trace (How)

A numbered sequence showing every decision point:

```
1. CF engine: combined_cf=0.72 → BUY direction.
2. Anomaly gate (G01): has_price_anomaly=false.
3. Sector gate (G02): advance_ratio=62% [passed].
4. Metarules: none.
5. Final: BUY (Buy)
```

```
1. CF engine: combined_cf=0.51 → BUY direction.
2. Anomaly gate (G01): has_price_anomaly=false.
3. Sector gate (G02): advance_ratio=43% [BLOCKED].
4. Metarules: none.
5. Final: HOLD
```

---

## Part 3 — Case-Based Reasoning

### CBR Overview

The CBR system follows the 4 R's pattern:

| Step | Component | What it does |
|---|---|---|
| **Retrieve** | `decision_cbr_lookup` | Finds past cases with ≥ 3 matching feature bins |
| **Reuse** | `decision_cbr_lookup` | Aggregates their outcomes (win rate, avg return) |
| **Revise** | `decision_signals` | CF engine and metarules already adjust signals — CBR note is advisory |
| **Retain** | `decision_case_outcomes` + Airflow DAG | Stores resolved outcomes as new cases |

### Feature Binning (5 dimensions)

Each case — past or current — is characterised by 5 categorical bins:

| Dimension | Column | Bins |
|---|---|---|
| Volatility | `annualized_vol` | very_low (<20%) / low (<35%) / medium (<50%) / high (<70%) / very_high |
| RSI | `rsi14` | oversold (<30) / neutral / overbought (>70) |
| 52W position | `pct_from_high` | near_low (<−50%) / mid_low / mid / mid_high / near_high (>−5%) |
| Sector momentum | `sector_advance_ratio` | bearish (<40%) / neutral / bullish (>60%) |
| Technical score | `signal_score` | strong_sell / sell / neutral / buy / strong_buy |

Cases with **≥ 3 matching bins** are considered similar. The lookup aggregates their outcomes:

- `similar_cases_count` — number of similar historical cases found
- `cbr_win_rate` — fraction of similar cases where the signal led to a WIN
- `cbr_avg_return_5d` — average 5-day forward return in similar cases
- `cbr_note` — plain-English summary, e.g. `"Based on 12 similar past cases: 75% win rate, avg 5d return=+2.3%"`

### Outcome Definition

The Airflow DAG `decision_cbr_outcomes` runs daily, looks up forward prices 5 and 20 trading days after each signal, and computes:

$$\text{return}_{5d} = \frac{\text{close}_{t+5} - \text{close}_t}{\text{close}_t}$$

| Signal | WIN condition |
|---|---|
| BUY | return_5d > 0% |
| SELL | return_5d < 0% |
| HOLD | \|return_5d\| ≤ 1% |

---

## Part 4 — Validation

`decision_validation` reports these metrics per month, signal type, and sector:

| Metric | Definition |
|---|---|
| **Accuracy** | Fraction of signals where outcome = WIN |
| **Reliability** | Same as accuracy (fraction of predictions empirically correct) |
| **avg_return_5d** | Average 5-day forward return across all signals of this type |
| **stddev_return_5d** | Standard deviation of 5-day returns (risk measure) |
| **Sensitivity** | Count of signals where \|combined_cf\| is between 0.35–0.45 (near the decision boundary — would flip if the threshold moved slightly) |

This table is empty until `decision_case_outcomes` is populated by the Airflow CBR DAG (requires 5+ trading days of history).

---

## Output Columns

### `decision_signals`

| Column | Type | Description |
|---|---|---|
| `symbol` | VARCHAR | 4-digit Tadawul code |
| `company_name` | VARCHAR | Full company name |
| `sector` | VARCHAR | Sector classification |
| `date` | DATE | Trading date |
| `close` | DOUBLE | Closing price (SAR) |
| `signal` | VARCHAR | **BUY / SELL / HOLD** |
| `confidence` | VARCHAR | **HIGH / MEDIUM / LOW** (from CF combination) |
| `explanation` | VARCHAR | Backward-compatible summary string |
| `why_signal` | VARCHAR | Why this signal was generated |
| `why_not_buy` | VARCHAR | Which gate blocked BUY (NULL if signal = BUY) |
| `why_not_sell` | VARCHAR | Why SELL was not triggered (NULL if signal = SELL) |
| `reasoning_trace` | VARCHAR | Numbered step-by-step inference trace |
| `combined_cf` | DOUBLE | Combined certainty factor (−1.0 to +1.0) |
| `cf_confidence` | VARCHAR | HIGH / MEDIUM / LOW from CF formula |
| `cf_buy_strength` | DOUBLE | combined_cf when ≥ 0.40, else 0 |
| `cf_sell_strength` | DOUBLE | \|combined_cf\| when ≤ −0.40, else 0 |
| `active_metarules` | VARCHAR | Space-separated fired metarule IDs |
| `required_score_for_buy` | INT | 3 normally; 6 when M01 fires |
| `signal_score` | INT | −8 to +8 net technical vote |
| `buy_signals` | INT | Count of buy votes (0–8) |
| `sell_signals` | INT | Count of sell votes (0–8) |
| `rating` | VARCHAR | Strong Buy / Buy / Neutral / Sell / Strong Sell |
| `rsi14` | DOUBLE | 14-day RSI |
| `sma50` / `sma200` | DOUBLE | Simple moving averages |
| `bb_lower` / `bb_upper` | DOUBLE | Bollinger Band boundaries |
| `high_52w` / `low_52w` | DOUBLE | 52-week high and low |
| `pct_from_high` | DOUBLE | % below 52-week high (negative) |
| `pct_from_low` | DOUBLE | % above 52-week low (positive) |
| `at_52w_high` | BOOLEAN | Within 2% of 52-week high |
| `at_52w_low` | BOOLEAN | Within 2% of 52-week low |
| `annualized_vol` | DOUBLE | 20-day vol × √252 |
| `has_price_anomaly` | BOOLEAN | IQR outlier on daily return |
| `has_volume_anomaly` | BOOLEAN | Z-score > 2.5 on volume |
| `sector_advance_ratio` | DOUBLE | Fraction of sector advancing (0–1) |

---

## Example Output

### Full row example — BUY signal

```
symbol:            2222
company_name:      Saudi Aramco
date:              2025-03-12
close:             29.80
signal:            BUY
confidence:        HIGH
combined_cf:       0.7900
cf_confidence:     HIGH
active_metarules:  (empty)
required_score_for_buy: 3

why_signal:
  "BUY: Buy rating (CF=0.79, score=+5/8) + no price anomaly + sector advance=64%"

why_not_sell:
  "SELL not triggered: rating is 'Buy' and no price anomaly"

reasoning_trace:
  "1. CF engine: combined_cf=0.79 → BUY direction.
   2. Anomaly gate (G01): has_price_anomaly=false.
   3. Sector gate (G02): advance_ratio=64% [passed].
   4. Metarules: none.
   5. Final: BUY (Buy)"

explanation:
  "Rating: Buy (score=5/8, buy=6, sell=1); RSI(14)=26.4; near 52W low (+1.1% above);
   sector advance=64%"
```

### Full row example — HOLD (gate blocked BUY)

```
symbol:            1010
signal:            HOLD
combined_cf:       0.5100
cf_confidence:     MEDIUM
active_metarules:  (empty)

why_signal:
  "HOLD: Buy rating (CF=0.51, score=+3/8) — insufficient conditions for BUY or SELL"

why_not_buy:
  "BUY blocked by gate G02: sector advance 43% < 50% threshold"

why_not_sell:
  "SELL not triggered: rating is 'Buy' and no price anomaly"

reasoning_trace:
  "1. CF engine: combined_cf=0.51 → BUY direction.
   2. Anomaly gate (G01): has_price_anomaly=false.
   3. Sector gate (G02): advance_ratio=43% [BLOCKED].
   4. Metarules: none.
   5. Final: HOLD"
```

### Full row example — SELL (metarule + anomaly)

```
symbol:            2010
signal:            SELL
active_metarules:  M01 G03
required_score_for_buy: 6

why_signal:
  "SELL: price-IQR anomaly with negative signal score (-2)"

why_not_buy:
  "BUY not triggered: rating is 'Neutral' (score=-2/8, requires Buy or Strong Buy)"

reasoning_trace:
  "1. CF engine: combined_cf=-0.43 → SELL direction.
   2. Anomaly gate (G01): has_price_anomaly=true.
   3. Sector gate (G02): advance_ratio=31% [BLOCKED].
   4. Metarules: M01 G03.
   5. Final: SELL"
```

---

## Query Examples

```sql
-- Latest BUY signals with HIGH confidence
SELECT symbol, company_name, sector, close, combined_cf, why_signal
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
  AND signal = 'BUY'
  AND confidence = 'HIGH'
ORDER BY combined_cf DESC;

-- Why was each HOLD blocked from becoming a BUY?
SELECT symbol, signal_score, combined_cf, why_not_buy, active_metarules
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
  AND signal = 'HOLD'
  AND rating IN ('Buy', 'Strong Buy')
ORDER BY combined_cf DESC;

-- Full reasoning trace for a specific symbol
SELECT date, signal, reasoning_trace, why_signal, why_not_buy
FROM iceberg.decision.decision_signals
WHERE symbol = '2222'
ORDER BY date DESC
LIMIT 10;

-- SELL signals near 52-week highs (potential breakdown)
SELECT symbol, close, high_52w, pct_from_high, why_signal, reasoning_trace
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
  AND signal = 'SELL'
  AND at_52w_high = TRUE;

-- Check which metarules are firing today
SELECT date, active_metarules, COUNT(*) AS symbols_affected
FROM iceberg.decision.decision_metarule_flags
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_metarule_flags)
  AND active_metarules != ''
GROUP BY date, active_metarules
ORDER BY symbols_affected DESC;

-- CBR: BUY signals with strong historical backing
SELECT s.symbol, s.close, s.why_signal, c.cbr_win_rate, c.cbr_avg_return_5d, c.cbr_note
FROM iceberg.decision.decision_signals s
JOIN iceberg.decision.decision_cbr_lookup c ON s.symbol = c.symbol AND s.date = c.date
WHERE s.date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
  AND s.signal = 'BUY'
  AND c.cbr_win_rate >= 0.65
ORDER BY c.cbr_win_rate DESC;

-- Validation: monthly accuracy by signal type
SELECT month, signal, total_signals, accuracy, reliability,
       avg_return_5d, threshold_sensitive_count
FROM iceberg.decision.decision_validation
ORDER BY month DESC, signal;

-- Knowledge Base: inspect all active rules
SELECT rule_id, rule_name, category, certainty_factor, condition_description
FROM iceberg.decision.knowledge_rules
WHERE is_active = TRUE
ORDER BY category, rule_id;
```

---

## Updating the Knowledge Base

To add, modify, or deactivate a rule:

1. Edit `dbt/seeds/knowledge_rules.csv` — change a certainty factor, add a row, or set `is_active=false`.
2. Run: `docker exec dbt dbt seed --select knowledge_rules --project-dir /usr/dbt --profiles-dir /root/.dbt`
3. Re-run the decision layer: `docker exec dbt dbt run --select decision_cf_engine+ --project-dir /usr/dbt --profiles-dir /root/.dbt`

The `+` selector propagates changes downstream through `decision_signals`, `decision_cbr_lookup`, and `decision_validation` automatically.

---

## Important Limitations

- **Not financial advice.** Signals are generated by deterministic SQL rules on historical prices. They do not account for fundamentals, news, earnings, or geopolitical events.
- **Shallow knowledge.** The system uses IF-THEN rules (shallow knowledge). It has no causal model of *why* prices move. Deep knowledge — economic causality, sector dynamics, earnings cycles — is outside the current scope.
- **Simulation data.** Outside Tadawul market hours (Sun–Thu 10:00–15:00 Riyadh time), the producer generates random-walk ticks. Signals derived from simulated data are for pipeline testing only.
- **CBR requires history.** `decision_cbr_lookup` and `decision_validation` return empty or near-empty results until the Airflow CBR DAG has run for several weeks to accumulate outcomes.
- **Yahoo Finance coverage.** The batch pipeline relies on yfinance (`.SR` suffix). Data quality varies by symbol, especially for smaller-cap stocks and dates before 2018.
- **No look-ahead bias.** All indicators use only information available at market close on the signal date.
