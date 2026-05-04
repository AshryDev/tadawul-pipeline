# Decision Layer — Signal Methodology

This document defines the six Gold layer models that feed the decision layer, how
their outputs are used as inputs to `dbt/seeds/decision_table.csv`, and how the
single `decision_signals` model produces a **BUY / SELL / HOLD** signal with a
formal certainty factor (−1.0 to +1.0) for every Tadawul symbol on every trading day.

---

## Architecture

```
  ┌────────────────────────────────────────────────────────────────┐
  │                     GOLD LAYER  (6 models)                     │
  │                                                                │
  │  gold_technical_rating   →  signal_score, rating, RSI, SMAs   │
  │  gold_volatility_index   →  annualized_vol, vol_level          │
  │  gold_anomaly_flags      →  has_price_anomaly, volume anomaly  │
  │  gold_52w_levels         →  at_52w_high, at_52w_low, position  │
  │  gold_sector_performance →  sector_advance_ratio, sector_ok    │
  │  gold_intraday_vwap      →  session_vwap                       │
  └───────────────────────────────────┬────────────────────────────┘
                                      │  all 6 read directly
                                      ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │  dbt/seeds/decision_table.csv   ← Knowledge Base (edit to update)│
  │  36 named production rules, each with signal_cf (−1.0 to +1.0)  │
  └───────────────────────────────────┬─────────────────────────────┘
                                      │  specificity-ordered lookup
                                      ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │  decision_signals   ← ONE unified output table                  │
  │  signal · signal_cf · confidence · state_id                     │
  │  why_signal · why_not_buy · reasoning_trace                     │
  └─────────────────────────────────────────────────────────────────┘
```

**Knowledge representation used (Lecture 4):**

| Technique | Where | § |
|---|---|---|
| **Decision table** | `decision_table.csv` — 36 named IF-THEN rules with CF | §4.7 Formal Logic |
| **Decision tree** | `signal_score → rating` — 5-branch CASE in SQL | §4.7 Formal Logic |
| **Production rules** | Each row of the decision table is a production rule | §4.1 |
| **Certainty Factors** | `signal_cf` column — formal uncertainty (−1.0 to +1.0) | §4.10 |

---

## Part 1 — The Six Gold Layer Models

### 1. `gold_technical_rating`

Computes **8 indicator votes** (+1 buy, 0 neutral, −1 sell) from the daily close price
and aggregates them into a composite score and rating label.

**Indicators and vote conditions:**

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

**Score and rating:**

```
signal_score = Σ votes   (range: −8 to +8)

 ≥ +5  →  Strong Buy
 ≥ +3  →  Buy
 ≤ −5  →  Strong Sell
 ≤ −3  →  Sell
  else →  Neutral
```

**Key columns used in `decision_signals`:**

| Column | Type | Used for |
|---|---|---|
| `signal_score` | INT | Decision tree classification → `rating` |
| `buy_signals` | INT | Explanation output |
| `sell_signals` | INT | Explanation output + anomaly SELL override |
| `rsi14` | DOUBLE | Explanation output |
| `sma50`, `sma200` | DOUBLE | Explanation output |
| `bb_lower`, `bb_upper` | DOUBLE | Explanation output |

---

### 2. `gold_volatility_index`

Computes **rolling volatility** from daily log-returns.

$$r_t = \ln\!\left(\frac{\text{close}_t}{\text{close}_{t-1}}\right) \qquad \text{annualized\_vol} = \sigma_{20d} \times \sqrt{252}$$

**Key columns used in `decision_signals`:**

| Column | Type | Used for |
|---|---|---|
| `annualized_vol` | DOUBLE | Classified into `vol_level` — decision table key |
| `log_return` | DOUBLE | Anomaly SELL override (score check) |

**`vol_level` classification (decision table key):**

```
annualized_vol > 80%  →  extreme   (daily swings > 5% — unsafe entry)
annualized_vol > 50%  →  high      (elevated — reduces buy conviction)
annualized_vol > 20%  →  normal    (standard trading environment)
annualized_vol ≤ 20%  →  low       (calm — highest buy confidence)
```

---

### 3. `gold_anomaly_flags`

Detects **abnormal price and volume behaviour** using two SQL-layer methods.

**Volume Z-score:**

$$z = \frac{V_t - \bar{V}_{30d}}{\sigma_{V,30d}} \qquad \text{flags when } |z| > 2.5$$

**Price IQR (Tukey fence on 90-day trailing log-returns):**

$$\text{lower} = Q_1 - 1.5(Q_3 - Q_1) \qquad \text{upper} = Q_3 + 1.5(Q_3 - Q_1)$$

Flags when daily log-return falls outside `[lower, upper]`.

**Key columns used in `decision_signals`:**

| Column | Type | Used for |
|---|---|---|
| `has_price_anomaly` | BOOLEAN | Decision table key (`has_anomaly`) · anomaly SELL override |
| `has_volume_anomaly` | BOOLEAN | Explanation output |

---

### 4. `gold_52w_levels`

Computes **rolling 52-week high and low** over a 252-trading-day window.

$$\text{high}_{52w} = \max(\text{high}_{t-251},\ldots,\text{high}_t) \qquad \text{low}_{52w} = \min(\text{low}_{t-251},\ldots,\text{low}_t)$$

$$\text{pct\_from\_high} = \frac{\text{close} - \text{high}_{52w}}{\text{high}_{52w}} \qquad \text{pct\_from\_low} = \frac{\text{close} - \text{low}_{52w}}{\text{low}_{52w}}$$

Proximity flags within 2%: `at_52w_high`, `at_52w_low`.

**Key columns used in `decision_signals`:**

| Column | Type | Used for |
|---|---|---|
| `at_52w_high` | BOOLEAN | Classified into `at_52w_pos` — decision table key |
| `at_52w_low` | BOOLEAN | Classified into `at_52w_pos` — decision table key |
| `high_52w`, `low_52w` | DOUBLE | Explanation output |
| `pct_from_high`, `pct_from_low` | DOUBLE | Explanation output |

**`at_52w_pos` classification (decision table key):**

```
at_52w_low  = true  →  near_low   (within 2% of 52W low — mean reversion zone)
at_52w_high = true  →  near_high  (within 2% of 52W high — resistance zone)
otherwise           →  neutral    (between extremes)
```

---

### 5. `gold_sector_performance`

Computes **sector-wide momentum** across all stocks in each Tadawul sector.

$$\text{advance\_ratio} = \frac{\text{\# stocks with positive daily return}}{\text{\# total stocks in sector}}$$

A value ≥ 0.5 means the sector is broadly advancing.

**Key columns used in `decision_signals`:**

| Column | Type | Used for |
|---|---|---|
| `advance_ratio` | DOUBLE | Classified into `sector_ok` (≥ 0.5 = true) — decision table key |

---

### 6. `gold_intraday_vwap`

Computes **session Volume-Weighted Average Price** from tick data.

$$\text{VWAP} = \frac{\sum_t \text{price}_t \times \text{volume}_t}{\sum_t \text{volume}_t}$$

**Key columns used in `decision_signals`:**

| Column | Type | Used for |
|---|---|---|
| `session_vwap` | DOUBLE | Passed through as context column — not a decision table key |

VWAP provides intraday context. It is available only during market hours (ticks from the Kafka producer). `NULL` outside trading sessions.

---

## Part 2 — Decision Table Lookup

### The Knowledge Base: `decision_table.csv`

Each row is a **named production rule** of the form:

```
IF  rating_group = X
AND has_anomaly  = Y     (true / false / any)
AND sector_ok    = Z     (true / false / any)
AND vol_level    = V     (low / normal / high / extreme / any)
AND at_52w_pos   = P     (near_low / neutral / near_high / any)
THEN  signal = S,  signal_cf = CF
```

The `signal_cf` value IS the formal certainty factor (§4.10) — a numeric scale
from −1.0 to +1.0 pre-assigned based on domain expertise:

```
+1.0  maximum BUY conviction
  │
+0.65  HIGH confidence threshold
  │
+0.40  MEDIUM confidence threshold
  │
 0.00  no directional conviction → HOLD
  │
−0.40  MEDIUM confidence threshold
  │
−0.65  HIGH confidence threshold
  │
−1.0  maximum SELL conviction
```

### Structure: 7 rows per rating group — symmetric coverage

Each of the 5 rating groups gets **7 rows** covering the same 7 scenarios:

| Row within group | Scenario | state_id range |
|---|---|---|
| 1 | High volatility gate (blocks or modifies) | S01, S08, S15, S22, S29 |
| 2 | Extreme volatility gate (stronger block) | S02, S09, S16, S23, S30 |
| 3 | Price anomaly gate | S03, S10, S17, S24, S31 |
| 4 | Sector headwind gate | S04, S11, S18, S25, S32 |
| 5 | Near 52W low differentiator (mean reversion) | S05, S12, S19, S26, S33 |
| 6 | Near 52W high differentiator (resistance/breakdown) | S06, S13, S20, S27, S34 |
| 7 | Default for this rating group | S07, S14, S21, S28, S35 |

Plus **S36** — the global fallback row (`any` for all conditions, HOLD, CF=0.00).

**Total: 36 rows**

### The 36 Rules

**Strong Buy group (S01–S07):**

| State | has\_anomaly | sector\_ok | vol | at\_52w | Signal | CF | Conf |
|---|---|---|---|---|---|---|---|
| S01 | false | true | high | any | HOLD | +0.22 | LOW |
| S02 | false | true | extreme | any | HOLD | −0.12 | LOW |
| S03 | true | any | any | any | HOLD | −0.20 | LOW |
| S04 | false | false | any | any | HOLD | +0.12 | LOW |
| S05 | false | true | any | near\_low | BUY | **+0.92** | HIGH |
| S06 | false | true | any | near\_high | BUY | +0.72 | HIGH |
| S07 | false | true | any | any | BUY | +0.88 | HIGH |

**Buy group (S08–S14):**

| State | has\_anomaly | sector\_ok | vol | at\_52w | Signal | CF | Conf |
|---|---|---|---|---|---|---|---|
| S08 | false | true | high | any | HOLD | +0.18 | LOW |
| S09 | false | true | extreme | any | HOLD | −0.12 | LOW |
| S10 | true | any | any | any | HOLD | −0.28 | LOW |
| S11 | false | false | any | any | HOLD | 0.00 | LOW |
| S12 | false | true | any | near\_low | BUY | +0.80 | HIGH |
| S13 | false | true | any | near\_high | BUY | +0.55 | MEDIUM |
| S14 | false | true | any | any | BUY | +0.70 | HIGH |

**Neutral group (S15–S21):**

| State | has\_anomaly | sector\_ok | vol | at\_52w | Signal | CF | Conf |
|---|---|---|---|---|---|---|---|
| S15 | false | true | high | any | HOLD | −0.10 | LOW |
| S16 | false | true | extreme | any | HOLD | −0.18 | LOW |
| S17 | true | any | any | any | HOLD | −0.20 | LOW |
| S18 | false | false | any | any | HOLD | −0.08 | LOW |
| S19 | false | true | any | near\_low | HOLD | +0.12 | LOW |
| S20 | false | true | any | near\_high | HOLD | −0.08 | LOW |
| S21 | any | any | any | any | HOLD | 0.00 | LOW |

**Sell group (S22–S28):**

| State | has\_anomaly | sector\_ok | vol | at\_52w | Signal | CF | Conf |
|---|---|---|---|---|---|---|---|
| S22 | false | true | high | any | SELL | −0.68 | HIGH |
| S23 | false | true | extreme | any | SELL | −0.60 | MEDIUM |
| S24 | true | any | any | any | SELL | **−0.85** | HIGH |
| S25 | false | false | any | any | SELL | −0.75 | HIGH |
| S26 | false | true | any | near\_low | SELL | −0.52 | MEDIUM |
| S27 | false | true | any | near\_high | SELL | −0.82 | HIGH |
| S28 | any | any | any | any | SELL | −0.70 | HIGH |

**Strong Sell group (S29–S35):**

| State | has\_anomaly | sector\_ok | vol | at\_52w | Signal | CF | Conf |
|---|---|---|---|---|---|---|---|
| S29 | false | true | high | any | SELL | −0.80 | HIGH |
| S30 | false | true | extreme | any | SELL | −0.72 | HIGH |
| S31 | true | any | any | any | SELL | **−0.95** | HIGH |
| S32 | false | false | any | any | SELL | −0.88 | HIGH |
| S33 | false | true | any | near\_low | SELL | −0.68 | HIGH |
| S34 | false | true | any | near\_high | SELL | **−0.92** | HIGH |
| S35 | any | any | any | any | SELL | −0.90 | HIGH |

**Fallback:**

| State | All conditions | Signal | CF | Conf |
|---|---|---|---|---|
| S36 | any / any / any / any / any | HOLD | 0.00 | LOW |

### Row Matching: Specificity-Ordered Lookup

Each (symbol, date) is joined against all matching rows. The row with the highest
`specificity_score` wins. When two rows tie, the **lower `state_id`** wins.

```
specificity_score = count of non-'any' conditions
                  = (rating_group ≠ 'any') + (has_anomaly ≠ 'any')
                  + (sector_ok ≠ 'any') + (vol_level ≠ 'any')
                  + (at_52w_pos ≠ 'any')
```

**Designed tie-breaking:** Within each group, gate rows (S_01–S_04 etc.) have
lower state_ids than differentiator rows (S_05–S_07 etc.). Both have
specificity=4 for the vol vs 52W case. The lower state_id (vol gate) wins —
meaning **high volatility always blocks BUY even when near 52W low**.

**Example resolutions:**

```
Strong Buy + no anomaly + sector_ok + high vol + near_low:
  S01 (high vol, spec=4) vs S05 (near_low, spec=4) → S01 wins → HOLD, CF=+0.22

Strong Buy + no anomaly + sector_ok + normal vol + near_low:
  S01 doesn't match (vol≠high) → S05 wins → BUY, CF=+0.92

Strong Buy + anomaly + near_low:
  S03 (anomaly=true, spec=2) vs S05 (anomaly=false → doesn't match) → S03 wins → HOLD, CF=-0.20

Strong Buy + no anomaly + sector_fail + near_low:
  S04 (sector_ok=false, spec=3) vs S05 (sector_ok=true → doesn't match) → S04 wins → HOLD, CF=+0.12
```

---

## Part 3 — Anomaly SELL Override

After the table lookup, one additional production rule fires before the final signal
is set. It handles a scenario not in the decision table: a **Neutral-rated stock**
(score in range −2 to 0) with a simultaneous price-IQR anomaly:

```
IF  has_price_anomaly = TRUE
AND signal_score < 0
THEN  signal = SELL,  signal_cf = −0.50,  confidence = MEDIUM
```

This covers states like S17 (Neutral + anomaly → HOLD, CF=−0.20) where the table
conservatively gives HOLD, but the combination of anomaly AND a negative score
constitutes a SELL trigger per the original system logic.

---

## Part 4 — CF Number Line

```
                         ┌── BUY gates required ──┐
  ───────────────────────────────────────────────────────────────
  −1.0    −0.65  −0.40        0        +0.40  +0.65    +1.0
  ───────────────────────────────────────────────────────────────
  ████████████ ░░░░░░░ ░░░░░░░░░░░░░░░░░ ░░░░░░░ ████████████
  ◄─HIGH SELL─►◄─MED─► ◄────── LOW / HOLD ──────► ◄─MED─►◄─H.BUY─►
```

| \|signal\_cf\| | `confidence` | Example states |
|---|---|---|
| ≥ 0.65 | **HIGH** | S05 (+0.92), S31 (−0.95), S34 (−0.92), S07 (+0.88) |
| ≥ 0.40 | **MEDIUM** | S13 (+0.55), S23 (−0.60), S26 (−0.52) |
| < 0.40 | **LOW** | S01 (+0.22), S04 (+0.12), S17 (−0.20), S21 (0.00) |

---

## Part 5 — Explanation Facility (§4.8–§4.9)

### `why_signal` — Dynamic (reconstructed from matched row)

```
"BUY (CF=0.92, HIGH): state=S05 — Best setup: strong technicals + clean conditions + near 52W low"
"SELL (CF=-0.85, HIGH): state=S24 — Sell rating + price anomaly = compounded bearish confirmation"
"HOLD (CF=0.22, LOW): state=S01 — High volatility blocks BUY — position sizing unsafe"
```

### `why_not_buy` — First blocking condition (NULL when signal = BUY)

```
"Rating is 'Neutral' (score=1/8) — not eligible for BUY"
"Price-IQR anomaly detected — spike entry risk (state S10)"
"Sector advancing 43% < 50% threshold"
"high volatility (62.4% ann.) — position risk"
"extreme volatility (83.1% ann.) — position risk"
```

### `reasoning_trace` — Numbered How trace (§4.9)

```
1. Inputs classified: rating=Strong Buy, score=+6/8, vol=normal,
   52W=near_low, anomaly=false, sector_ok=true.
2. Decision table matched: state=S05 (specificity=4).
3. Signal=BUY, CF=0.92 (HIGH).
```

```
1. Inputs classified: rating=Buy, score=+3/8, vol=high,
   52W=neutral, anomaly=false, sector_ok=true.
2. Decision table matched: state=S08 (specificity=4).
3. Signal=HOLD, CF=0.18 (LOW). [high vol gate fired]
```

---

## Output Columns

| Column | Type | Description |
|---|---|---|
| `symbol` | VARCHAR | 4-digit Tadawul code |
| `company_name` | VARCHAR | Full company name |
| `sector` | VARCHAR | Sector classification |
| `date` | DATE | Trading date |
| `close` | DOUBLE | Closing price (SAR) |
| `signal` | VARCHAR | **BUY / SELL / HOLD** |
| `signal_cf` | DOUBLE | Certainty factor −1.0 to +1.0 (§4.10) |
| `confidence` | VARCHAR | HIGH (≥0.65) / MEDIUM (≥0.40) / LOW (<0.40) |
| `state_id` | VARCHAR | Matched decision table row (e.g. `S05`) |
| `why_signal` | VARCHAR | Dynamic explanation of this signal |
| `why_not_buy` | VARCHAR | What blocked BUY — NULL if signal = BUY |
| `why_not_sell` | VARCHAR | Why SELL not triggered — NULL if signal = SELL |
| `reasoning_trace` | VARCHAR | Numbered step-by-step inference trace |
| `rating` | VARCHAR | Strong Buy / Buy / Neutral / Sell / Strong Sell |
| `signal_score` | INT | −8 to +8 net indicator vote |
| `buy_signals` | INT | Count of BUY votes (0–8) |
| `sell_signals` | INT | Count of SELL votes (0–8) |
| `rsi14` | DOUBLE | 14-day RSI |
| `sma50` / `sma200` | DOUBLE | Simple moving averages |
| `bb_lower` / `bb_upper` | DOUBLE | Bollinger Band boundaries |
| `annualized_vol` | DOUBLE | 20-day vol × √252 |
| `vol_level` | VARCHAR | low / normal / high / extreme |
| `high_52w` / `low_52w` | DOUBLE | 52-week extremes |
| `pct_from_high` / `pct_from_low` | DOUBLE | % distance from 52W extremes |
| `at_52w_high` / `at_52w_low` | BOOLEAN | Within 2% of 52W extreme |
| `at_52w_pos` | VARCHAR | near\_low / neutral / near\_high |
| `has_price_anomaly` | BOOLEAN | IQR outlier on daily return |
| `has_volume_anomaly` | BOOLEAN | Z-score > 2.5 on volume |
| `sector_advance_ratio` | DOUBLE | Fraction of sector advancing (0–1) |
| `sector_ok` | BOOLEAN | sector\_advance\_ratio ≥ 0.50 |
| `session_vwap` | DOUBLE | Intraday VWAP from tick stream |

---

## Query Examples

```sql
-- Today's BUY signals ranked by CF
SELECT symbol, company_name, sector, close, signal_cf, state_id, why_signal
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
  AND signal = 'BUY'
ORDER BY signal_cf DESC;

-- Which decision table row matched each symbol today?
SELECT symbol, rating, vol_level, at_52w_pos,
       has_price_anomaly, sector_ok, state_id, signal, signal_cf
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
ORDER BY signal_cf DESC;

-- Buy/Strong Buy stocks that got blocked — what state matched?
SELECT symbol, rating, signal_score, vol_level, at_52w_pos,
       has_price_anomaly, sector_ok, state_id, signal_cf, why_not_buy
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
  AND signal = 'HOLD'
  AND rating IN ('Buy', 'Strong Buy')
ORDER BY signal_cf DESC;

-- Distribution of matched states today
SELECT state_id, signal, confidence, COUNT(*) AS symbols_matched
FROM iceberg.decision.decision_signals
WHERE date = (SELECT MAX(date) FROM iceberg.decision.decision_signals)
GROUP BY state_id, signal, confidence
ORDER BY symbols_matched DESC;

-- Full trace for a specific symbol
SELECT date, signal, state_id, signal_cf, confidence, reasoning_trace
FROM iceberg.decision.decision_signals
WHERE symbol = '2222'
ORDER BY date DESC LIMIT 10;
```

---

## Updating the Knowledge Base

To adjust a signal, change a CF value, or add a new rule:

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
| Standard BUY: good conditions, neutral 52W | +0.70 to +0.88 |
| Moderate BUY: near resistance or slightly elevated vol | +0.50 to +0.65 |
| BUY blocked by gate — positive lean | +0.10 to +0.28 |
| BUY blocked by gate — neutral | 0.00 |
| BUY blocked by anomaly — negative lean | −0.12 to −0.30 |
| Standard SELL | −0.68 to −0.75 |
| SELL near support (near 52W low) — lower conviction | −0.50 to −0.60 |
| SELL confirmed by anomaly or sector | −0.80 to −0.88 |
| Maximum SELL: Strong Sell + anomaly | −0.90 to −0.95 |

**Adding a new rule** — add a row with a new `state_id` (e.g. `S37`) and assign
`specificity_score` as the count of non-`any` conditions. State_ids S01–S36 are
reserved; new rows should use S37+ to avoid changing existing tiebreaker ordering.

---

## Limitations

- **Not financial advice.** Signals are deterministic SQL rules on historical prices.
- **Shallow knowledge (§6.6).** No causal model — earnings surprises, news, and geopolitical events are not modelled.
- **Simulation data.** The Kafka producer uses random-walk simulation outside Tadawul market hours (Sun–Thu 10:00–15:00 Riyadh time). Signals from simulated ticks are for pipeline testing only.
- **VWAP gaps.** `session_vwap` is NULL for days with no tick data (batch-only dates).
- **CBR requires history.** `decision_validation` is empty until the Airflow CBR DAG has accumulated 30+ days of WIN/LOSS outcomes.
- **No look-ahead bias.** All indicators use only information available at market close on the signal date.
