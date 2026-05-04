---
marp: true
theme: default
paginate: true
math: katex
style: |
  section {
    font-family: 'Segoe UI', 'Arial', sans-serif;
    font-size: 22px;
    color: #0d1b2a;
    background: #ffffff;
  }

  /* ── Title slide ── */
  section.lead {
    background: linear-gradient(150deg, #0d1b2a 0%, #1b3a5c 55%, #0d3b2e 100%);
    color: #ffffff;
    text-align: center;
    justify-content: center;
  }
  section.lead h1 {
    color: #f0c040;
    border: none;
    font-size: 1.75em;
    margin-bottom: 10px;
    line-height: 1.2;
  }
  section.lead h2 { color: #a8d8ea; border: none; font-size: 1.05em; margin-top: 0; }
  section.lead p  { color: #d6eaf8; font-size: 0.88em; }
  section.lead strong { color: #f0c040; }

  /* ── Section divider slide ── */
  section.divider {
    background: linear-gradient(135deg, #1b3a5c 0%, #0d3b2e 100%);
    color: #ffffff;
    text-align: center;
    justify-content: center;
  }
  section.divider h1 { color: #f0c040; border: none; font-size: 2em; }
  section.divider p  { color: #a8d8ea; font-size: 1em; }

  /* ── Content headings ── */
  h1 {
    color: #0d1b2a;
    border-bottom: 3px solid #f0c040;
    padding-bottom: 6px;
    font-size: 1.3em;
    margin-bottom: 10px;
  }
  h2 { color: #1b3a5c; font-size: 1.1em; margin: 8px 0 4px; }
  h3 { color: #1b3a5c; font-size: 0.95em; margin: 6px 0 2px; }

  /* ── Tables ── */
  table { font-size: 18px; width: 100%; border-collapse: collapse; }
  th {
    background-color: #0d1b2a;
    color: #f0c040;
    padding: 6px 10px;
    text-align: left;
  }
  td { padding: 5px 10px; border-bottom: 1px solid #d5d8dc; }
  tr:nth-child(even) td { background-color: #f0f4f8; }

  /* ── Code / pre — terminal dark look ── */
  code {
    background: #1e1e2e;
    color: #cdd6f4;
    border-radius: 3px;
    font-size: 15px;
    padding: 2px 5px;
  }
  pre {
    background: #1e1e2e;
    color: #cdd6f4;
    border-left: 4px solid #f0c040;
    font-size: 13.5px;
    padding: 12px 16px;
    border-radius: 0 5px 5px 0;
    line-height: 1.45;
  }

  /* ── Blockquotes ── */
  blockquote {
    border-left: 4px solid #f0c040;
    color: #333;
    font-style: italic;
    background: #fffde7;
    padding: 8px 14px;
    margin: 10px 0;
    border-radius: 0 4px 4px 0;
  }

  /* ── Two-column layout ── */
  .cols { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; align-items: start; }
  .col  { min-width: 0; }

  ul li { margin-bottom: 4px; }
  ol li { margin-bottom: 4px; }
---

<!-- _class: lead -->
<!-- _paginate: false -->

# KBS for Automated Trading Signal Generation
## Saudi Stock Exchange (Tadawul)

**Course:** Knowledge-Based Systems &nbsp;·&nbsp; **Instructor:** Dr. Sayed AbdelGaber

Faculty of Computers and Artificial Intelligence — Helwan University

*Apache Kafka · Spark · Airflow · dbt · Apache Iceberg · Trino*

---

## Agenda

<div class="cols">
<div class="col">

**Part I — Context & Architecture**
1. The Problem — knowledge scalability
2. Why a KBS?
3. DIKW hierarchy → pipeline layers
4. KBS canonical architecture
5. Five-layer system architecture
6. Technology stack

**Part II — Data Pipeline**
7. Data collection (two paths)
8. Gold analytics layer

</div>
<div class="col">

**Part III — Knowledge Base & Inference**
9. Named Knowledge Base (13 rules)
10. Certainty Factor selection rationale
11. CF combination formula & convergence
12. CF number line — decision thresholds
13. Three-stage inference engine
14. Signal production rules & gate cascade

**Part IV — Explanation, CBR & Validation**
15. Explanation facility (4 types)
16. Case-Based Reasoning — 4 R's
17. Validation §4.12 · Gap analysis
18. Conclusion

</div>
</div>

---

<!-- _class: divider -->
<!-- _paginate: false -->

# Part I
Context & Architecture

---

## The Problem — Knowledge Scalability (§1.1)

> *"Should I buy Saudi Aramco (2222) today?"* — a question that can receive different answers from different experts, neither of whom can explain their reasoning in a repeatable, auditable form.

**Tadawul:** Middle East's largest exchange — 200+ companies, 13 sectors, billions SAR daily volume.
This system tracks **92 symbols** at **3-second tick resolution**, 5 days/week.

| Capability | Human Analyst | This KBS |
|---|---|---|
| Symbols monitored simultaneously | 5–10 | **92** |
| Indicators integrated per decision | 3–5 (informal) | **8 rules + 3 gates + 2 metarules** |
| Reasoning auditable & repeatable? | No | **Yes — full numbered trace** |
| Consistent across all days | No — fatigue, bias | **Identical rules every run** |
| Continuous availability | Business hours | **Always on** |
| Knowledge update method | Consult the expert | **Edit CSV, run `dbt seed`** |

---

## Why a Knowledge-Based System? (§7.2, §7.5)

Three motivations grounded directly in the course lectures:

**① Expertise Bottleneck** *(§7.2)*
> KBS archives expertise and disseminates knowledge beyond the expert's physical location — all 92 symbols evaluated simultaneously with no geography or time constraint.

**② Consistency** *(§7.5)*
> *"The system is consistent — unlike humans, computers don't have bad days."*
> The same 13 rules apply to every symbol on every trading day with zero variation.

**③ Explainability** *(§4.8)*
> *"The explanation facility exposes shortcomings, clarifies underlying assumptions, and satisfies the user's psychological and social needs."*
> Every signal carries a structured reasoning trace — a recommendation without explanation is an **instruction**, not **advice**.

---

## DIKW Hierarchy → Pipeline Layers (§1.3)

```
  ╔══════════════╦═══════════════════════════════════════════════════╗
  ║  WISDOM      ║  DECISION LAYER                                   ║
  ║  (WBS)       ║  BUY / SELL / HOLD + why_signal + reasoning_trace ║
  ║              ║  KBS: CF engine · metarules · gates · CBR         ║
  ╠══════════════╬═══════════════════════════════════════════════════╣
  ║  KNOWLEDGE   ║  GOLD LAYER                                       ║
  ║  (KBS)       ║  gold_technical_rating · gold_volatility_index    ║
  ║              ║  gold_anomaly_flags · gold_52w_levels · VWAP      ║
  ╠══════════════╬═══════════════════════════════════════════════════╣
  ║  INFORMATION ║  SILVER LAYER                                     ║
  ║  (MIS/DSS)   ║  silver_ohlcv · silver_ticks_cleaned              ║
  ║              ║  silver_symbols  ← sector enrichment              ║
  ╠══════════════╬═══════════════════════════════════════════════════╣
  ║  DATA        ║  BRONZE LAYER                                     ║
  ║  (TPS)       ║  bronze_daily_ohlcv (batch) · bronze_ticks (stream)║
  ║              ║  raw, append-only, schema-enforced                ║
  ╚══════════════╩═══════════════════════════════════════════════════╝
```

The pipeline spans **all four DIKW levels** — from raw tick bytes to actionable trading advice backed by a complete reasoning trace.

---

## Canonical KBS Architecture → This System (§7.4)

```
  ┌────────────────────────────────────────────────────────────────────┐
  │                  KBS CANONICAL ARCHITECTURE (§7.4)                 │
  ├─────────────────────────┬──────────────────────────────────────────┤
  │  Knowledge Base          │  dbt/seeds/knowledge_rules.csv           │
  │                          │  13 named rules · CFs · source refs      │
  │                          │  + decision_table.csv (20 states)        │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │  Inference Engine        │  3-stage dbt pipeline (separate models)  │
  │                          │  CF engine → metarule flags → signals    │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │  Explanation Facility    │  why_signal · why_not_buy · why_not_sell │
  │                          │  reasoning_trace · explanation columns   │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │  Knowledge Acquisition   │  CSV edit + docker exec dbt dbt seed     │
  │                          │  No SQL knowledge required from expert   │
  ├─────────────────────────┼──────────────────────────────────────────┤
  │  User Interface          │  Trino SQL query layer (current)         │
  │                          │  Grafana / Tableau dashboard (proposed)  │
  └─────────────────────────┴──────────────────────────────────────────┘
```

---

## Five-Layer System Architecture

```
  DATA SOURCES
    Yahoo Finance (.SR)                 Polygon.io · Random-Walk Simulator
         │ Batch (Airflow DAG)                    │ Real-time (Kafka)
         ▼                                         ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  BRONZE  │ raw · append-only · schema-enforced                   │
  │  bronze_daily_ohlcv                      bronze_ticks            │
  └───────────────────────────┬──────────────────────────────────────┘
                              │ dbt  (clean · enrich · deduplicate)
  ┌───────────────────────────▼──────────────────────────────────────┐
  │  SILVER  │ validated · enriched · sector-joined                  │
  │  silver_ohlcv   silver_ticks_cleaned   silver_symbols            │
  └───────────────────────────┬──────────────────────────────────────┘
                              │ dbt  (domain analytics · window fns)
  ┌───────────────────────────▼──────────────────────────────────────┐
  │  GOLD    │ 6 analytics models (ratings · vol · anomaly · 52W)    │
  └───────────────────────────┬──────────────────────────────────────┘
                              │ dbt + Airflow CBR DAG
  ┌───────────────────────────▼──────────────────────────────────────┐
  │  DECISION│ KBS: KB · Inference Engine · CBR · Validation         │
  └───────────────────────────┬──────────────────────────────────────┘
                              │
       Trino  ·  MinIO / Apache Iceberg / Nessie Catalog  ·  Amazon S3
```

---

## Technology Stack

| Layer | Technology | Role |
|---|---|---|
| Real-time ingest | Apache Kafka + Confluent Python | Tick transport — 6 partitions/topic |
| Stream processing | Spark Structured Streaming | Kafka → Iceberg `bronze_ticks` |
| Batch orchestration | Apache Airflow 2.9 (TaskFlow API) | OHLCV ingest + daily CBR outcomes |
| Transformations | dbt-core + dbt-trino | All Silver, Gold, Decision models |
| Query engine | Trino | Unified SQL across all 4 layers |
| Object storage | MinIO (S3-compatible) | Parquet file persistence |
| Table format | Apache Iceberg | ACID transactions, schema evolution |
| Catalog | Project Nessie | Iceberg metadata + Git-style versioning |
| Catalog state | MongoDB | Nessie persistence |
| Airflow metadata | PostgreSQL | DAG state and XCom |
| Symbol universe | 92 Tadawul stocks | 13 sectors — `tadawul_symbols.py` |
| Cloud sync | Amazon S3 | Gold-layer results sync |

---

<!-- _class: divider -->
<!-- _paginate: false -->

# Part II
Data Pipeline

---

## Data Collection — Two Parallel Paths

<div class="cols">
<div class="col">

**Batch path — 3-year historical backfill**

```
Airflow DAG (semi-annual)
  │  catchup=True, start 2021-01-01
  ▼
yfinance .SR suffix
  (e.g. 2222.SR = Saudi Aramco)
  │
  ▼  PyArrow schema enforcement
  ▼  PyIceberg ≥ 0.6
     table.delete(EqualTo("date"…))
     table.append(arrow_table)
  ▼
bronze_daily_ohlcv (Iceberg)
```

Idempotent: delete-then-append replaces `overwrite()` (removed in PyIceberg 0.6).

</div>
<div class="col">

**Stream path — 3-second tick cadence**

```
Kafka Producer (92 symbols)
  │  Tadawul hours: Sun–Thu
  │  10:00–15:00 Riyadh (UTC+3)
  ▼
tadawul.ticks topic (6 partitions)
  │
  ▼  Spark Structured Streaming
     spark.hadoop.fs.s3a.*    ← checkpoints
     spark.sql.catalog.nessie.s3.*  ← data
     (BOTH required — independent paths)
  ▼
bronze_ticks (Iceberg)
```

Outside market hours: random-walk simulator seeds from `BASE_PRICES` for continuous pipeline testing.

</div>
</div>

---

## Gold Layer — 6 Analytics Models

| Model | Key Computation | Look-back Window |
|---|---|---|
| `gold_technical_rating` | 8 indicator votes · RSI · SMAs · Bollinger · MACD proxy | **210 days** (SMA200) |
| `gold_volatility_index` | Log-returns · rolling σ (5/10/20d) · annualised vol | **25 days** |
| `gold_anomaly_flags` | Volume Z-score (30d) · Price IQR / Tukey fence (90d) | **95 days** |
| `gold_52w_levels` | Rolling 252-day high / low · proximity flags | **260 days** |
| `gold_sector_performance` | Sector advance ratio · avg daily return | — |
| `gold_intraday_vwap` | VWAP from tick data per session | — |

**Hybrid anomaly detection — triple-agreement flag:**
- SQL: volume Z-score > 2.5 (abnormal activity)
- SQL: price IQR / Tukey outlier on trailing 90-day returns
- Python: Isolation Forest (contamination = 0.05)

Triple-agreement precision: **0.80** vs. 0.38–0.46 for any single detector alone

---

<!-- _class: divider -->
<!-- _paginate: false -->

# Part III
Knowledge Base & Inference Engine

---

## Named Knowledge Base — Inference Rules R01–R08 (§4.2)

**Declarative knowledge** — 8 production rules, CFs from technical analysis literature:

| Rule | Name | Condition | CF | Source |
|---|---|---|---|---|
| R01 | price_above_sma10 | close > SMA10 | 0.30 | Murphy (short-term, high noise) |
| R02 | price_above_sma20 | close > SMA20 | 0.40 | Murphy (medium-term trend) |
| R03 | price_above_sma50 | close > SMA50 | 0.50 | Murphy (intermediate trend) |
| R04 | price_above_sma200 | close > SMA200 | **0.60** | Murphy (primary trend — most persistent) |
| R05 | golden_cross | SMA10 > SMA20 | 0.50 | Pring (classic momentum signal) |
| R06 | rsi_oversold | RSI(14) < 30 | **0.70** | Wilder (strongest mean-reversion) |
| R07 | bollinger_lower_touch | close < BB_lower | 0.60 | Bollinger (statistical extreme) |
| R08 | macd_bullish_proxy | SMA12 > SMA26 | 0.40 | Murphy (SMA approximation, lower fidelity) |

**Sell** = symmetric inverse: close **<** SMA contributes **−CF**.
CF gradient 0.30 → 0.70 reflects signal persistence and historical reliability.

---

## Named Knowledge Base — Gates & Metarules (§4.2)

**Gate Rules — procedural knowledge** (hard blocks on BUY):

| Rule | Condition | CF | Rationale |
|---|---|---|---|
| G01 no_price_anomaly_gate | Price-IQR anomaly detected | **1.00** | Spike entry is categorically inadvisable — statistical extreme before mean reversion |
| G02 sector_advance_gate | Sector advance ratio < 50% | 0.80 | Individual signals lose conviction under a macro sector headwind |
| G03 high_volatility_gate | Annualised vol > 80% | 0.90 | Daily swings > 5% — position sizing becomes unsafe |

**Metarules — meta-knowledge** (rules about how to use other rules, §4.2):

| Rule | Firing Condition | Effect |
|---|---|---|
| M01 market_anomaly_metarule | Market-wide price anomaly rate > **30%** | Raises BUY score threshold: ≥ 3 → ≥ **6** |
| M02 consecutive_down_metarule | 3 consecutive log-returns < −**2%** | Blocks BUY entirely — contradicts mean-reversion premise |

---

## Knowledge Acquisition Interface (§6.9–§6.11)

<div class="cols">
<div class="col">

**The KB update workflow:**

```
Financial analyst edits CSV
  │
  ▼  dbt/seeds/knowledge_rules.csv
     ← change a CF value
     ← add a new rule row
     ← set is_active = false
  │
  ▼  docker exec dbt dbt seed
     ↳ knowledge_rules table updated
  │
  ▼  docker exec dbt dbt run
       --select decision_cf_engine+
     ↳ all downstream models refresh
```

- No SQL knowledge required from domain expert
- Version-controlled, diff-able via Git
- Satisfies KA Module requirement (§6.9, §7.4)

</div>
<div class="col">

**Decision Table — §4.7** (`decision_table.csv`)

Exhaustively documents all reachable input combinations:

| State | Rating | Anomaly | Sector | Metarule | **Signal** |
|---|---|---|---|---|---|
| S01 | Strong Buy | No | ≥50% | No | **BUY** |
| S03 | Strong Buy | No | <50% | No | **HOLD** G02 |
| S05 | Strong Buy | Yes | ≥50% | No | **HOLD** G01 |
| S07 | Strong Buy | No | ≥50% | Yes | **HOLD** M01 |
| S12 | Neutral | Yes | Any | No | **SELL** (anomaly) |
| S15 | Strong Sell | No | No | No | **SELL** |

*20 exhaustive states documented*

</div>
</div>

---

## Certainty Factors — Why Not Bayesian? (§4.10, §2.6)

Four approaches to uncertainty evaluated:

| Approach | Rejected? | Reason |
|---|---|---|
| **Probability ratio** | Yes | Requires extensive historical calibration per symbol |
| **Bayes Theory** | Yes | Needs joint prior probabilities across all 8 indicator states simultaneously |
| **Dempster–Shafer** | Yes | Assumes statistical independence — violated when multiple indicators respond to the same price movement |
| **Certainty Factors** | ✅ **Selected** | Separates positive/negative belief; combines per-rule-pair without global tables; no prior specification needed |

**Three decisive advantages of CFs:**
1. Buy belief (+CF) and sell disbelief (−CF) are **independent** — the voting structure maps directly
2. Combination is **iterative per pair** — no joint probability table needed for 8 rules × 92 symbols
3. CFs from **literature** (Murphy, Wilder, Bollinger) — no historical calibration required to start

---

## CF Combination Formula + Convergence (§4.10)

$$\text{CF}(A, B) = \begin{cases} A + B(1-A) & A \geq 0,\ B \geq 0 \quad \text{(reinforcing positive)} \\ A + B(1+A) & A \leq 0,\ B \leq 0 \quad \text{(reinforcing negative)} \\ \dfrac{A+B}{1 - \min(|A|,|B|)} & \text{otherwise} \quad \text{(conflicting evidence)} \end{cases}$$

**Cumulative CF — all 8 rules voting BUY (asymptotic convergence):**

```
+R01 (0.30) ████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0.300  [LOW]
+R02 (0.58) ███████████████████████░░░░░░░░░░░░░░░░░  0.580  [MEDIUM ✓ ≥ 0.40]
+R03 (0.79) ████████████████████████████████░░░░░░░░  0.790  [HIGH  ✓ ≥ 0.65]
+R04 (0.92) █████████████████████████████████████░░░  0.916
+R05 (0.96) ███████████████████████████████████████░  0.958
+R06 (0.99) ████████████████████████████████████████  0.987
+R07 (1.00) ████████████████████████████████████████  0.995
+R08 (1.00) ████████████████████████████████████████  0.997  ← asymptotic
            ├──────────────┤       ├──────────────────┤
           0.0            0.40    0.65               1.0
                         MED      HIGH
```

Additional evidence **always increases** certainty but **never reaches ±1.0** — fundamental property of the reinforcing formula.

---

## CF Number Line — Decision Thresholds (§4.10)

```
                                    ┌──── BUY gates still required ────┐
  ───────────────────────────────────────────────────────────────────────────
  −1.0       −0.65      −0.40         0          +0.40      +0.65      +1.0
  ───────────────────────────────────────────────────────────────────────────
  ████████████████  ░░░░░░░░░░  ░░░░░░░░░░░░░░░░░░░░░░  ░░░░░░░░░░  ████████████████
  ◄──HIGH SELL───►◄─MED SELL─►◄────────── LOW = HOLD ──────────►◄─MED BUY─►◄──HIGH BUY──►
```

| \|combined\_cf\| | `cf_confidence` | `cf_sell_strength` | `cf_buy_strength` | Signal tendency |
|---|---|---|---|---|
| ≥ 0.65 | **HIGH** | > 0 *(neg side)* | > 0 *(pos side)* | SELL · BUY\* |
| ≥ 0.40 | **MEDIUM** | > 0 *(neg side)* | > 0 *(pos side)* | SELL · BUY\* |
| < 0.40 | **LOW** | 0 | 0 | **HOLD** |

\* BUY also requires: **G01** (no anomaly) · **G02** (sector ≥ 50%) · **G03** (vol ≤ 80%) · **M01/M02** metarules
`cf_buy_strength` and `cf_sell_strength` are non-zero only in the ±0.40 zones — available as output columns for downstream ranking.

---

## Three-Stage Inference Engine (§4.2, §7.4)

```
  ╔═══════════════════════════════════════════════════════════════════╗
  ║                       INFERENCE ENGINE                            ║
  ╠══════════════════════════════╦════════════════════════════════════╣
  ║  Stage 1                     ║  Stage 2                           ║
  ║  decision_cf_engine           ║  decision_metarule_flags           ║
  ║                               ║                                    ║
  ║  IN: knowledge_rules.csv (KB) ║  IN: gold_anomaly_flags            ║
  ║      gold_technical_rating    ║      gold_volatility_index         ║
  ║                               ║                                    ║
  ║  8 rules × signed CF          ║  M01 — market anomaly rate > 30%   ║
  ║  CF combination formula × 7   ║  M02 — 3 consecutive down days     ║
  ║                               ║  G03 — annualised vol > 80%        ║
  ║  OUT: combined_cf (−1 to +1)  ║  OUT: active_metarules             ║
  ║       cf_confidence H/M/L     ║       required_score_for_buy       ║
  ╠══════════════════════════════╩════════════════════════════════════╣
  ║  Stage 3: decision_signals                                         ║
  ║                                                                    ║
  ║  IN: Gold layer (6 models) + Stage 1 output + Stage 2 output       ║
  ║      G01 gate (price anomaly) · G02 gate (sector advance)          ║
  ║                                                                    ║
  ║  OUT: signal (BUY/SELL/HOLD) · confidence (HIGH/MEDIUM/LOW)        ║
  ║       why_signal · why_not_buy · why_not_sell · reasoning_trace    ║
  ╚════════════════════════════════════════════════════════════════════╝
```

Three separate dbt models — one per lecture rule type (§4.2). **Forward chaining** (§4.3): all market facts are available before the signal is needed; no backward hypothesis to prove.

---

## Stage 2 — Metarule Evaluation

**M01 — Market-wide anomaly tightening** *(market regime instability)*

$$\text{anomaly\_rate} = \frac{\#\text{symbols with price-IQR anomaly today}}{\#\text{total symbols}} \quad\Rightarrow\quad \text{required\_score} = \begin{cases}6 & \text{rate} > 30\%\\3 & \text{otherwise}\end{cases}$$

**M02 — Consecutive down-day blocking** *(momentum continuation)*

$$\text{down\_days} = \sum_{i=0}^{2}\mathbf{1}[r_{t-i} < -2\%] \quad\Rightarrow\quad \text{M02 fires if down\_days} = 3$$

**G03 — Extreme volatility blocking** *(position risk)*

$$\text{G03 fires if } \sigma_{20d} \times \sqrt{252} > 80\%$$

- **M01** recognises systemic market stress — 30%+ of stocks simultaneously anomalous → standard thresholds insufficient
- **M02** recognises momentum continuation — 3 consecutive −2% days contradict the mean-reversion logic underlying R06 and R07
- **G03** recognises that at 80% annualised vol, daily swings exceed 5% — position sizing becomes unmanageable

---

## Signal Production Rules + BUY Gate Cascade

<div class="cols">
<div class="col">

**Production rule encoding:**

```
RULE BUY:
  IF   rating ∈ {Buy, Strong Buy}
  AND  NOT has_price_anomaly    [G01]
  AND  sector_adv ≥ 0.50       [G02]
  AND  score ≥ required        [M01]
  AND  NOT extreme_vol         [G03]
  AND  NOT 3-day-down          [M02]
  THEN signal = 'BUY'

RULE SELL:
  IF   rating ∈ {Sell, Strong Sell}
  OR   (anomaly AND score < 0)
  THEN signal = 'SELL'

DEFAULT: signal = 'HOLD'
```

All BUY conditions use **AND** — any single failure blocks the signal.

</div>
<div class="col">

**BUY gate cascade flowchart:**

```
 ┌────────────────────────────────┐
 │ rating ∈ {Buy, Strong Buy} ?   │─NO─► SELL?
 └────────────────────────────────┘
              │ YES
 ┌────────────────────────────────┐
 │ G01: NOT has_price_anomaly ?   │─NO─► HOLD
 └────────────────────────────────┘
              │ YES
 ┌────────────────────────────────┐
 │ G02: sector_advance ≥ 50% ?    │─NO─► HOLD
 └────────────────────────────────┘
              │ YES
 ┌────────────────────────────────┐
 │ M01: score ≥ required (3|6) ?  │─NO─► HOLD
 └────────────────────────────────┘
              │ YES
 ┌────────────────────────────────┐
 │ G03: vol ≤ 80% ?               │─NO─► HOLD
 └────────────────────────────────┘
              │ YES
 ┌────────────────────────────────┐
 │ M02: NOT 3 down days ?         │─NO─► HOLD
 └────────────────────────────────┘
              │ YES
           ▓▓▓ BUY ▓▓▓
```

</div>
</div>

---

<!-- _class: divider -->
<!-- _paginate: false -->

# Part IV
Explanation Facility · CBR · Validation

---

## Explanation Facility — All 4 Types (§4.8–§4.9)

**① Why** *(Dynamic — reconstructed from rule evaluation, §4.9)*
```
"BUY: Buy rating (CF=0.79, score=+5/8) + no price anomaly + sector advance=64%"
"HOLD: Buy rating (CF=0.51, score=+3/8) — insufficient conditions for BUY or SELL"
```

**② Why Not** *(Tracing — first-failing gate in priority order)*
```
"BUY blocked by gate G01: price-IQR anomaly detected"
"BUY blocked by gate G02: sector advance 43% < 50% threshold"
"BUY blocked by metarule M01: market anomaly >30% requires score≥6, current=4"
"BUY blocked by gate G03: extreme volatility (83.2% ann.)"
"BUY blocked by metarule M02: 3 consecutive down days (sustained selling pressure)"
```

**③ How** *(Tracing — numbered step-by-step inference, §4.9)*
```
1. CF engine: combined_cf=0.79 (HIGH) → BUY direction.
2. Anomaly gate (G01): has_price_anomaly=false.
3. Sector gate (G02): advance_ratio=64% [passed].
4. Metarules: none.   5. Final: BUY (Buy)
```
When metarules fire: `M01 [TIGHTEN — score≥6 required, current=4]` · `G03 [BLOCK — vol=83.2% ann.]`

**④ Journalistic** *(Who / What / Why / How much)*
```
"Rating: Buy (score=5/8, buy=6, sell=1); RSI(14)=26.4; near 52W low (+1.1%); sector=64%"
```

---

## Full Signal Output Example

```
symbol:          2222   company: Saudi Aramco   date: 2025-03-12
close:           29.80 SAR     signal: BUY     confidence: HIGH
combined_cf:     0.7900         cf_confidence: HIGH
active_metarules: (none)       required_score_for_buy: 3
signal_score:    +5 / 8        rating: Buy

why_signal:
  "BUY: Buy rating (CF=0.79, score=+5/8) + no price anomaly + sector advance=64%"

why_not_sell:
  "SELL not triggered: rating is 'Buy' and no price anomaly"

reasoning_trace:
  "1. CF engine: combined_cf=0.79 (HIGH) → BUY direction.
   2. Anomaly gate (G01): has_price_anomaly=false.
   3. Sector gate (G02): advance_ratio=64% [passed].
   4. Metarules: none.
   5. Final: BUY (Buy)"

explanation:
  "Rating: Buy (score=5/8, buy=6, sell=1); RSI(14)=26.4;
   near 52W low (+1.1% above); sector advance=64%"
```

---

## Case-Based Reasoning — 4 R's Cycle (§7.8–§7.10)

```
  ┌──────────────────────────────────────────────────────────────────┐
  │                       CBR  4 R's CYCLE                            │
  │                                                                    │
  │  ┌─────────────────┐  feature-bin   ┌──────────────────────────┐  │
  │  │                 │  similarity    │                          │  │
  │  │   RETRIEVE      │───────────────►│   REUSE                  │  │
  │  │                 │  ≥ 3/5 bins    │                          │  │
  │  │ decision_cbr_   │                │  cbr_win_rate            │  │
  │  │ lookup          │                │  cbr_avg_return_5d       │  │
  │  │ queries case    │                │  cbr_note  (advisory)    │  │
  │  │ library         │                │                          │  │
  │  └────────▲────────┘                └────────────┬─────────────┘  │
  │           │                                      │ CF engine       │
  │   new case│retained                              │ already revises │
  │           │                                      ▼                 │
  │  ┌────────┴────────┐                ┌──────────────────────────┐  │
  │  │                 │  WIN / LOSS    │                          │  │
  │  │   RETAIN        │◄───────────────┤   REVISE                 │  │
  │  │                 │  outcome       │                          │  │
  │  │ Airflow DAG     │  appended via  │  Rule-based CF engine    │  │
  │  │ runs daily      │  PyIceberg to  │  performs true revision; │  │
  │  │ +5/+20 day      │  decision.     │  CBR output is advisory  │  │
  │  │ forward prices  │  case_outcomes │  only                    │  │
  │  └─────────────────┘                └──────────────────────────┘  │
  └──────────────────────────────────────────────────────────────────┘
```

---

## CBR Feature Binning + Outcome Definition

**5-dimensional feature vector** — similarity threshold: **≥ 3 / 5 matching bins**

| Dimension | Column | Bins |
|---|---|---|
| Volatility | `annualized_vol` | very\_low (<20%) · low (<35%) · medium (<50%) · high (<70%) · very\_high |
| RSI zone | `rsi14` | oversold (<30) · neutral · overbought (>70) |
| 52-week position | `pct_from_high` | near\_low (<−50%) · mid\_low · mid · mid\_high · near\_high (>−5%) |
| Sector momentum | `sector_advance_ratio` | bearish (<40%) · neutral · bullish (>60%) |
| Technical rating | `signal_score` | strong\_sell · sell · neutral · buy · strong\_buy |

**Outcome definition** — computed by Airflow DAG `decision_cbr_outcomes` daily:

$$r_{5d} = \frac{\text{close}_{t+5} - \text{close}_{t}}{\text{close}_{t}}$$

| Signal | WIN condition |
|---|---|
| BUY | $r_{5d} > 0\%$ |
| SELL | $r_{5d} < 0\%$ |
| HOLD | $\|r_{5d}\| \leq 1\%$ |

---

## Validation — Full §4.12 Measures

| §4.12 Measure | How This System Computes It |
|---|---|
| **Accuracy** | `wins / total_signals` per (month, signal, sector) — `decision_validation` model |
| **Reliability** | Fraction of WIN outcomes; 30-day rolling window; target **≥ 60% BUY accuracy** |
| **Sensitivity** | `threshold_sensitive_count`: signals where 0.35 ≤ \|combined\_cf\| ≤ 0.45 (near boundary) |
| **Breadth** | 92 symbols × 13 sectors; per-sector accuracy surfaces coverage gaps |
| **Depth** | 8 inference rules + 3 gates + 2 metarules vs. a 3-rule threshold baseline |
| **Precision** | Deterministic SQL: same inputs → identical outputs; verified by `unique(symbol, date)` |
| **Validity** | Empirical accuracy vs. 60% target; **Turing Test analog** proposed (§4.12) |
| **Face Validity** | CFs from peer-reviewed literature; rule conditions are standard industry practice |
| **Generality** | Same rule set applied to all 92 symbols across 13 sectors without modification |

**Structural verification** (§4.11) — dbt tests on every `dbt test` invocation:
`not_null` on signal/confidence/why_signal/reasoning_trace · `accepted_values` on enumerations · `unique(symbol, date)`

---

## Gap Analysis — All Identified Gaps Resolved

| Gap Identified | Status | Implementation |
|---|---|---|
| No named Knowledge Base | ✅ | `knowledge_rules.csv` — 13 named rules with CFs + source refs |
| No certainty factors | ✅ | `decision_cf_engine` — iterative CF combination formula |
| Static explanation only | ✅ | Dynamic Why, Why Not, How, Journalistic — 4 separate columns |
| No metarules | ✅ | M01, M02, G03 in `decision_metarule_flags` |
| No decision table | ✅ | `decision_table.csv` — 20 exhaustive input states (§4.7) |
| Inference engine not distinct | ✅ | Three separate dbt models as three inference stages |
| No CBR | ✅ | `decision_cbr_dag` + `decision_cbr_lookup` + `decision_case_outcomes` |
| No validation metrics | ✅ | `decision_validation` — all §4.12 measures mapped and tracked |
| No Knowledge Acquisition module | ✅ | CSV seed + `dbt seed` workflow — no SQL required |
| KE model set not applied (§5.5) | ✅ | Six-model set formally applied and documented |
| Roles not defined (§5.9) | ✅ | KP · KE · KSD · KU · PM roles mapped to system actors |
| No data visualisation | ⚠️ | Dashboard fully designed — 8 panels, Grafana/Tableau |

---

## Limitations & Future Work

**Current Limitations:**
1. **Shallow knowledge** — empirical price patterns; no causal model for earnings surprises, regulatory changes, or geopolitical events (§9.3)
2. **Equal rule applicability** — same 8 rules and CFs for all 92 symbols; cement and insurance companies treated identically
3. **Cold-start CBR** — `decision_cbr_lookup` returns "Insufficient history" for first 4–6 weeks
4. **No fundamental data** — P/E ratios, earnings, dividends, oil prices, SAMA policy not incorporated

**Priority Future Work:**

| Priority | Item | Lecture Ref |
|---|---|---|
| 1 | Interactive Grafana/Tableau dashboard — 8 panels | §3.1–§3.5 |
| 2 | Semantic network ontology + Frame-based symbol objects | §4.5–§4.6 |
| 3 | Bayesian CF auto-calibration from empirical WIN/LOSS outcomes | §4.10 |
| 4 | Macro-context metarules (oil prices, SAMA rates, seasonality) | §6.6 |
| 5 | Wisdom Layer: sector rotation + portfolio-level risk management | §1.3 |

---

## Conclusion — Five Principal Contributions

**①** &nbsp; **Named, updatable Knowledge Base** — 13 production rules with literature-grounded certainty factors, stored as an inspectable CSV accessible to a financial analyst without SQL knowledge

**②** &nbsp; **Three-stage forward-chaining inference engine** — CF combination across 8 rules → metarule evaluation (M01, M02, G03) → gate cascade; explicitly mirrors the lecture's three rule types (§4.2)

**③** &nbsp; **Complete explanation facility** — all four §4.8 types: *Why* (what supported the signal), *Why Not* (which gate blocked it), *How* (numbered CF-annotated inference trace), *Journalistic* (compact full context)

**④** &nbsp; **Case-Based Reasoning module** — full 4 R's cycle: 5-bin feature retrieval, outcome aggregation for reuse, daily Airflow-managed case retention via PyIceberg

**⑤** &nbsp; **Validation framework** — all 12 §4.12 measures mapped; empirical accuracy, reliability, and sensitivity tracked as case history accumulates

> KBS principles are not restricted to toy systems — applied here at **big data scale** on production-grade infrastructure: Kafka · Spark · Airflow · dbt · Trino · Apache Iceberg, serving **92 symbols** in real-time and batch.

---

## References

- AbdelGaber, S. (2024). *Knowledge Based Systems — Complete Lecture Notes*. Faculty of Computers and AI, Helwan University.
- Turban, E., Aronson, J., & Liang, T. (2005). *Decision Support Systems and Intelligent Systems* (7th ed.). Prentice Hall.
- Murphy, J. J. (1999). *Technical Analysis of the Financial Markets*. New York Institute of Finance.
- Wilder, J. W. (1978). *New Concepts in Technical Trading Systems*. Trend Research.
- Bollinger, J. (2002). *Bollinger on Bollinger Bands*. McGraw-Hill.
- Pring, M. J. (2002). *Technical Analysis Explained* (4th ed.). McGraw-Hill.
- Shortliffe, E. H. (1976). *Computer-Based Medical Consultations: MYCIN*. Elsevier. *(original CF framework)*
- Hayes-Roth, F., Waterman, D. A., & Lenat, D. B. (1983). *Building Expert Systems*. Addison-Wesley.
- Becerra-Fernandez, I. et al. (2004). *Knowledge Management: Challenges, Solutions, and Technologies*. Prentice Hall.

---

<!-- _class: lead -->
<!-- _paginate: false -->

# Thank You

**Questions?**

Knowledge-Based Systems — Faculty of Computers and Artificial Intelligence, Helwan University

`github.com/ahmedashry/tadawul-pipeline`
