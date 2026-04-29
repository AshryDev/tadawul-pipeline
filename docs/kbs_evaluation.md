# KBS Project Evaluation
**Course:** Knowledge-Based Systems — Dr. Sayed AbdelGaber, Helwan University
**Project:** Tadawul Stock Analysis Pipeline — Buy / Sell / Hold Decision Layer

---

## 1. Compliance Assessment

**Verdict: Partially Compliant**

The project correctly implements a production-rule-based signal layer (`decision_signals`) that sits on top of a well-engineered data pipeline. It satisfies the most basic KBS definition from Lecture 1 — *"explicit representation of knowledge together with a reasoning system that derives new knowledge"* — but only at a shallow level. The knowledge is embedded in SQL CASE statements rather than stored in an independent, inspectable Knowledge Base. The inference mechanism is a single-pass SQL JOIN with no chaining. The explanation facility produces static text strings rather than dynamic reasoning traces. Several core KBS components defined in Lectures 4, 6, and 7 are absent entirely.

---

## 2. Gap Analysis

### 2.1 Knowledge Base — Partially Implemented

**Lecture ref:** §4.1, §4.2, §7.4

The lectures distinguish three types of AI rules:
- **Knowledge rules** — facts and relationships stored in the KB
- **Inference rules** — advise how to proceed given facts; part of the inference engine
- **Metarules** — rules about how to use other rules

**What the project has:**
The signal logic in `decision_signals.sql` uses production rules (IF-THEN) in SQL CASE expressions. This maps to §4.1 production rules in form only.

**What is missing:**

| Missing element | Lecture location |
|---|---|
| Rules are not named, stored, or retrievable independently — they are hardcoded inside SQL | §4.1 "highly modular, flexible (easy to add or modify)" |
| No **Metarules** | §4.2 "rules about how to use other rules" |
| No rule weighting — all 8 technical indicators vote equally | §4.10 Certainty Factors |
| No distinction between Knowledge Rules and Inference Rules in the codebase | §4.2 |

**Example of a missing metarule:** "If the market-wide volume anomaly rate exceeds 30% of symbols today, tighten the BUY threshold to `signal_score ≥ 6`."

---

### 2.2 Inference Engine — Absent as a Formal Component

**Lecture ref:** §4.3, §7.4

The lectures specify the inference engine as a separate component that directs search through the knowledge base via:
- **Forward chaining** — data-driven; start from facts, derive conclusions
- **Backward chaining** — goal-driven; start from a hypothesis, find supporting evidence
- **Inference/goal trees** — AND/OR/NOT nodes answering "why" and "how"

**What the project has:**
`decision_signals.sql` performs a flat, single-pass SQL computation. It evaluates all inputs simultaneously and produces an output. There is no chaining, no search, and no separation between the rule store and the rule evaluator.

**What is missing:**

| Missing element | Lecture location |
|---|---|
| No forward chaining — the system cannot derive intermediate conclusions and apply further rules to them | §4.3 |
| No backward chaining — given a target (e.g., "why is this a HOLD?"), the system cannot trace back through which rules blocked BUY or SELL | §4.3 |
| Inference engine is not a distinct, reusable component; it is fused with the Knowledge Base into one SQL file | §7.4 "Inference engine — uses the KB to reach conclusions; must understand the KB format" |
| No inference/goal tree structure | §4.3 |

---

### 2.3 Explanation Facility — Static Text Only

**Lecture ref:** §4.8, §4.9, §7.4

The lectures define four types of explanations the facility must support:

| Type | Definition |
|---|---|
| **Why** | Why did the system give this recommendation? |
| **How** | How was this conclusion reached step by step? |
| **Why not** | Why was a different conclusion not reached? |
| **Journalistic** | Who, what, where, when, why, how |

The lectures also define four generation methods (§4.9): Static, Dynamic, Tracing, Justification.

**What the project has:**
The `explanation` column in `decision_signals` is a `CONCAT(...)` string such as:
```
Rating: Buy (score=4/8, buy=6, sell=2); RSI(14)=28.3; near 52W low (+1.2% above); sector advance=62%
```
This is a **static explanation** (§4.9 "pre-inserted text") — it summarises what values are present but does not reason about the decision path.

**What is missing:**

| Missing explanation type | Example of what should be generated |
|---|---|
| **Why not BUY** (when signal = HOLD) | "BUY was blocked because sector advance ratio was 43% (below 50% threshold), despite a Buy technical rating." |
| **Why not SELL** (when signal = HOLD) | "SELL was not triggered because no price anomaly was detected and signal score is above the Sell threshold." |
| **How** (step-by-step trace) | "Step 1: signal_score = +4 → rating = Buy. Step 2: has_price_anomaly = FALSE → anomaly gate passed. Step 3: sector_advance_ratio = 0.43 → sector gate FAILED. Result: HOLD." |
| **Dynamic explanation** | Reconstructed by replaying each rule against the current facts, not by concatenating pre-formatted strings | §4.9 |
| **Tracing** | A recorded line of reasoning showing which rules fired in which order | §4.9 |

---

### 2.4 Uncertainty Handling — Incomplete

**Lecture ref:** §4.10

The lectures cover four formal approaches to uncertainty:
1. Probability ratio
2. Bayes Theory
3. Dempster–Shafer belief functions
4. **Certainty Factors** — belief and disbelief are independent; can be combined within and across rules

**What the project has:**
The `confidence` column maps `|signal_score|` to HIGH/MEDIUM/LOW. This is a **numeric scale** (§4.10 "Numeric scale — e.g., 1 to 100") but it is not a certainty factor system.

**What is missing:**

| Missing element | Lecture location |
|---|---|
| Each rule has no individual certainty factor — the RSI signal and the SMA10 signal carry identical weight | §4.10 CF: "represent belief in an event given evidence" |
| No combination formula for uncertainty across multiple rules | §4.10 "multiple rules can be combined" |
| `confidence` reflects only indicator agreement, not the reliability or historical accuracy of each indicator | §4.10 |
| The BUY/SELL/HOLD output carries no probability estimate — the user cannot know that a HIGH-confidence BUY has, say, a 78% historical accuracy | §4.10 Probability ratio |

**Concrete fix:** Assign a pre-researched certainty factor (CF, range −1 to +1) to each of the 8 indicators based on known empirical reliability. Combine using the standard CF combination formula:

```
CF_combined = CF1 + CF2 × (1 − CF1)    [when both positive]
CF_combined = CF1 + CF2 × (1 + CF1)    [when both negative]
CF_combined = (CF1 + CF2) / (1 − min(|CF1|, |CF2|))   [mixed]
```

---

### 2.5 Knowledge Representation Format — Only One Form Used

**Lecture ref:** §4.1–§4.7

The lectures cover four representation techniques: Production Rules, Semantic Networks, Frames, and Formal Logic (decision tables, decision trees, predicate logic).

**What the project has:**
Only production rules (IF-THEN in SQL CASE).

**What is missing:**

| Missing technique | What it would add | Lecture location |
|---|---|---|
| **Decision table** | A structured matrix mapping combinations of (rating, anomaly, sector_trend) to outputs — more readable and verifiable than nested SQL CASE | §4.7 "spreadsheet format where all possible attributes are compared to conclusions" |
| **Frames** | A structured object representation for each symbol: slots for sector, price level, volatility regime, current rating — enabling inheritance and defaults | §4.6 |
| **Semantic network** | A graph of concept relationships: "BUY is-a signal", "Strong Buy implies BUY", "price anomaly suppresses BUY", "sector headwind reduces confidence" | §4.5 |
| **Metarules** as explicit named rules | Currently buried in SQL logic with no named identity | §4.2 |

---

### 2.6 Knowledge Acquisition Module — Absent

**Lecture ref:** §6.9–§6.11, §7.4

The lectures define knowledge acquisition as a five-step process (§6.10):
1. Identification — break the problem into parts
2. Conceptualization — identify concepts
3. Formalization — represent knowledge
4. Implementation — program the system
5. Testing — validate

Expert system components (§7.4) must include a **knowledge acquisition module** that "helps build new knowledge bases."

**What the project has:**
Rules are hardcoded in `decision_signals.sql`. To change a rule, a developer must edit SQL and redeploy a dbt model. No non-technical user can inspect, modify, or extend the knowledge base.

**What is missing:**

| Missing element | Lecture location |
|---|---|
| No interface for a domain expert (financial analyst) to add or modify rules without SQL | §7.4 Knowledge Acquisition Module |
| No repository where rules live independently of code — e.g., a rules table in a database, a YAML config, or a DSL | §4.1 "highly modular, flexible (easy to add or modify)" |
| Knowledge acquisition was not formally performed — no interviews with stock market experts, no protocol analysis | §6.11 Manual Methods |
| Rules originate from general technical analysis theory, not from elicited expert knowledge | §6.4 "Undocumented — people's knowledge and expertise" |

---

### 2.7 Validation and Verification — Only Data Quality Testing

**Lecture ref:** §4.11, §4.12

The lectures distinguish:
- **Evaluation** — assesses overall system value
- **Validation** — compares system performance to experts; identifies concordance
- **Verification** — ensures correct implementation

Validation measures include: Accuracy, Reliability, Depth, Breadth, Robustness, Sensitivity, Face validity.

**What the project has:**
dbt schema tests (`not_null`, `accepted_values`) verify data correctness — this is **verification only**.

**What is missing:**

| Missing element | Lecture location |
|---|---|
| No backtesting of signals against historical price outcomes — accuracy is unknown | §4.12 Accuracy, Reliability |
| No comparison of system signals to a financial expert's signals (concordance test) | §4.11 "compares system performance to experts" |
| No sensitivity analysis — how does changing the `signal_score ≥ 3` threshold affect outputs? | §4.12 Sensitivity |
| No face validity assessment — no financial expert has reviewed whether the rules are credible | §4.12 Face Validity |
| No Turing test analog — can a financial analyst distinguish system signals from human analyst signals? | §4.12 Turing Test |
| No depth/breadth measurement — how much of the Tadawul domain does the system actually cover? | §4.12 Depth, Breadth |

---

### 2.8 Case-Based Reasoning (CBR) — Absent

**Lecture ref:** §7.8–§7.10

CBR is one of the KBS types listed in §6.13 and covered extensively in Lecture 7. The 4 R's: Retrieve, Reuse, Revise, Retain.

**What the project has:** None.

**What is missing:** A CBR component that:
1. **Stores** resolved signal cases: (symbol, date, market conditions, signal, outcome after N days)
2. **Retrieves** the most similar past market condition when evaluating a new one
3. **Reuses** the past decision as a prior
4. **Retains** new cases to improve over time

This is especially valuable for Tadawul because market conditions repeat (oil price shocks, Ramadan seasonality, geopolitical events) and these patterns are not capturable by pure rule-based indicators.

---

### 2.9 Data-to-Wisdom Hierarchy — Stops at Knowledge

**Lecture ref:** §1.1, §1.3

The hierarchy from Lecture 1:

```
Data → Information → Knowledge → Wisdom
TPS  →   MIS/DSS   →    KBS    →   WBS
```

**What the project has:**
- Data: Bronze layer (raw ticks, OHLCV)
- Information: Silver layer (cleaned, enriched)
- Knowledge: Gold + Decision layers (structured rules, conclusions)

**What is missing:**
The system does not reach **Wisdom** — applying heuristics, morals, and experience to novel situations. Concretely: the system cannot handle novel market conditions it has never seen (a new regulatory change, a market-wide circuit breaker, a black swan event). A wisdom-level capability would require reasoning by analogy, learning from near-miss cases, or applying higher-level strategic principles.

---

### 2.10 Shallow vs. Deep Knowledge

**Lecture ref:** §6.6

The lectures define:
- **Shallow knowledge** — specific IF-THEN rules; limited meaning; no explanation of underlying causality
- **Deep knowledge** — causal structure; built from many inputs including intuition; difficult to encode

**What the project has:** Purely shallow knowledge. The system knows "if RSI < 30, vote BUY" but has no model of *why* RSI < 30 suggests overselling, no understanding of market microstructure, and no causal model of price dynamics.

This is acceptable for a rule-based expert system, but the lectures expect the student to **acknowledge this limitation explicitly** and ideally propose how deep knowledge could be added (e.g., a financial causal model layer).

---

## 3. Recommended Improvements

### 3.1 Separate Knowledge Base from Inference Logic

Create a `rules` table (or YAML file) where each rule is a named, explicit row:

```sql
CREATE TABLE decision.knowledge_base (
    rule_id       VARCHAR,    -- e.g., 'R01_RSI_OVERSOLD'
    description   VARCHAR,    -- human-readable
    condition     VARCHAR,    -- SQL expression or DSL string
    action        VARCHAR,    -- 'BUY_VOTE' / 'SELL_VOTE' / 'BLOCK_BUY'
    certainty_factor DOUBLE,  -- -1.0 to +1.0
    rule_type     VARCHAR,    -- 'knowledge' / 'inference' / 'metarule'
    is_active     BOOLEAN
);
```

The inference engine (a separate dbt model or Python script) reads this table at runtime, evaluates each rule against the current data, and assembles the final signal. This makes rules inspectable, modifiable by non-developers, and auditable.

### 3.2 Implement "Why Not" Explanations

Add a `blocked_signals` column that records which signal was computed before gates were applied, and which gate blocked it:

```
signal: HOLD
attempted_signal: BUY
blocked_by: sector_gate (advance_ratio=0.43 < 0.50)
explanation: "BUY signal was generated by technical indicators (score=+4) 
              but blocked because sector advance ratio (43%) is below the 
              50% confirmation threshold. SELL was not triggered (no price 
              anomaly, score not negative)."
```

This satisfies the **Why not** explanation type from §4.8.

### 3.3 Add Certainty Factors Per Rule

Replace the binary vote (+1/0/−1) with a certainty factor per indicator. Base initial values on established technical analysis research (e.g., RSI oversold in trending markets has lower CF than in ranging markets):

| Rule | Initial CF (Buy) | Initial CF (Sell) |
|---|---|---|
| Price > SMA200 | +0.6 | −0.6 |
| Price > SMA50 | +0.5 | −0.5 |
| Price > SMA20 | +0.4 | −0.4 |
| Price > SMA10 | +0.3 | −0.3 |
| SMA10/SMA20 cross | +0.5 | −0.5 |
| RSI < 30 / > 70 | +0.7 | −0.7 |
| Bollinger breakout | +0.6 | −0.6 |
| MACD proxy | +0.4 | −0.4 |

Combine using the CF combination formula (§4.10). Use the combined CF as `confidence_score` and map to HIGH/MEDIUM/LOW with thresholds validated by backtesting.

### 3.4 Add a Minimal CBR Table

Create an Iceberg table `decision.case_history`:

```
symbol, date, market_condition_features, signal_given, 
close_t0, close_t5, close_t20, outcome (WIN/LOSS/NEUTRAL), stored_at
```

After each dbt run, append today's signals. After N days, compute the outcome by looking at forward returns. Use this table to:
1. Retrieve the most similar past market conditions (k-NN on feature vectors)
2. Report "In 15 similar past cases, BUY led to positive returns 11 times (73%)"
3. Revise and retain over time

This directly satisfies CBR requirements from §7.8–§7.10.

### 3.5 Add a Backtesting Validation Model

Create `decision.validation_accuracy` as a dbt model that joins `decision.case_history` to actual future OHLCV data and computes:
- Signal accuracy (% of BUY signals followed by positive 5-day return)
- Reliability (fraction of predictions empirically correct — §4.12)
- Sensitivity (how output changes when BUY threshold moves from score ≥ 3 to ≥ 4)

This directly satisfies the Validation measures from §4.11–§4.12.

### 3.6 Add a Decision Table

Document the signal logic as an explicit decision table (§4.7) in a code comment or a seed CSV:

| Rating | Price Anomaly | Sector Advance ≥ 50% | → Signal |
|---|---|---|---|
| Strong Buy / Buy | No | Yes | BUY |
| Strong Buy / Buy | No | No | HOLD |
| Strong Buy / Buy | Yes | Any | HOLD |
| Neutral | Any | Any | HOLD |
| Sell / Strong Sell | Any | Any | SELL |
| Any | Yes + score < 0 | Any | SELL |

This makes the exhaustive rule space visible, reveals gaps or contradictions, and can be referenced during validation.

### 3.7 Acknowledge Shallow Knowledge Limitation

Add a section to `docs/decision_signals.md` explicitly stating that the system uses **shallow knowledge** (§6.6) and listing what deep knowledge would require: a causal model of how macro-economic variables, earnings, and sector rotation drive Tadawul price dynamics — beyond the scope of the current implementation.

---

## 4. Priority Plan

### High Priority
*(Required for the project to qualify as a proper KBS per lectures)*

| # | Item | Lecture ref | Effort |
|---|---|---|---|
| H1 | Separate Knowledge Base from inference SQL — name and store rules explicitly | §4.1, §4.2, §7.4 | Medium |
| H2 | Add "Why not" explanations to the `explanation` column | §4.8, §4.9 | Low |
| H3 | Add step-by-step reasoning trace ("How" explanation) | §4.8, §4.9 | Medium |
| H4 | Assign certainty factors to each of the 8 rules; use CF combination formula | §4.10 | Medium |
| H5 | Add a decision table documenting all rule combinations | §4.7 | Low |

### Medium Priority
*(Strongly recommended for a complete KBS design)*

| # | Item | Lecture ref | Effort |
|---|---|---|---|
| M1 | Implement minimal CBR case history table | §7.8–§7.10 | Medium |
| M2 | Add backtesting validation model for accuracy and reliability metrics | §4.11–§4.12 | High |
| M3 | Define at least 2–3 explicit metarules | §4.2 | Low |
| M4 | Add knowledge acquisition documentation: which expert/source each rule came from | §6.11, §5.9 | Low |

### Low Priority
*(Enrichments toward a complete academic submission)*

| # | Item | Lecture ref | Effort |
|---|---|---|---|
| L1 | Frames for stock domain objects (symbol, sector, market regime) | §4.6 | Medium |
| L2 | Semantic network diagram showing rule/concept relationships | §4.5 | Low |
| L3 | Formal Knowledge Engineering models: Task model, Agent model, Design model | §5.5 | Medium |
| L4 | Explicit acknowledgement of shallow-vs-deep knowledge gap in documentation | §6.6 | Low |
| L5 | Sensitivity analysis: vary signal_score thresholds and report output change | §4.12 | Medium |

---

## 5. Summary Table

| KBS Component | Status | Lecture Section |
|---|---|---|
| Knowledge Base (explicit, named rules) | Partial — rules exist but are anonymous SQL | §4.1, §7.4 |
| Inference Engine (forward/backward chaining) | Absent — single-pass SQL computation | §4.3, §7.4 |
| Production Rules (IF-THEN) | Present | §4.1 |
| Metarules | Absent | §4.2 |
| Explanation Facility — Why / How / Why Not | Partial — static text only; no "Why not" or trace | §4.8, §4.9 |
| Uncertainty / Certainty Factors | Partial — numeric scale only, no per-rule CF | §4.10 |
| Decision Table | Absent | §4.7 |
| Semantic Network / Frames | Absent | §4.5, §4.6 |
| Knowledge Acquisition Module | Absent | §6.9, §7.4 |
| Validation / Verification | Partial — data tests only; no signal accuracy | §4.11, §4.12 |
| Case-Based Reasoning | Absent | §7.8–§7.10 |
| Big Data (Volume, Velocity, Variety, Veracity) | Present — Kafka + Iceberg + batch pipeline | §2.3 |
| Data Visualization | Absent — no dashboard or chart layer | Lecture 3 |
| Data-to-Wisdom hierarchy | Reaches Knowledge; Wisdom layer absent | §1.1 |
