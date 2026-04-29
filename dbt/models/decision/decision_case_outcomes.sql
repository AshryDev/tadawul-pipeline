{{
    config(
        materialized = 'table',
        schema       = 'decision',
    )
}}

/*
  decision_case_outcomes
  ───────────────────────
  Case-Based Reasoning outcome store (Lecture 7 §7.8–7.10 — Retain step).

  This shell model creates the Iceberg table schema. Actual rows are written by
  the Airflow DAG `decision_cbr_outcomes`, which:
    1. Reads historical decision_signals (5–30 days old).
    2. Looks up actual forward close prices from silver_ohlcv.
    3. Computes 5-day and 20-day forward returns.
    4. Assigns an outcome (WIN / LOSS) and appends via PyIceberg.

  Outcome definition:
    BUY  signal → WIN if return_5d > 0%,  else LOSS
    SELL signal → WIN if return_5d < 0%,  else LOSS
    HOLD signal → WIN if |return_5d| ≤ 1%, else LOSS

  This table grows over time as the case library is built up (Retain step).
  The decision_cbr_lookup model reads it for similarity matching (Retrieve step).
*/

SELECT
    CAST(NULL AS VARCHAR)   AS symbol,
    CAST(NULL AS VARCHAR)   AS signal_date,
    CAST(NULL AS VARCHAR)   AS signal,
    CAST(NULL AS DOUBLE)    AS combined_cf,
    CAST(NULL AS INTEGER)   AS signal_score,
    CAST(NULL AS VARCHAR)   AS sector,
    CAST(NULL AS DOUBLE)    AS close_at_signal,
    CAST(NULL AS DOUBLE)    AS close_5d,
    CAST(NULL AS DOUBLE)    AS close_20d,
    CAST(NULL AS DOUBLE)    AS return_5d,
    CAST(NULL AS DOUBLE)    AS return_20d,
    CAST(NULL AS VARCHAR)   AS outcome,
    CAST(NULL AS TIMESTAMP) AS recorded_at
WHERE 1 = 0
