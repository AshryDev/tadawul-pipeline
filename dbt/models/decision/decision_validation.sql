{{
    config(
        materialized = 'table',
        schema       = 'decision',
    )
}}

/*
  decision_validation
  ────────────────────
  Signal accuracy and reliability metrics per month, signal type, and sector.
  Implements Lecture 4 §4.11–4.12 — Evaluation, Validation, and Verification.

  Measures reported (§4.12):
    Accuracy    — fraction of signals where outcome = WIN
    Reliability — fraction of predictions empirically correct (= accuracy here)
    Sensitivity — count of signals near the decision boundary (|combined_cf| 0.35–0.45)
                  i.e. signals that would flip if the threshold moved slightly

  Requires decision_case_outcomes to be populated by the Airflow CBR DAG.
  Returns an empty table until outcomes exist (no errors on first run).
*/

WITH outcomes AS (
    SELECT
        o.symbol,
        o.signal_date,
        o.signal,
        o.return_5d,
        o.return_20d,
        o.outcome,
        s.combined_cf,
        s.signal_score,
        s.sector
    FROM {{ ref('decision_case_outcomes') }} AS o
    JOIN {{ ref('decision_signals') }}       AS s
        ON o.symbol = s.symbol
        AND CAST(o.signal_date AS DATE) = s.date
    WHERE o.outcome IS NOT NULL
)

SELECT
    DATE_TRUNC('month', CAST(signal_date AS DATE))          AS month,
    signal,
    sector,
    COUNT(*)                                                 AS total_signals,
    SUM(CASE WHEN outcome = 'WIN'  THEN 1 ELSE 0 END)       AS wins,
    SUM(CASE WHEN outcome = 'LOSS' THEN 1 ELSE 0 END)       AS losses,

    -- §4.12 Accuracy: how well the system reflects reality
    ROUND(
        CAST(SUM(CASE WHEN outcome = 'WIN' THEN 1.0 ELSE 0.0 END) AS DOUBLE)
        / NULLIF(COUNT(*), 0),
    3)                                                       AS accuracy,

    -- §4.12 Reliability: fraction of predictions empirically correct
    ROUND(
        CAST(SUM(CASE WHEN outcome = 'WIN' THEN 1.0 ELSE 0.0 END) AS DOUBLE)
        / NULLIF(COUNT(*), 0),
    3)                                                       AS reliability,

    ROUND(AVG(return_5d),    4)                              AS avg_return_5d,
    ROUND(AVG(return_20d),   4)                              AS avg_return_20d,
    ROUND(STDDEV(return_5d), 4)                              AS stddev_return_5d,

    -- §4.12 Sensitivity: signals near the CF decision boundary (would flip with slight threshold change)
    SUM(CASE WHEN ABS(combined_cf) BETWEEN 0.35 AND 0.45 THEN 1 ELSE 0 END)
                                                             AS threshold_sensitive_count,

    -- §4.12 Breadth/Depth: sector-level accuracy spread
    ROUND(
        CAST(SUM(CASE WHEN outcome = 'WIN' THEN 1.0 ELSE 0.0 END) AS DOUBLE)
        / NULLIF(COUNT(*), 0),
    3)                                                       AS sector_accuracy,

    CURRENT_TIMESTAMP                                        AS dbt_updated_at

FROM outcomes
GROUP BY DATE_TRUNC('month', CAST(signal_date AS DATE)), signal, sector
ORDER BY month DESC, signal, sector
