{{
    config(
        materialized         = 'incremental',
        unique_key           = ['symbol', 'date'],
        on_schema_change     = 'sync_all_columns',
        incremental_strategy = 'delete+insert',
        schema               = 'decision',
    )
}}

/*
  decision_cbr_lookup
  ────────────────────
  Case-Based Reasoning — Retrieve and Reuse steps (Lecture 7 §7.8–7.10).

  For each (symbol, date) in decision_signals, searches decision_case_outcomes
  for historically similar market situations and reports what happened.

  Similarity is measured across 5 feature dimensions binned into categories:
    vol_bin      — volatility regime   (very_low / low / medium / high / very_high)
    rsi_bin      — RSI zone            (oversold / neutral / overbought)
    position_bin — 52-week position    (near_low / mid_low / mid / mid_high / near_high)
    sector_bin   — sector momentum     (bearish / neutral / bullish)
    score_bin    — technical rating    (strong_sell / sell / neutral / buy / strong_buy)

  Cases with ≥ 3 matching bins are "similar" (Retrieve).
  Their win rates and average returns are aggregated to advise reliability (Reuse).

  Returns empty rows when case_outcomes has insufficient history (< 3 cases).
*/

WITH
-- Feature bins for current signals (same-day join)
current_signals AS (
    SELECT
        symbol, date, signal, signal_score, rsi14, pct_from_high,
        annualized_vol, sector_advance_ratio,

        CASE
            WHEN COALESCE(annualized_vol, 0.0) < 0.20 THEN 'very_low'
            WHEN annualized_vol < 0.35               THEN 'low'
            WHEN annualized_vol < 0.50               THEN 'medium'
            WHEN annualized_vol < 0.70               THEN 'high'
            ELSE                                          'very_high'
        END AS vol_bin,

        CASE
            WHEN COALESCE(rsi14, 50.0) < 30 THEN 'oversold'
            WHEN rsi14 > 70                 THEN 'overbought'
            ELSE                                 'neutral'
        END AS rsi_bin,

        CASE
            WHEN COALESCE(pct_from_high, -0.25) < -0.50 THEN 'near_low'
            WHEN pct_from_high < -0.30                  THEN 'mid_low'
            WHEN pct_from_high < -0.15                  THEN 'mid'
            WHEN pct_from_high < -0.05                  THEN 'mid_high'
            ELSE                                             'near_high'
        END AS position_bin,

        CASE
            WHEN COALESCE(sector_advance_ratio, 0.5) >= 0.60 THEN 'bullish'
            WHEN sector_advance_ratio < 0.40                 THEN 'bearish'
            ELSE                                                  'neutral'
        END AS sector_bin,

        CASE
            WHEN signal_score >=  5 THEN 'strong_buy'
            WHEN signal_score >=  3 THEN 'buy'
            WHEN signal_score >= -2 THEN 'neutral'
            WHEN signal_score >= -4 THEN 'sell'
            ELSE                        'strong_sell'
        END AS score_bin

    FROM {{ ref('decision_signals') }}

    {% if is_incremental() %}
    WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

-- Feature bins for historical outcomes
historical_cases AS (
    SELECT
        o.symbol      AS case_symbol,
        o.signal_date,
        o.signal      AS case_signal,
        o.signal_score,
        o.outcome,
        o.return_5d,
        o.return_20d,

        CASE
            WHEN COALESCE(s.annualized_vol, 0.0) < 0.20 THEN 'very_low'
            WHEN s.annualized_vol < 0.35               THEN 'low'
            WHEN s.annualized_vol < 0.50               THEN 'medium'
            WHEN s.annualized_vol < 0.70               THEN 'high'
            ELSE                                            'very_high'
        END AS vol_bin,

        CASE
            WHEN COALESCE(s.rsi14, 50.0) < 30 THEN 'oversold'
            WHEN s.rsi14 > 70                 THEN 'overbought'
            ELSE                                   'neutral'
        END AS rsi_bin,

        CASE
            WHEN COALESCE(s.pct_from_high, -0.25) < -0.50 THEN 'near_low'
            WHEN s.pct_from_high < -0.30                  THEN 'mid_low'
            WHEN s.pct_from_high < -0.15                  THEN 'mid'
            WHEN s.pct_from_high < -0.05                  THEN 'mid_high'
            ELSE                                               'near_high'
        END AS position_bin,

        CASE
            WHEN COALESCE(s.sector_advance_ratio, 0.5) >= 0.60 THEN 'bullish'
            WHEN s.sector_advance_ratio < 0.40                 THEN 'bearish'
            ELSE                                                    'neutral'
        END AS sector_bin,

        CASE
            WHEN o.signal_score >=  5 THEN 'strong_buy'
            WHEN o.signal_score >=  3 THEN 'buy'
            WHEN o.signal_score >= -2 THEN 'neutral'
            WHEN o.signal_score >= -4 THEN 'sell'
            ELSE                          'strong_sell'
        END AS score_bin

    FROM {{ ref('decision_case_outcomes') }}  AS o
    JOIN {{ ref('decision_signals') }}        AS s
        ON o.symbol = s.symbol
        AND CAST(o.signal_date AS DATE) = s.date
    WHERE o.outcome IS NOT NULL
),

-- Aggregate similarity metrics per (current_symbol, current_date)
similarity_agg AS (
    SELECT
        c.symbol,
        c.date,
        COUNT(*)                                               AS similar_cases_count,
        ROUND(AVG(h.return_5d),  4)                          AS cbr_avg_return_5d,
        ROUND(AVG(h.return_20d), 4)                          AS cbr_avg_return_20d,
        ROUND(
            CAST(SUM(CASE WHEN h.outcome = 'WIN' THEN 1.0 ELSE 0.0 END) AS DOUBLE)
            / NULLIF(COUNT(*), 0),
        3)                                                     AS cbr_win_rate,
        ROUND(
            CAST(SUM(CASE WHEN h.case_signal = c.signal THEN 1.0 ELSE 0.0 END) AS DOUBLE)
            / NULLIF(COUNT(*), 0),
        3)                                                     AS cbr_signal_agreement
    FROM current_signals c
    JOIN historical_cases h ON c.symbol = h.case_symbol
    WHERE (
        CASE WHEN c.vol_bin      = h.vol_bin      THEN 1 ELSE 0 END +
        CASE WHEN c.rsi_bin      = h.rsi_bin      THEN 1 ELSE 0 END +
        CASE WHEN c.position_bin = h.position_bin THEN 1 ELSE 0 END +
        CASE WHEN c.sector_bin   = h.sector_bin   THEN 1 ELSE 0 END +
        CASE WHEN c.score_bin    = h.score_bin    THEN 1 ELSE 0 END
    ) >= 3
    GROUP BY c.symbol, c.date
)

SELECT
    c.symbol,
    c.date,
    COALESCE(a.similar_cases_count,  0)   AS similar_cases_count,
    COALESCE(a.cbr_avg_return_5d,    0.0) AS cbr_avg_return_5d,
    COALESCE(a.cbr_avg_return_20d,   0.0) AS cbr_avg_return_20d,
    COALESCE(a.cbr_win_rate,         0.0) AS cbr_win_rate,
    COALESCE(a.cbr_signal_agreement, 0.0) AS cbr_signal_agreement,

    CASE
        WHEN COALESCE(a.similar_cases_count, 0) < 3
        THEN 'Insufficient history (< 3 similar cases found)'
        ELSE CONCAT(
            'Based on ', CAST(a.similar_cases_count AS VARCHAR), ' similar past cases: ',
            CAST(ROUND(a.cbr_win_rate * 100, 0) AS VARCHAR), '% win rate, ',
            'avg 5d return=',
            CAST(ROUND(a.cbr_avg_return_5d * 100, 1) AS VARCHAR), '%'
        )
    END AS cbr_note,

    CURRENT_TIMESTAMP AS dbt_updated_at

FROM current_signals        AS c
LEFT JOIN similarity_agg    AS a ON c.symbol = a.symbol AND c.date = a.date

{% if is_incremental() %}
WHERE c.date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
