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
  decision_signals
  ─────────────────
  Final BUY / SELL / HOLD signal per symbol per day with full KBS-compliant
  explanation facility (Lecture 4 §4.8–4.9 — Why / Why Not / How).

  Aggregates five gold-layer models plus the CF engine and metarule flags
  from the decision layer.

  Signal logic (evaluated in order):
    BUY   — technical rating is Buy or Strong Buy
            AND no price-IQR anomaly (gate G01)
            AND sector advance ratio ≥ 50% (gate G02)
            AND signal_score ≥ required_score_for_buy (3 normally; 6 when M01 fires)
            AND annualised vol ≤ 80% (gate G03)
            AND no 3-consecutive-down-day streak (metarule M02)
    SELL  — technical rating is Sell or Strong Sell
            OR price anomaly fires AND signal_score < 0
    HOLD  — all other cases

  Confidence reflects the CF combination formula (§4.10):
    HIGH   — |combined_cf| ≥ 0.65
    MEDIUM — |combined_cf| ≥ 0.40
    LOW    — |combined_cf| < 0.40

  Explanation columns (§4.8 Why / Why Not / How):
    why_signal      — Why was THIS specific signal generated
    why_not_buy     — Which gate/rule blocked BUY (NULL if signal = BUY)
    why_not_sell    — Why SELL was not triggered (NULL if signal = SELL)
    reasoning_trace — Numbered step-by-step How trace of the inference path
*/

WITH
tech AS (
    SELECT
        symbol, company_name, sector, market_cap_tier,
        date, close, volume,
        sma50, sma200, rsi14, bb_lower, bb_upper,
        signal_score, buy_signals, sell_signals, neutral_signals, rating
    FROM {{ ref('gold_technical_rating') }}

    {% if is_incremental() %}
    WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

levels AS (
    SELECT symbol, date, high_52w, low_52w, pct_from_high, pct_from_low, at_52w_high, at_52w_low
    FROM {{ ref('gold_52w_levels') }}
),

vol AS (
    SELECT symbol, date, annualized_vol
    FROM {{ ref('gold_volatility_index') }}
),

anomalies AS (
    SELECT
        symbol, date,
        MAX(CASE WHEN anomaly_type = 'price_iqr'     AND is_anomaly THEN TRUE ELSE FALSE END) AS has_price_anomaly,
        MAX(CASE WHEN anomaly_type = 'volume_zscore' AND is_anomaly THEN TRUE ELSE FALSE END) AS has_volume_anomaly
    FROM {{ ref('gold_anomaly_flags') }}
    GROUP BY symbol, date
),

sector_perf AS (
    SELECT sector, date,
           avg_daily_return  AS sector_avg_return,
           advance_ratio     AS sector_advance_ratio
    FROM {{ ref('gold_sector_performance') }}
),

cf AS (
    SELECT symbol, date, combined_cf, cf_confidence, cf_buy_strength, cf_sell_strength
    FROM {{ ref('decision_cf_engine') }}
),

metarules AS (
    SELECT symbol, date,
           m01_tighten_buy, m02_consecutive_down, g03_extreme_volatility,
           active_metarules, required_score_for_buy
    FROM {{ ref('decision_metarule_flags') }}
),

joined AS (
    SELECT
        t.symbol, t.company_name, t.sector, t.market_cap_tier,
        t.date, t.close, t.volume,
        t.sma50, t.sma200, t.rsi14, t.bb_lower, t.bb_upper,
        t.signal_score, t.buy_signals, t.sell_signals, t.neutral_signals, t.rating,
        l.high_52w, l.low_52w, l.pct_from_high, l.pct_from_low,
        COALESCE(l.at_52w_high,               FALSE) AS at_52w_high,
        COALESCE(l.at_52w_low,                FALSE) AS at_52w_low,
        v.annualized_vol,
        COALESCE(a.has_price_anomaly,         FALSE) AS has_price_anomaly,
        COALESCE(a.has_volume_anomaly,        FALSE) AS has_volume_anomaly,
        sp.sector_avg_return,
        sp.sector_advance_ratio,
        COALESCE(cf.combined_cf,              0.0)   AS combined_cf,
        COALESCE(cf.cf_confidence,            'LOW') AS cf_confidence,
        COALESCE(cf.cf_buy_strength,          0.0)   AS cf_buy_strength,
        COALESCE(cf.cf_sell_strength,         0.0)   AS cf_sell_strength,
        COALESCE(mr.active_metarules,         '')    AS active_metarules,
        COALESCE(mr.required_score_for_buy,   3)     AS required_score_for_buy,
        COALESCE(mr.m01_tighten_buy,          FALSE) AS m01_tighten_buy,
        COALESCE(mr.m02_consecutive_down,     FALSE) AS m02_consecutive_down,
        COALESCE(mr.g03_extreme_volatility,   FALSE) AS g03_extreme_volatility
    FROM tech             AS t
    LEFT JOIN levels      AS l  ON t.symbol = l.symbol  AND t.date = l.date
    LEFT JOIN vol         AS v  ON t.symbol = v.symbol  AND t.date = v.date
    LEFT JOIN anomalies   AS a  ON t.symbol = a.symbol  AND t.date = a.date
    LEFT JOIN sector_perf AS sp ON t.sector  = sp.sector AND t.date = sp.date
    LEFT JOIN cf          AS cf ON t.symbol = cf.symbol  AND t.date = cf.date
    LEFT JOIN metarules   AS mr ON t.symbol = mr.symbol  AND t.date = mr.date
),

with_signal AS (
    SELECT
        *,

        -- ── Signal (production rules with metarule overrides) ─────────────────
        CASE
            WHEN rating IN ('Strong Buy', 'Buy')
             AND NOT has_price_anomaly
             AND COALESCE(sector_advance_ratio, 0.5) >= 0.5
             AND signal_score >= required_score_for_buy
             AND NOT g03_extreme_volatility
             AND NOT m02_consecutive_down
            THEN 'BUY'

            WHEN rating IN ('Strong Sell', 'Sell')
              OR (has_price_anomaly AND signal_score < 0)
            THEN 'SELL'

            ELSE 'HOLD'
        END AS signal,

        -- ── Confidence (from CF combination formula §4.10) ────────────────────
        CASE
            WHEN ABS(combined_cf) >= 0.65 THEN 'HIGH'
            WHEN ABS(combined_cf) >= 0.40 THEN 'MEDIUM'
            ELSE                               'LOW'
        END AS confidence

    FROM joined
),

-- ── Explanation facility (§4.8: Why / Why Not / How) ─────────────────────────
with_explanations AS (
    SELECT
        *,

        -- WHY: why the given signal was produced
        CASE signal
            WHEN 'BUY' THEN CONCAT(
                'BUY: ', rating, ' rating (CF=', CAST(ROUND(combined_cf, 2) AS VARCHAR),
                ', score=+', CAST(signal_score AS VARCHAR), '/8)',
                ' + no price anomaly + sector advance=',
                CAST(ROUND(COALESCE(sector_advance_ratio, 0) * 100, 0) AS VARCHAR), '%'
            )
            WHEN 'SELL' THEN
                CASE
                    WHEN rating IN ('Strong Sell', 'Sell') AND has_price_anomaly
                    THEN CONCAT('SELL: ', rating, ' rating (score=', CAST(signal_score AS VARCHAR), '/8) + price anomaly confirmed')
                    WHEN rating IN ('Strong Sell', 'Sell')
                    THEN CONCAT('SELL: ', rating, ' rating (score=', CAST(signal_score AS VARCHAR), '/8)')
                    ELSE CONCAT('SELL: price-IQR anomaly with negative signal score (', CAST(signal_score AS VARCHAR), ')')
                END
            ELSE CONCAT(
                'HOLD: ', rating, ' rating (CF=', CAST(ROUND(combined_cf, 2) AS VARCHAR),
                ', score=', CAST(signal_score AS VARCHAR), '/8) — insufficient conditions for BUY or SELL'
            )
        END AS why_signal,

        -- WHY NOT BUY: first failing gate (NULL when signal = BUY)
        CASE
            WHEN signal = 'BUY' THEN NULL
            WHEN NOT (rating IN ('Strong Buy', 'Buy'))
            THEN CONCAT(
                'BUY not triggered: rating is ''', rating,
                ''' (score=', CAST(signal_score AS VARCHAR), '/8, requires Buy or Strong Buy)'
            )
            WHEN has_price_anomaly
            THEN 'BUY blocked by gate G01: price-IQR anomaly detected'
            WHEN COALESCE(sector_advance_ratio, 0.5) < 0.5
            THEN CONCAT(
                'BUY blocked by gate G02: sector advance ',
                CAST(ROUND(COALESCE(sector_advance_ratio, 0) * 100, 0) AS VARCHAR),
                '% < 50% threshold'
            )
            WHEN signal_score < required_score_for_buy
            THEN CONCAT(
                'BUY blocked by metarule M01: market-wide anomaly rate >30% requires score≥',
                CAST(required_score_for_buy AS VARCHAR),
                ', current score=', CAST(signal_score AS VARCHAR)
            )
            WHEN g03_extreme_volatility
            THEN CONCAT(
                'BUY blocked by gate G03: extreme volatility (',
                CAST(ROUND(COALESCE(annualized_vol, 0) * 100, 1) AS VARCHAR), '% ann.)'
            )
            WHEN m02_consecutive_down
            THEN 'BUY blocked by metarule M02: 3 consecutive down days (sustained selling pressure)'
            ELSE 'BUY not triggered'
        END AS why_not_buy,

        -- WHY NOT SELL: why SELL was not triggered (NULL when signal = SELL)
        CASE
            WHEN signal = 'SELL' THEN NULL
            WHEN NOT (rating IN ('Strong Sell', 'Sell')) AND NOT has_price_anomaly
            THEN CONCAT(
                'SELL not triggered: rating is ''', rating, ''' and no price anomaly'
            )
            WHEN NOT (rating IN ('Strong Sell', 'Sell')) AND has_price_anomaly AND signal_score >= 0
            THEN CONCAT(
                'SELL not triggered: price anomaly present but signal_score=',
                CAST(signal_score AS VARCHAR), ' is non-negative (anomaly SELL requires score < 0)'
            )
            WHEN NOT (rating IN ('Strong Sell', 'Sell'))
            THEN CONCAT(
                'SELL not triggered: rating is ''', rating,
                ''' (score=', CAST(signal_score AS VARCHAR), ')'
            )
            ELSE 'SELL not triggered'
        END AS why_not_sell,

        -- HOW: numbered step-by-step reasoning trace (§4.9 Tracing)
        CONCAT(
            '1. CF engine: combined_cf=', CAST(ROUND(combined_cf, 2) AS VARCHAR),
            ' → ', CASE
                WHEN combined_cf >= 0.40  THEN 'BUY direction'
                WHEN combined_cf <= -0.40 THEN 'SELL direction'
                ELSE 'neutral'
            END, '. ',
            '2. Anomaly gate (G01): has_price_anomaly=', CAST(has_price_anomaly AS VARCHAR), '. ',
            '3. Sector gate (G02): advance_ratio=',
            CAST(ROUND(COALESCE(sector_advance_ratio, 0) * 100, 0) AS VARCHAR), '%',
            CASE WHEN COALESCE(sector_advance_ratio, 0.5) >= 0.5 THEN ' [passed]' ELSE ' [BLOCKED]' END, '. ',
            '4. Metarules: ',
            CASE WHEN active_metarules = '' THEN 'none' ELSE active_metarules END, '. ',
            '5. Final: ',
            CASE signal
                WHEN 'BUY'  THEN CONCAT('BUY (', rating, ')')
                WHEN 'SELL' THEN CONCAT('SELL (', rating, ')')
                ELSE 'HOLD'
            END
        ) AS reasoning_trace

    FROM with_signal
)

SELECT
    symbol,
    company_name,
    sector,
    market_cap_tier,
    date,
    close,
    signal,
    confidence,

    -- ── Summary explanation (backward-compatible) ─────────────────────────────
    CONCAT(
        'Rating: ', rating,
        ' (score=', CAST(signal_score AS VARCHAR), '/8',
        ', buy=',   CAST(buy_signals  AS VARCHAR),
        ', sell=',  CAST(sell_signals AS VARCHAR), ')',
        CASE WHEN rsi14 IS NOT NULL
             THEN CONCAT('; RSI(14)=', CAST(ROUND(rsi14, 1) AS VARCHAR)) ELSE '' END,
        CASE WHEN at_52w_high
             THEN CONCAT('; near 52W high (',
                         CAST(ROUND(ABS(pct_from_high) * 100, 1) AS VARCHAR), '% below)') ELSE '' END,
        CASE WHEN at_52w_low
             THEN CONCAT('; near 52W low (+',
                         CAST(ROUND(pct_from_low * 100, 1) AS VARCHAR), '% above)') ELSE '' END,
        CASE WHEN has_price_anomaly  THEN '; price anomaly'  ELSE '' END,
        CASE WHEN has_volume_anomaly THEN '; unusual volume' ELSE '' END,
        CASE WHEN COALESCE(annualized_vol, 0.0) > 0.5
             THEN CONCAT('; high vol (', CAST(ROUND(annualized_vol * 100, 1) AS VARCHAR), '% ann.)') ELSE '' END,
        CASE WHEN sector_advance_ratio IS NOT NULL
             THEN CONCAT('; sector advance=', CAST(ROUND(sector_advance_ratio * 100, 0) AS VARCHAR), '%') ELSE '' END
    ) AS explanation,

    -- ── KBS explanation facility (§4.8–4.9) ──────────────────────────────────
    why_signal,
    why_not_buy,
    why_not_sell,
    reasoning_trace,

    -- ── CF engine output (§4.10) ──────────────────────────────────────────────
    ROUND(combined_cf,      4) AS combined_cf,
    cf_confidence,
    ROUND(cf_buy_strength,  4) AS cf_buy_strength,
    ROUND(cf_sell_strength, 4) AS cf_sell_strength,

    -- ── Metarule state ────────────────────────────────────────────────────────
    active_metarules,
    required_score_for_buy,

    -- ── Technical metrics ─────────────────────────────────────────────────────
    signal_score, buy_signals, sell_signals, neutral_signals, rating,
    ROUND(rsi14,    2) AS rsi14,
    ROUND(sma50,    4) AS sma50,
    ROUND(sma200,   4) AS sma200,
    ROUND(bb_lower, 4) AS bb_lower,
    ROUND(bb_upper, 4) AS bb_upper,
    high_52w, low_52w,
    ROUND(pct_from_high,  4) AS pct_from_high,
    ROUND(pct_from_low,   4) AS pct_from_low,
    at_52w_high, at_52w_low,
    ROUND(annualized_vol, 4) AS annualized_vol,
    has_price_anomaly,
    has_volume_anomaly,
    ROUND(sector_avg_return,    6) AS sector_avg_return,
    ROUND(sector_advance_ratio, 4) AS sector_advance_ratio,
    CURRENT_TIMESTAMP               AS dbt_updated_at

FROM with_explanations

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
