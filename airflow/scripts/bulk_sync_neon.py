"""
Bulk one-shot sync from Iceberg lakehouse → Neon PostgreSQL via Trino.

Replaces the sync_to_neon DAG's catchup pattern (275 weekly runs × 7-day
windows) with a single full-table copy per table.

Usage (inside the Airflow container):
    python /opt/airflow/scripts/bulk_sync_neon.py

Or from host:
    docker exec airflow-webserver python /opt/airflow/scripts/bulk_sync_neon.py

Optional flags:
    --tables silver_ohlcv gold_anomaly_flags   # sync only specific tables
    --no-truncate                              # append instead of replace
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Table registry ────────────────────────────────────────────────────────────
# (neon_table, iceberg_catalog_schema_table)
TABLES: list[tuple[str, str]] = [
    ("silver_symbols",          "iceberg.silver.silver_symbols"),
    ("silver_ohlcv",            "iceberg.silver.silver_ohlcv"),
    ("gold_52w_levels",         "iceberg.gold.gold_52w_levels"),
    ("gold_anomaly_flags",      "iceberg.gold.gold_anomaly_flags"),
    ("gold_sector_performance", "iceberg.gold.gold_sector_performance"),
    ("gold_volatility_index",   "iceberg.gold.gold_volatility_index"),
    ("gold_intraday_vwap",      "iceberg.gold.gold_intraday_vwap"),
    ("gold_technical_rating",   "iceberg.gold.gold_technical_rating"),
]

# ── DDL (Trino SQL — forwarded to Neon by the PostgreSQL connector) ───────────
_DDL: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS neon.public.silver_symbols (
        symbol          VARCHAR NOT NULL,
        company_name    VARCHAR NOT NULL,
        sector          VARCHAR NOT NULL,
        market_cap_tier VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS neon.public.silver_ohlcv (
        symbol          VARCHAR NOT NULL,
        date            DATE    NOT NULL,
        open            DOUBLE,
        high            DOUBLE,
        low             DOUBLE,
        close           DOUBLE,
        volume          BIGINT,
        vwap            DOUBLE,
        transactions    BIGINT,
        ingestion_time  TIMESTAMP,
        dbt_updated_at  TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS neon.public.gold_52w_levels (
        symbol          VARCHAR NOT NULL,
        company_name    VARCHAR,
        sector          VARCHAR,
        market_cap_tier VARCHAR,
        date            DATE    NOT NULL,
        open            DOUBLE,
        high            DOUBLE,
        low             DOUBLE,
        close           DOUBLE,
        volume          BIGINT,
        high_52w        DOUBLE,
        low_52w         DOUBLE,
        pct_from_high   DOUBLE,
        pct_from_low    DOUBLE,
        at_52w_high     BOOLEAN,
        at_52w_low      BOOLEAN,
        dbt_updated_at  TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS neon.public.gold_anomaly_flags (
        symbol       VARCHAR NOT NULL,
        date         DATE    NOT NULL,
        anomaly_type VARCHAR NOT NULL,
        score        DOUBLE,
        threshold    DOUBLE,
        is_anomaly   BOOLEAN,
        detected_at  TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS neon.public.gold_sector_performance (
        sector                VARCHAR NOT NULL,
        date                  DATE    NOT NULL,
        symbol_count          BIGINT,
        avg_daily_return      DOUBLE,
        max_daily_return      DOUBLE,
        min_daily_return      DOUBLE,
        stddev_daily_return   DOUBLE,
        total_sector_volume   BIGINT,
        advance_ratio         DOUBLE,
        dbt_updated_at        TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS neon.public.gold_volatility_index (
        symbol              VARCHAR NOT NULL,
        company_name        VARCHAR,
        sector              VARCHAR,
        date                DATE    NOT NULL,
        close               DOUBLE,
        volume              BIGINT,
        high                DOUBLE,
        low                 DOUBLE,
        log_return          DOUBLE,
        vol_5d              DOUBLE,
        vol_10d             DOUBLE,
        vol_20d             DOUBLE,
        annualized_vol_5d   DOUBLE,
        annualized_vol_10d  DOUBLE,
        annualized_vol      DOUBLE,
        dbt_updated_at      TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS neon.public.gold_intraday_vwap (
        symbol          VARCHAR NOT NULL,
        company_name    VARCHAR,
        sector          VARCHAR,
        market_cap_tier VARCHAR,
        session_date    DATE    NOT NULL,
        vwap            DOUBLE,
        session_open    DOUBLE,
        session_high    DOUBLE,
        session_low     DOUBLE,
        session_close   DOUBLE,
        total_volume    BIGINT,
        tick_count      BIGINT,
        session_start   TIMESTAMP,
        session_end     TIMESTAMP,
        dbt_updated_at  TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS neon.public.gold_technical_rating (
        symbol           VARCHAR NOT NULL,
        company_name     VARCHAR,
        sector           VARCHAR,
        market_cap_tier  VARCHAR,
        date             DATE    NOT NULL,
        close            DOUBLE,
        volume           BIGINT,
        avg_volume_20d   DOUBLE,
        sma10            DOUBLE,
        sma20            DOUBLE,
        sma50            DOUBLE,
        sma200           DOUBLE,
        bb_lower         DOUBLE,
        bb_upper         DOUBLE,
        rsi14            DOUBLE,
        sig_price_sma10  INTEGER,
        sig_price_sma20  INTEGER,
        sig_price_sma50  INTEGER,
        sig_price_sma200 INTEGER,
        sig_ma_cross     INTEGER,
        sig_rsi          INTEGER,
        sig_bb           INTEGER,
        sig_macd_proxy   INTEGER,
        buy_signals      INTEGER,
        sell_signals     INTEGER,
        neutral_signals  INTEGER,
        signal_score     INTEGER,
        rating           VARCHAR,
        dbt_updated_at   TIMESTAMP
    )
    """,
]


def _trino_connect():
    """Return a raw DBAPI connection to Trino."""
    import trino

    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="airflow",
        http_scheme="http",
    )


def _run(cursor, sql: str, label: str) -> None:
    log.info("%s", label)
    cursor.execute(sql.strip())
    # Trino DBAPI is lazy — fetchone() drives execution to completion.
    try:
        cursor.fetchone()
    except Exception:
        pass


def ensure_schema(cursor) -> None:
    for ddl in _DDL:
        table_name = ddl.strip().split("neon.public.")[1].split()[0]
        _run(cursor, ddl, f"DDL: ensuring neon.public.{table_name}")
    log.info("Schema check complete.")


def iceberg_table_exists(cursor, full_iceberg_name: str) -> bool:
    """e.g. 'iceberg.gold.gold_anomaly_flags' → check information_schema."""
    _, schema, table = full_iceberg_name.split(".")
    cursor.execute(
        f"SELECT COUNT(*) FROM iceberg.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    )
    row = cursor.fetchone()
    return bool(row and row[0] > 0)


def sync_table(cursor, neon_tbl: str, iceberg_full: str, truncate: bool) -> None:
    if not iceberg_table_exists(cursor, iceberg_full):
        log.warning("%s: source table %s not found in Iceberg — skipping.", neon_tbl, iceberg_full)
        return

    if truncate:
        _run(cursor, f"DELETE FROM neon.public.{neon_tbl}", f"Truncating neon.public.{neon_tbl}")

    _run(
        cursor,
        f"INSERT INTO neon.public.{neon_tbl} SELECT * FROM {iceberg_full}",
        f"Inserting neon.public.{neon_tbl} ← {iceberg_full}",
    )

    # Row count for confirmation
    cursor.execute(f"SELECT COUNT(*) FROM neon.public.{neon_tbl}")
    row = cursor.fetchone()
    log.info("neon.public.%s: %s rows now present.", neon_tbl, row[0] if row else "?")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk sync Iceberg → Neon via Trino")
    parser.add_argument(
        "--tables",
        nargs="+",
        metavar="TABLE",
        help="Sync only these Neon table names (default: all)",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Append to existing Neon data instead of replacing it",
    )
    args = parser.parse_args()

    target_tables = set(args.tables) if args.tables else None
    truncate = not args.no_truncate

    conn = _trino_connect()
    cur = conn.cursor()

    ensure_schema(cur)

    errors: list[str] = []
    for neon_tbl, iceberg_full in TABLES:
        if target_tables and neon_tbl not in target_tables:
            log.info("Skipping %s (not in --tables list).", neon_tbl)
            continue
        try:
            sync_table(cur, neon_tbl, iceberg_full, truncate)
        except Exception as exc:
            log.error("Failed to sync %s: %s", neon_tbl, exc)
            errors.append(neon_tbl)

    cur.close()
    conn.close()

    if errors:
        log.error("Sync finished with errors on: %s", ", ".join(errors))
        sys.exit(1)

    log.info("Bulk sync complete — all tables done.")


if __name__ == "__main__":
    main()
