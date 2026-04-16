"""
Airflow DAG: sync_to_neon
==========================
Exports Iceberg lakehouse data to Neon PostgreSQL by routing all SQL through
Trino, which already holds an open connection to Neon via the PostgreSQL
connector configured in trino/catalog/neon.properties.

Uses TrinoHook (airflow.providers.trino) so Trino connection settings are
managed via the Airflow connection store (AIRFLOW_CONN_TRINO_CONN env var)
rather than hard-coded in the DAG.

Tables synced
─────────────
Silver:
  • silver_symbols   — company / sector metadata       (full replace, no date)
  • silver_ohlcv     — cleaned daily OHLCV bars         (window delete + insert)

Gold:
  • gold_52w_levels           — 52-week high/low analysis  (window delete + insert)
  • gold_anomaly_flags        — volume/price anomaly flags  (window delete + insert)
  • gold_sector_performance   — sector rotation metrics     (window delete + insert)
  • gold_volatility_index     — rolling volatility          (window delete + insert)
  • gold_intraday_vwap        — intraday VWAP from ticks    (window delete + insert)

All Neon tables live in the `public` schema → accessed as neon.public.<table>.

Task flow
─────────
  ensure_schema
       │
       ├── sync_silver_symbols          (DELETE all + INSERT)
       └── sync_date_partitioned_tables (DELETE window + INSERT)

Scheduling
──────────
@weekly, catchup=True, aligned with backfill_ohlcv (same start_date, same
BATCH_DAYS window).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook

log = logging.getLogger(__name__)

BATCH_DAYS: int = int(os.getenv("OHLCV_BATCH_DAYS", "7"))
TRINO_CONN_ID: str = "trino_conn"

# ── DDL ───────────────────────────────────────────────────────────────────────
# Written in Trino SQL types (VARCHAR, DOUBLE, BIGINT …).
# Trino's PostgreSQL connector translates these to native Postgres types when
# it forwards the CREATE TABLE to Neon.

_DDL: list[str] = [
    # ── Silver ────────────────────────────────────────────────────────────────
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
    # ── Gold ──────────────────────────────────────────────────────────────────
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
]


# ── DAG definition ────────────────────────────────────────────────────────────

@dag(
    dag_id="sync_to_neon",
    description="Weekly sync of silver + gold Iceberg tables to Neon PostgreSQL via Trino",
    schedule="@weekly",
    start_date=datetime(2021, 1, 4),   # aligned with backfill_ohlcv
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
    tags=["tadawul", "neon", "postgres", "export", "silver", "gold"],
)
def sync_to_neon_dag():

    @task()
    def ensure_schema() -> None:
        """
        Create all Neon tables if they don't already exist.

        Issues CREATE TABLE IF NOT EXISTS through Trino's neon catalog.
        Safe to run on every execution — already-existing tables are skipped.
        """
        hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)

        for ddl in _DDL:
            table_hint = ddl.strip().split("neon.public.")[1].split()[0]
            try:
                hook.run(ddl)
                log.info("Table ready: neon.public.%s", table_hint)
            except Exception as exc:
                # A pre-existing table with a mismatched schema will surface
                # as an error in the sync tasks rather than here.
                log.warning("Could not create neon.public.%s: %s", table_hint, exc)

        log.info("Schema check complete.")

    @task()
    def sync_silver_symbols() -> None:
        """
        Replace silver_symbols in Neon with the current Iceberg snapshot.

        silver_symbols is a static 10-row dimension table — a full delete +
        insert is simpler and safer than a windowed sync.
        """
        hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)

        hook.run("DELETE FROM neon.public.silver_symbols")
        log.info("silver_symbols: existing rows deleted.")

        hook.run(
            "INSERT INTO neon.public.silver_symbols "
            "SELECT * FROM iceberg.silver.silver_symbols"
        )
        log.info("silver_symbols: synced from Iceberg.")

    @task()
    def sync_date_partitioned_tables(execution_date=None) -> None:
        """
        Sync the [execution_date, execution_date + BATCH_DAYS) window for every
        date-partitioned table.

        For each table: delete the window from Neon, then re-insert from
        Iceberg via Trino. Idempotent — re-running the same window always
        produces the correct result.

        Tables and their date-partition columns:
            silver_ohlcv            → date
            gold_52w_levels         → date
            gold_anomaly_flags      → date
            gold_sector_performance → date
            gold_volatility_index   → date
            gold_intraday_vwap      → session_date
        """
        start_str = execution_date.strftime("%Y-%m-%d")
        end_str   = (execution_date + timedelta(days=BATCH_DAYS)).strftime("%Y-%m-%d")

        # (neon_table, iceberg_schema, iceberg_table, date_col)
        TABLES = [
            ("silver_ohlcv",            "silver", "silver_ohlcv",            "date"),
            ("gold_52w_levels",         "gold",   "gold_52w_levels",          "date"),
            ("gold_anomaly_flags",      "gold",   "gold_anomaly_flags",        "date"),
            ("gold_sector_performance", "gold",   "gold_sector_performance",   "date"),
            ("gold_volatility_index",   "gold",   "gold_volatility_index",     "date"),
            ("gold_intraday_vwap",      "gold",   "gold_intraday_vwap",        "session_date"),
        ]

        log.info("Syncing window %s – %s (%d tables).", start_str, end_str, len(TABLES))

        hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)

        for neon_tbl, ice_schema, ice_tbl, date_col in TABLES:
            # Check the table exists in Iceberg before attempting to sync.
            # gold_intraday_vwap (and any other streaming-dependent model) may
            # not be materialised yet if the Spark pipeline hasn't run.
            exists_rows = hook.get_records(
                f"SELECT COUNT(*) FROM iceberg.information_schema.tables "  # noqa: S608
                f"WHERE table_schema = '{ice_schema}' "
                f"AND table_name = '{ice_tbl}'"
            )
            if not exists_rows or exists_rows[0][0] == 0:
                log.warning(
                    "%s: iceberg.%s.%s not found — skipping (run dbt to materialise).",
                    neon_tbl, ice_schema, ice_tbl,
                )
                continue

            window = (
                f"{date_col} >= DATE '{start_str}' "
                f"AND {date_col} < DATE '{end_str}'"
            )

            hook.run(f"DELETE FROM neon.public.{neon_tbl} WHERE {window}")

            hook.run(
                f"INSERT INTO neon.public.{neon_tbl} "          # noqa: S608
                f"SELECT * FROM iceberg.{ice_schema}.{ice_tbl} "
                f"WHERE {window}"
            )

            log.info("%s: window %s – %s synced.", neon_tbl, start_str, end_str)

        log.info("Date-partitioned sync complete for window %s – %s.", start_str, end_str)

    # ── Task dependencies ─────────────────────────────────────────────────────
    schema_ready = ensure_schema()
    schema_ready >> [sync_silver_symbols(), sync_date_partitioned_tables()]


sync_to_neon_dag()
