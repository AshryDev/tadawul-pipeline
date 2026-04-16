"""
Airflow DAG: backfill_ohlcv
============================
Daily batch DAG that fetches daily OHLCV data from Yahoo Finance (yfinance)
for each Tadawul symbol and writes it to the Iceberg bronze_daily_ohlcv table
on MinIO via the Nessie catalog.

After ingestion, triggers dbt silver + gold model refresh.

Idempotency: Deletes the partition for the execution date before re-writing,
so re-running for the same date is always safe.

Data source: Yahoo Finance via the `yfinance` library. Tadawul symbols use
the `.SR` suffix (e.g. `2222.SR`). Missing data for a symbol on a given date
(market closed, holiday, symbol not covered) is handled gracefully — a warning
is logged and the symbol is skipped.
"""

from __future__ import annotations

import logging
import os
import subprocess
import shlex
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Any

import pandas as pd
import pyarrow as pa
import yfinance as yf
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# ── Symbols ───────────────────────────────────────────────────────────────────
SYMBOLS: list[str] = [
    s.strip()
    for s in os.getenv(
        "SYMBOLS", "2222,1010,2010,7010,1120,4280,2380,8010,4003,2060"
    ).split(",")
    if s.strip()
]


# ── PyArrow schema for bronze_daily_ohlcv ────────────────────────────────────
BRONZE_OHLCV_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("date", pa.string(), nullable=False),  # ISO-8601 date string
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("vwap", pa.float64()),
        pa.field("transactions", pa.int64()),
        pa.field("ingestion_time", pa.timestamp("us")),  # tz-naive matches Iceberg TimestampType
    ]
)


# ── Yahoo Finance helpers ─────────────────────────────────────────────────────

# Number of months each DAG run covers. Mirrors the 6-month schedule.
# Override via OHLCV_BATCH_MONTHS env var if needed.
BATCH_MONTHS: int = int(os.getenv("OHLCV_BATCH_MONTHS", "6"))


def _fetch_ohlcv_range(start_str: str, end_str: str) -> list[dict]:
    """
    Fetch OHLCV data for all symbols over a date range via Yahoo Finance.

    Uses ``yf.download()`` to pull all tickers in a single HTTP request,
    which is substantially faster than calling ``Ticker.history()`` per symbol.

    ``end_str`` is *exclusive* (Yahoo Finance convention).  Pass the day
    *after* the last date you want included.

    Returns a list of record dicts (one per symbol × trading day) ready to be
    written to bronze_daily_ohlcv.
    """
    ingestion_time = datetime.utcnow()
    yf_tickers = [f"{s}.SR" for s in SYMBOLS]

    log.info(
        "Downloading %d symbols from %s to %s (exclusive) via yf.download().",
        len(yf_tickers), start_str, end_str,
    )

    try:
        raw: pd.DataFrame = yf.download(
            tickers=yf_tickers,
            start=start_str,
            end=end_str,
            auto_adjust=True,
            group_by="ticker",
            threads=True,
            progress=False,
        )
    except Exception as exc:
        log.error("yf.download() failed for range %s–%s: %s", start_str, end_str, exc)
        raise

    if raw.empty:
        log.warning("yf.download() returned no data for %s–%s.", start_str, end_str)
        return []

    records: list[dict] = []

    for symbol in SYMBOLS:
        yf_ticker = f"{symbol}.SR"
        # When multiple tickers are requested, yf.download returns a
        # MultiIndex DataFrame with the ticker as the top-level column key.
        try:
            df = raw[yf_ticker] if len(yf_tickers) > 1 else raw
        except KeyError:
            log.warning("No data column for %s in download result.", yf_ticker)
            continue

        if df.empty:
            log.warning("No data for %s in range %s–%s.", symbol, start_str, end_str)
            continue

        for ts, row in df.iterrows():
            # Skip rows where all OHLCV values are NaN (non-trading days)
            if pd.isna(row.get("Open")):
                continue

            date_str = ts.strftime("%Y-%m-%d")
            open_  = float(row["Open"])
            high   = float(row["High"])
            low    = float(row["Low"])
            close  = float(row["Close"])
            volume = int(row["Volume"]) if not pd.isna(row["Volume"]) else 0
            # Yahoo Finance doesn't provide VWAP; approximate as OHLC average
            vwap   = round((open_ + high + low + close) / 4, 4)

            records.append({
                "symbol": symbol,
                "date": date_str,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "vwap": vwap,
                "transactions": 0,  # not provided by Yahoo Finance
                "ingestion_time": ingestion_time,
            })
            log.debug(
                "Fetched %s on %s: close=%.4f vwap=%.4f vol=%d",
                symbol, date_str, close, vwap, volume,
            )

    log.info("Collected %d records for range %s–%s.", len(records), start_str, end_str)
    return records


# ── PyIceberg catalog helper ──────────────────────────────────────────────────
def _get_catalog():
    """Return a PyIceberg REST catalog pointed at Nessie + MinIO."""
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg/"),
            # Nessie 0.100+ identifies warehouses by name, not by S3 URI.
            # The warehouse "stocks" is configured server-side in Nessie.
            "warehouse": os.environ.get("NESSIE_WAREHOUSE", "stocks"),
            "s3.endpoint": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": os.environ.get("MINIO_ACCESS_KEY", "admin"),
            "s3.secret-access-key": os.environ.get("MINIO_SECRET_KEY", "password"),
            "s3.path-style-access": "true",
            "s3.region": os.environ.get("MINIO_REGION", "eu-south-1"),
        },
    )


def _ensure_bronze_ohlcv_table(catalog) -> Any:
    """Create the bronze_daily_ohlcv Iceberg table if it doesn't exist."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DoubleType,
        IntegerType,
        LongType,
        NestedField,
        StringType,
        TimestampType,
    )
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform

    try:
        catalog.create_namespace("bronze")
    except Exception:
        pass  # namespace already exists

    try:
        return catalog.load_table("bronze.bronze_daily_ohlcv")
    except Exception:
        pass

    schema = Schema(
        NestedField(1, "symbol", StringType(), required=True),
        NestedField(2, "date", StringType(), required=True),
        NestedField(3, "open", DoubleType()),
        NestedField(4, "high", DoubleType()),
        NestedField(5, "low", DoubleType()),
        NestedField(6, "close", DoubleType()),
        NestedField(7, "volume", LongType()),
        NestedField(8, "vwap", DoubleType()),
        NestedField(9, "transactions", LongType()),
        NestedField(10, "ingestion_time", TimestampType()),
    )

    # Partition by date (identity transform on string date)
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="date")
    )

    return catalog.create_table(
        identifier="bronze.bronze_daily_ohlcv",
        schema=schema,
        partition_spec=partition_spec,
    )


# ── DAG definition ────────────────────────────────────────────────────────────
@dag(
    dag_id="backfill_ohlcv",
    description="6-monthly OHLCV ingestion from Yahoo Finance → Iceberg bronze_daily_ohlcv (MinIO/Nessie) → dbt silver/gold",
    # Runs every 6 months (Jan 1 and Jul 1). Each run covers a 6-month window
    # starting at the execution date, so the full backfill needs ~2 runs/year.
    schedule="0 0 1 1,7 *",
    start_date=datetime(2021, 1, 1),
    catchup=True,
    max_active_runs=2,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
    tags=["tadawul", "bronze", "batch", "yfinance"],
)
def backfill_ohlcv_dag():

    @task()
    def fetch_ohlcv(execution_date=None) -> list[dict]:
        """
        Fetch OHLCV for all symbols over a BATCH_MONTHS window starting at the
        DAG execution date.  Uses yf.download() for a single bulk HTTP request
        instead of one Ticker.history() call per symbol.
        """
        start_str = execution_date.strftime("%Y-%m-%d")
        end_dt = execution_date + relativedelta(months=BATCH_MONTHS)
        end_str = end_dt.strftime("%Y-%m-%d")  # exclusive upper bound for yfinance

        log.info(
            "Fetching OHLCV for window %s – %s (%d months, %d symbols).",
            start_str, end_str, BATCH_MONTHS, len(SYMBOLS),
        )
        records = _fetch_ohlcv_range(start_str, end_str)
        log.info("Fetched %d records for window %s – %s.", len(records), start_str, end_str)
        return records

    @task()
    def write_bronze(records: list[dict], execution_date=None) -> None:  # type: ignore[return]
        """
        Write records to Iceberg bronze_daily_ohlcv.

        Idempotent: for each distinct date present in the batch, deletes the
        existing partition before appending, so re-running for the same window
        produces correct results.
        """
        from pyiceberg.expressions import EqualTo

        if not records:
            log.warning("No records to write — skipping bronze write.")
            return

        catalog = _get_catalog()
        table = _ensure_bronze_ohlcv_table(catalog)

        # Group records by date so we write (and delete) one partition at a time.
        dates_seen: dict[str, list[dict]] = {}
        for rec in records:
            dates_seen.setdefault(rec["date"], []).append(rec)

        log.info(
            "Writing %d records across %d date partitions to bronze_daily_ohlcv.",
            len(records), len(dates_seen),
        )

        for date_str, date_records in sorted(dates_seen.items()):
            # Delete existing partition for this date (idempotency)
            try:
                table.delete(EqualTo("date", date_str))
                log.info("Deleted existing partition for %s.", date_str)
            except Exception as exc:
                log.debug("Delete partition skipped for %s (may not exist): %s", date_str, exc)

            arrow_table = pa.Table.from_pylist(date_records, schema=BRONZE_OHLCV_SCHEMA)
            table.append(arrow_table)
            log.info("Appended %d rows for %s.", len(date_records), date_str)

        log.info("Successfully wrote all %d rows to bronze_daily_ohlcv.", len(records))

    @task()
    def run_dbt(_upstream: None = None) -> None:
        """
        Run dbt silver and gold models.

        Invokes dbt as a subprocess so it runs in the same Python environment
        where dbt-trino is installed (inside the Airflow container).

        _upstream is a dummy parameter that carries the write_bronze XCom so
        Airflow creates an explicit task dependency (run_dbt waits for
        write_bronze to succeed before it starts).

        --log-path /tmp/dbt-logs redirects dbt's rotating log file out of the
        host-mounted ./dbt volume, which the airflow user (uid 50000) cannot
        write to.
        """
        dbt_flags = (
            "--profiles-dir /opt/airflow/dbt "
            "--project-dir /opt/airflow/dbt "
            "--log-path /tmp/dbt-logs"
        )

        for label, cmd in [
            ("dbt deps", f"dbt deps {dbt_flags}"),
            # Exclude tick-dependent models (silver_ticks_cleaned,
            # gold_intraday_vwap) — those require bronze_ticks which is
            # written by the Spark streaming consumer, not this batch DAG.
            ("dbt run",  f"dbt run --select silver_ohlcv+ silver_symbols {dbt_flags}"),
        ]:
            log.info("Running: %s", cmd)
            result = subprocess.run(
                shlex.split(cmd),
                capture_output=True,
                text=True,
                cwd="/opt/airflow/dbt",
            )
            if result.stdout:
                log.info("%s stdout:\n%s", label, result.stdout)
            if result.stderr:
                log.warning("%s stderr:\n%s", label, result.stderr)
            if result.returncode != 0:
                raise RuntimeError(
                    f"{label} failed with exit code {result.returncode}.\n"
                    f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
                )

        log.info("dbt run completed successfully.")

    # ── Task dependencies ─────────────────────────────────────────────────────
    records     = fetch_ohlcv()
    bronze_done = write_bronze(records)
    run_dbt(bronze_done)


backfill_ohlcv_dag()
