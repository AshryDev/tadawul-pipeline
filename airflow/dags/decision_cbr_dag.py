"""
Decision CBR DAG
=================
Case-Based Reasoning outcome recorder — Retain step (Lecture 7 §7.8–7.10).

Daily DAG that completes the CBR cycle for historical decision signals:
  1. query_pending_signals  — find signals from 5–30 days ago without outcomes
  2. compute_outcomes       — look up actual forward prices; compute return + WIN/LOSS
  3. write_outcomes         — append to decision.case_outcomes via PyIceberg
  4. run_dbt_cbr            — refresh decision_cbr_lookup and decision_validation

Outcome definition:
  BUY  signal → WIN if return_5d > 0%,   else LOSS
  SELL signal → WIN if return_5d < 0%,   else LOSS
  HOLD signal → WIN if |return_5d| ≤ 1%, else LOSS
"""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
from datetime import datetime, timedelta

import pyarrow as pa
from airflow.decorators import dag, task

log = logging.getLogger("tadawul.cbr")

CBR_OUTCOMES_SCHEMA = pa.schema(
    [
        pa.field("symbol",          pa.string(),      nullable=False),
        pa.field("signal_date",     pa.string(),      nullable=False),
        pa.field("signal",          pa.string(),      nullable=False),
        pa.field("combined_cf",     pa.float64()),
        pa.field("signal_score",    pa.int32()),
        pa.field("sector",          pa.string()),
        pa.field("close_at_signal", pa.float64()),
        pa.field("close_5d",        pa.float64()),
        pa.field("close_20d",       pa.float64()),
        pa.field("return_5d",       pa.float64()),
        pa.field("return_20d",      pa.float64()),
        pa.field("outcome",         pa.string(),      nullable=False),
        pa.field("recorded_at",     pa.timestamp("us")),
    ]
)


def _trino_connect():
    import trino
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="airflow",
        catalog="iceberg",
    )


def _get_catalog():
    from pyiceberg.catalog import load_catalog
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg/"),
            "warehouse": os.environ.get("NESSIE_WAREHOUSE", "stocks"),
            "s3.endpoint": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": os.environ.get("MINIO_ACCESS_KEY", "admin"),
            "s3.secret-access-key": os.environ.get("MINIO_SECRET_KEY", "password"),
            "s3.path-style-access": "true",
            "s3.region": os.environ.get("MINIO_REGION", "eu-south-1"),
        },
    )


def _ensure_case_outcomes_table(catalog):
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DoubleType, IntegerType, NestedField, StringType, TimestampType,
    )
    from pyiceberg.partitioning import PartitionSpec

    try:
        catalog.create_namespace("decision")
    except Exception:
        pass

    try:
        return catalog.load_table("decision.case_outcomes")
    except Exception:
        pass

    schema = Schema(
        NestedField(1,  "symbol",          StringType(),    required=True),
        NestedField(2,  "signal_date",     StringType(),    required=True),
        NestedField(3,  "signal",          StringType(),    required=True),
        NestedField(4,  "combined_cf",     DoubleType()),
        NestedField(5,  "signal_score",    IntegerType()),
        NestedField(6,  "sector",          StringType()),
        NestedField(7,  "close_at_signal", DoubleType()),
        NestedField(8,  "close_5d",        DoubleType()),
        NestedField(9,  "close_20d",       DoubleType()),
        NestedField(10, "return_5d",       DoubleType()),
        NestedField(11, "return_20d",      DoubleType()),
        NestedField(12, "outcome",         StringType(),    required=True),
        NestedField(13, "recorded_at",     TimestampType()),
    )
    return catalog.create_table(
        identifier="decision.case_outcomes",
        schema=schema,
        partition_spec=PartitionSpec(),
    )


@dag(
    dag_id="decision_cbr_outcomes",
    description="CBR: record outcomes for historical decision signals (KBS §7.8–7.10 Retain step)",
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
    tags=["tadawul", "decision", "cbr", "kbs"],
)
def decision_cbr_dag():

    @task()
    def query_pending_signals(execution_date=None) -> list[dict]:
        """
        Find decision_signals from the past 5–30 days that do not yet have
        a recorded outcome in decision.case_outcomes.
        """
        today     = (execution_date or datetime.utcnow()).strftime("%Y-%m-%d")
        date_from = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        date_to   = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")

        sql = f"""
            SELECT
                s.symbol,
                CAST(s.date AS VARCHAR)    AS signal_date,
                s.signal,
                s.combined_cf,
                s.signal_score,
                s.sector,
                s.close                    AS close_at_signal
            FROM iceberg.decision.decision_signals s
            WHERE s.date BETWEEN DATE '{date_from}' AND DATE '{date_to}'
              AND NOT EXISTS (
                  SELECT 1
                  FROM iceberg.decision.case_outcomes o
                  WHERE o.symbol = s.symbol
                    AND o.signal_date = CAST(s.date AS VARCHAR)
              )
        """
        conn = _trino_connect()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            result = [dict(zip(cols, row)) for row in rows]
            log.info("Found %d pending signals to record outcomes for.", len(result))
            return result
        finally:
            cur.close()
            conn.close()

    @task()
    def compute_outcomes(pending: list[dict]) -> list[dict]:
        """
        Look up forward prices for each pending signal and compute return + outcome.
        Searches up to 5 calendar days ahead to account for weekends/holidays.
        """
        if not pending:
            log.info("No pending signals — skipping outcome computation.")
            return []

        symbols  = list({r["symbol"] for r in pending})
        dates    = [r["signal_date"] for r in pending]
        date_min = min(dates)
        date_max_plus = (
            datetime.strptime(max(dates), "%Y-%m-%d") + timedelta(days=30)
        ).strftime("%Y-%m-%d")

        sym_list = ", ".join(f"'{s}'" for s in symbols)
        sql = f"""
            SELECT
                CAST(symbol AS VARCHAR) AS symbol,
                CAST(date   AS VARCHAR) AS date,
                close
            FROM iceberg.silver.silver_ohlcv
            WHERE symbol IN ({sym_list})
              AND date BETWEEN DATE '{date_min}' AND DATE '{date_max_plus}'
        """
        conn = _trino_connect()
        cur  = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()

        price_map: dict[tuple[str, str], float] = {
            (str(r[0]), str(r[1])): float(r[2]) for r in rows
        }

        def nearest_close(symbol: str, signal_date: str, days_ahead: int) -> float | None:
            base = datetime.strptime(signal_date, "%Y-%m-%d")
            for offset in range(days_ahead, days_ahead + 5):
                candidate = (base + timedelta(days=offset)).strftime("%Y-%m-%d")
                if (symbol, candidate) in price_map:
                    return price_map[(symbol, candidate)]
            return None

        outcomes: list[dict] = []
        now = datetime.utcnow()

        for rec in pending:
            sym    = rec["symbol"]
            sdate  = rec["signal_date"]
            signal = rec["signal"]
            c0     = rec.get("close_at_signal") or price_map.get((sym, sdate))
            if not c0:
                log.debug("No close price for %s on %s — skipping.", sym, sdate)
                continue

            c5  = nearest_close(sym, sdate, 5)
            c20 = nearest_close(sym, sdate, 20)

            if c5 is None:
                log.debug("No 5d forward price for %s from %s — skipping.", sym, sdate)
                continue

            r5  = round((c5  - c0) / c0, 6)
            r20 = round((c20 - c0) / c0, 6) if c20 else None

            if signal == "BUY":
                outcome = "WIN" if r5 > 0 else "LOSS"
            elif signal == "SELL":
                outcome = "WIN" if r5 < 0 else "LOSS"
            else:  # HOLD
                outcome = "WIN" if abs(r5) <= 0.01 else "LOSS"

            outcomes.append({
                "symbol":          sym,
                "signal_date":     sdate,
                "signal":          signal,
                "combined_cf":     float(rec.get("combined_cf") or 0.0),
                "signal_score":    int(rec.get("signal_score") or 0),
                "sector":          rec.get("sector") or "",
                "close_at_signal": float(c0),
                "close_5d":        float(c5),
                "close_20d":       float(c20) if c20 else None,
                "return_5d":       r5,
                "return_20d":      r20,
                "outcome":         outcome,
                "recorded_at":     now,
            })

        log.info("Computed outcomes for %d / %d signals.", len(outcomes), len(pending))
        return outcomes

    @task()
    def write_outcomes(outcomes: list[dict]) -> None:
        """Append CBR outcome rows to decision.case_outcomes via PyIceberg."""
        if not outcomes:
            log.info("No outcomes to write.")
            return

        catalog = _get_catalog()
        table   = _ensure_case_outcomes_table(catalog)
        arrow_table = pa.Table.from_pylist(outcomes, schema=CBR_OUTCOMES_SCHEMA)
        table.append(arrow_table)
        log.info("Appended %d outcome rows to decision.case_outcomes.", len(outcomes))

    @task()
    def run_dbt_cbr(_upstream: None = None) -> None:
        """Run dbt CBR lookup and validation models after outcomes are written."""
        dbt_flags = (
            "--profiles-dir /opt/airflow/dbt "
            "--project-dir /opt/airflow/dbt "
            "--log-path /tmp/dbt-logs"
        )
        cmd = f"dbt run --select decision_cbr_lookup decision_validation {dbt_flags}"
        log.info("Running: %s", cmd)
        result = subprocess.run(
            shlex.split(cmd),
            capture_output=True,
            text=True,
            cwd="/opt/airflow/dbt",
        )
        if result.stdout:
            log.info("dbt stdout:\n%s", result.stdout)
        if result.stderr:
            log.warning("dbt stderr:\n%s", result.stderr)
        if result.returncode != 0:
            raise RuntimeError(
                f"dbt run failed (exit {result.returncode}).\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        log.info("dbt CBR models refreshed successfully.")

    # ── DAG wiring ─────────────────────────────────────────────────────────────
    pending  = query_pending_signals()
    computed = compute_outcomes(pending)
    written  = write_outcomes(computed)
    run_dbt_cbr(written)


decision_cbr_dag()
