"""
Tadawul Kafka Producer
======================
Polls Polygon.io every 3 seconds for real-time OHLCV tick data for Tadawul
(Saudi Exchange) stocks. Falls back to a realistic random-walk simulation when
the market is closed or the API is unavailable.

Produces JSON messages to the tadawul.ticks Kafka topic, keyed by symbol.

Environment variables (all required unless noted):
    POLYGON_API_KEY          — Polygon.io API key
    KAFKA_BOOTSTRAP_SERVERS  — e.g. "localhost:9092" (default)
    KAFKA_TOPIC_TICKS        — topic name (default: "tadawul.ticks")
    SYMBOLS                  — comma-separated Tadawul codes, e.g. "2222,1010"
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from datetime import time as dtime
from functools import partial

import numpy as np
import pytz
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

# tadawul_symbols.py lives in airflow/dags/ — resolve relative to this file.
_dags_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "airflow", "dags")
if _dags_dir not in sys.path:
    sys.path.insert(0, _dags_dir)
from tadawul_symbols import BASE_PRICES, get_symbols  # noqa: E402

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("tadawul.producer")

# ── Configuration ─────────────────────────────────────────────────────────────
POLYGON_API_KEY: str = os.environ["POLYGON_API_KEY"]
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TICKS: str = os.getenv("KAFKA_TOPIC_TICKS", "tadawul.ticks")
POLL_INTERVAL_SECONDS: float = 3.0
POLYGON_BASE_URL: str = "https://api.polygon.io/v2/aggs/ticker"

SYMBOLS: list[str] = get_symbols()

# Number of threads used to build ticks in parallel (Polygon calls or simulation).
# confluent_kafka.Producer.produce() is NOT thread-safe; ticks are collected
# from the thread pool then produced from the main thread.
_TICK_WORKERS: int = min(32, len(SYMBOLS))

# ── Market hours (Tadawul: Sun–Thu, 10:00–15:00 Riyadh time = UTC+3) ─────────
RIYADH_TZ = pytz.timezone("Asia/Riyadh")
MARKET_OPEN = dtime(10, 0)
MARKET_CLOSE = dtime(15, 0)
# Python weekday(): Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
TRADING_DAYS: frozenset[int] = frozenset({6, 0, 1, 2, 3})  # Sun through Thu

# ── Simulation state (module-level for continuity across ticks) ───────────────
# BASE_PRICES is imported from tadawul_symbols; unknown symbols default to 100.0.
_last_prices: dict[str, float] = dict(BASE_PRICES)

# Graceful shutdown flag
_running = True


# ── Market hours ──────────────────────────────────────────────────────────────
def is_market_open() -> bool:
    """Return True if Tadawul is currently in a trading session."""
    now_riyadh = datetime.now(RIYADH_TZ)
    return (
        now_riyadh.weekday() in TRADING_DAYS
        and MARKET_OPEN <= now_riyadh.time() <= MARKET_CLOSE
    )


# ── Polygon.io ────────────────────────────────────────────────────────────────
def fetch_polygon_ticks(symbol: str) -> list[dict]:
    """
    Fetch the last minute's bar from Polygon.io for a Tadawul symbol.
    Returns a list of tick dicts (may be empty if no data for the period).

    Uses exponential backoff on HTTP 429 (rate limit) up to 60 seconds.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    url = f"{POLYGON_BASE_URL}/X:{symbol}/range/1/minute/{yesterday}/{today}"
    params = {
        "adjusted": "true",
        "sort": "desc",
        "limit": "1",
        "apiKey": POLYGON_API_KEY,
    }

    retry_delay = 2.0
    while True:
        try:
            resp = requests.get(url, params=params, timeout=10)
        except requests.RequestException as exc:
            log.warning("Network error fetching %s: %s", symbol, exc)
            return []

        if resp.status_code == 200:
            results = resp.json().get("results", [])
            return results

        if resp.status_code == 429:
            log.warning("Rate limited by Polygon. Backing off %.1fs …", retry_delay)
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2.0, 60.0)
            continue

        if resp.status_code in (403, 404):
            # Free tier doesn't cover this symbol / time range — use simulation
            log.debug("Polygon returned %d for %s — using simulation.", resp.status_code, symbol)
            return []

        log.error("Unexpected Polygon response %d for %s: %s", resp.status_code, symbol, resp.text)
        return []


def polygon_bar_to_tick(symbol: str, bar: dict) -> dict:
    """Convert a Polygon aggregate bar dict to a tadawul.ticks payload."""
    close_price = float(bar["c"])
    spread = close_price * 0.001  # 0.1% synthetic spread
    _last_prices[symbol] = close_price
    return {
        "symbol": symbol,
        "price": round(close_price, 4),
        "volume": int(bar.get("v", 0)),
        "bid": round(close_price - spread / 2, 4),
        "ask": round(close_price + spread / 2, 4),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "session_status": "live",
    }


# ── Simulation ────────────────────────────────────────────────────────────────
def simulate_tick(symbol: str) -> dict:
    """
    Generate a realistic random-walk tick for a symbol.

    Price drift: normally distributed, μ=0, σ=0.1% per tick.
    Volume:      normally distributed around 10M shares, σ=2M, clipped at 1.
    Spread:      0.1% of mid-price (realistic for liquid Tadawul stocks).
    """
    base = _last_prices.get(symbol, BASE_PRICES.get(symbol, 100.0))
    drift = float(np.random.normal(0.0, 0.001))
    new_price = max(round(base * (1.0 + drift), 4), 0.01)
    _last_prices[symbol] = new_price

    spread = new_price * 0.001
    volume = max(int(np.random.normal(10_000_000, 2_000_000)), 1)

    return {
        "symbol": symbol,
        "price": new_price,
        "volume": volume,
        "bid": round(new_price - spread / 2, 4),
        "ask": round(new_price + spread / 2, 4),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "session_status": "simulated",
    }


def build_tick(symbol: str, market_open: bool) -> dict:
    """Return a live or simulated tick depending on market state."""
    if market_open:
        bars = fetch_polygon_ticks(symbol)
        if bars:
            return polygon_bar_to_tick(symbol, bars[0])
        # API returned no data even though market is open — fall back
        log.debug("No Polygon data for %s, using simulation.", symbol)
    return simulate_tick(symbol)


# ── Kafka ─────────────────────────────────────────────────────────────────────
def delivery_callback(err, msg) -> None:
    if err:
        log.error("Delivery failed | topic=%s key=%s error=%s", msg.topic(), msg.key(), err)
    else:
        log.debug(
            "Delivered | topic=%s partition=%d offset=%d key=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.key().decode(),
        )


def create_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "tadawul-market-producer",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "compression.type": "snappy",
        "linger.ms": 10,
        "batch.size": 65536,
    }
    return Producer(conf)


# ── Shutdown handler ──────────────────────────────────────────────────────────
def _signal_handler(sig, frame) -> None:
    global _running
    log.info("Received signal %d — shutting down gracefully.", sig)
    _running = False


# ── Main loop ─────────────────────────────────────────────────────────────────
def main() -> None:
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    log.info("Starting Tadawul producer. Symbols: %s", SYMBOLS)
    log.info("Kafka: %s  Topic: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TICKS)

    producer = create_producer()

    try:
        while _running:
            loop_start = time.monotonic()
            open_flag = is_market_open()
            status_label = "LIVE" if open_flag else "SIMULATED"

            # Build all ticks in parallel (parallelises Polygon I/O during live
            # sessions; negligible overhead during simulation).
            _build = partial(build_tick, market_open=open_flag)
            with ThreadPoolExecutor(max_workers=_TICK_WORKERS) as pool:
                ticks = list(pool.map(_build, SYMBOLS))

            # Produce from main thread — confluent_kafka Producer is not thread-safe.
            published = 0
            for symbol, tick in zip(SYMBOLS, ticks):
                if tick is None:
                    continue
                payload = json.dumps(tick).encode("utf-8")
                producer.produce(
                    topic=KAFKA_TOPIC_TICKS,
                    key=symbol.encode("utf-8"),
                    value=payload,
                    callback=delivery_callback,
                )
                producer.poll(0)
                published += 1

            log.info(
                "Published %d ticks [%s] to %s",
                published,
                status_label,
                KAFKA_TOPIC_TICKS,
            )

            elapsed = time.monotonic() - loop_start
            sleep_for = max(0.0, POLL_INTERVAL_SECONDS - elapsed)
            time.sleep(sleep_for)

    finally:
        log.info("Flushing remaining messages …")
        producer.flush(timeout=30)
        log.info("Producer shut down cleanly.")


if __name__ == "__main__":
    main()
