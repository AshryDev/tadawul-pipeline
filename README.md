# Tadawul Real-Time Market Analytics

A dual-mode financial analytics platform for Saudi Arabia's stock exchange (Tadawul).
Combines a real-time Kafka/Spark streaming pipeline with a historical Airflow/dbt batch
lakehouse — both landing in Apache Iceberg tables on a local MinIO object store (Nessie
catalog), with gold-layer results synced to Amazon S3 for cloud persistence.

## Architecture

```
                       ┌─────────────────────────────────────────────┐
                       │            REAL-TIME PIPELINE                │
  Market Simulation ──► Kafka Producer ──► Kafka (tadawul.ticks)
                                                  │
                                        Spark Structured Streaming
                                                  │
                                  Iceberg bronze_ticks (MinIO / Nessie)
                       └─────────────────────────────────────────────┘

                       ┌─────────────────────────────────────────────┐
                       │            BATCH PIPELINE                    │
  Yahoo Finance ──────► Airflow DAG ──► Iceberg bronze_daily_ohlcv
  (yfinance .SR)                              (MinIO / Nessie)
                                                  │
                                        dbt run (via Trino)
                                                  │
                        Silver: silver_ohlcv, silver_ticks_cleaned, silver_symbols
                        Gold:   gold_intraday_vwap, gold_sector_performance,
                                gold_volatility_index, gold_anomaly_flags, gold_52w_levels
                                                  │
                                         MongoDB (serving layer)
                                                  │
                                         Amazon S3 (cloud sync)
                       └─────────────────────────────────────────────┘
```

**Infrastructure:**
- **Local (Docker):** Kafka, Spark, Airflow, Trino, MinIO, Nessie, MongoDB, dbt
- **Cloud (AWS):** S3 bucket — receives gold-layer results synced from MinIO

---

## Tracked Symbols

| Code | Company | Sector |
|------|---------|--------|
| 2222 | Saudi Aramco | Energy |
| 1010 | Al Rajhi Bank | Banking |
| 2010 | SABIC | Petrochemicals |
| 7010 | STC | Telecom |
| 1120 | Al Jazira Bank | Banking |
| 4280 | Jarir Bookstore | Retail |
| 2380 | Petro Rabigh | Petrochemicals |
| 8010 | SABB | Banking |
| 4003 | Bahri | Transportation |
| 2060 | Saudi Kayan | Petrochemicals |

Yahoo Finance tickers use the `.SR` suffix (e.g. `2222.SR` for Saudi Aramco).

---

## Prerequisites

- **Docker Desktop** 24+ (with Compose v2)
- **AWS account** (free tier is sufficient) — for the cloud sync target:
  - Create an S3 bucket (e.g. `tadawul-lakehouse`) in your preferred region
  - Create an IAM user with `AmazonS3FullAccess`
  - Note the access key and secret key
- **Python 3.11** (only needed to run the producer locally; not required for Docker services)
- **curl** (for downloading Spark JARs)

> No external market data API key is required. Historical OHLCV data is fetched
> for free from Yahoo Finance via `yfinance`.

---

## Step 1 — Clone and Configure

```bash
git clone <your-repo-url> tadawul
cd tadawul

# Copy the example env file and fill in your credentials
cp .env.example .env
```

Edit `.env` and set the following (at minimum):
```
# AWS — used only for gold-layer cloud sync
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=me-south-1
S3_BUCKET_NAME=tadawul-lakehouse

# Airflow
AIRFLOW__CORE__FERNET_KEY=   # see generation command below
AIRFLOW__WEBSERVER__SECRET_KEY=
```

Generate the Fernet key:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

> MinIO and MongoDB credentials are hardcoded in `docker-compose.yml`
> (`admin` / `password` and `root` / `password` respectively). Change them
> for any non-local deployment.

---

## Step 2 — Download Spark JARs

These JARs are required by the Spark Structured Streaming consumer and are
**not included in the repo** (too large; see `.gitignore`).

```bash
mkdir -p spark/jars

# Iceberg Spark runtime (Spark 3.5, Scala 2.12)
curl -L -o spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

# Iceberg AWS bundle (S3FileIO + AWS SDK v2)
curl -L -o spark/jars/iceberg-aws-bundle-1.4.3.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.3/iceberg-aws-bundle-1.4.3.jar
```

---

## Step 3 — Start Infrastructure

```bash
# Start core messaging + metadata services first
docker compose up -d zookeeper kafka postgres
docker compose ps   # wait until all show "healthy"

# Create Kafka topics (one-shot container, exits when done)
docker compose up kafka-setup

# Start the lakehouse services
docker compose up -d minio nessie mongo

# Start everything else
docker compose up -d
```

Check all services are up:

```bash
docker compose ps
```

Expected healthy services:

| Service | Port |
|---------|------|
| Kafka | 9092 |
| PostgreSQL | 5432 |
| MinIO API | 9000 |
| MinIO Console | http://localhost:9091 |
| Nessie | http://localhost:19120 |
| MongoDB | 27017 |
| Spark Master UI | http://localhost:8081 |
| Airflow UI | http://localhost:9093 |
| Trino UI | http://localhost:8080 |

---

## Step 4 — Initialize Airflow

Open the Airflow UI at **http://localhost:9093** (admin / admin).

Navigate to **Admin → Connections** and add a Trino connection:

```
Conn ID:   trino_default
Conn Type: Trino
Host:      trino
Port:      8080
Schema:    iceberg
```

Or via CLI:
```bash
docker exec airflow-scheduler airflow connections add trino_default \
  --conn-type trino --conn-host trino --conn-port 8080 --conn-schema iceberg
```

---

## Step 5 — Run the Historical Backfill

In the Airflow UI:
1. Navigate to **DAGs → backfill_ohlcv**
2. Toggle the DAG to **Active**
3. The DAG has `catchup=True` and `start_date=2021-01-01` — Airflow schedules
   a run for every calendar day from 2021-01-01 to today.

Each daily run:
1. Fetches OHLCV for all 10 symbols from Yahoo Finance (`{symbol}.SR`)
2. Writes to `bronze.bronze_daily_ohlcv` (Iceberg on MinIO, Nessie catalog)
3. Runs `dbt run --select silver+ gold+` via Trino

> Yahoo Finance data availability varies by symbol. Missing dates (market
> holidays, weekends, or unavailable history) are logged as warnings and
> skipped — the DAG does not fail.

---

## Step 6 — Start the Kafka Producer

The producer simulates Tadawul market ticks every 3 seconds. During Tadawul
trading hours (Sun–Thu 10:00–15:00 Riyadh time) it uses realistic price
simulation; outside hours it falls back to a random-walk model.

**Option A — run locally:**
```bash
cd producer
pip install -r requirements.txt
python kafka_producer.py
```

**Option B — run in Docker:**
```bash
docker run --rm --network tadawul-pipeline_tadawul_net \
  --env-file .env \
  -v $(pwd)/producer:/app \
  python:3.11-slim \
  bash -c "pip install -r /app/requirements.txt -q && python /app/kafka_producer.py"
```

Verify messages are flowing:
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tadawul.ticks \
  --from-beginning \
  --max-messages 5
```

---

## Step 7 — Start the Spark Streaming Consumer

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/extra/iceberg-spark-runtime-3.5_2.12-1.4.3.jar,/opt/spark/jars/extra/iceberg-aws-bundle-1.4.3.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  /opt/spark-app/streaming_consumer.py
```

The consumer writes to `bronze.bronze_ticks` every 10 seconds.
Monitor the Spark UI at **http://localhost:8081**.

Verify data is landing:
```bash
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.bronze.bronze_ticks"
```

---

## Step 8 — Run dbt Transformations

dbt runs automatically as the last task of each `backfill_ohlcv` DAG run.
To trigger it manually:

```bash
# Run all silver and gold models
docker exec dbt dbt run --select silver+ gold+ \
  --project-dir /usr/dbt --profiles-dir /root/.dbt

# Run tests
docker exec dbt dbt test \
  --project-dir /usr/dbt --profiles-dir /root/.dbt

# Check source freshness
docker exec dbt dbt source freshness \
  --project-dir /usr/dbt --profiles-dir /root/.dbt
```

---

## Step 9 — Verify End-to-End

```bash
# 1. Kafka — live tick stream
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tadawul.ticks --max-messages 3

# 2. Bronze — Spark writes
docker exec trino trino --execute \
  "SELECT symbol, COUNT(*) AS ticks FROM iceberg.bronze.bronze_ticks GROUP BY symbol"

# 3. Bronze — Airflow batch writes
docker exec trino trino --execute \
  "SELECT symbol, COUNT(*) AS days FROM iceberg.bronze.bronze_daily_ohlcv GROUP BY symbol"

# 4. Silver — dbt models
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.silver.silver_ohlcv"

# 5. Gold — volatility index
docker exec trino trino --execute \
  "SELECT symbol, date, ROUND(annualized_vol*100,1) AS vol_pct
   FROM iceberg.gold.gold_volatility_index
   ORDER BY date DESC LIMIT 10"

# 6. Gold — anomaly flags
docker exec trino trino --execute \
  "SELECT symbol, date, anomaly_type, score, is_anomaly
   FROM iceberg.gold.gold_anomaly_flags
   WHERE is_anomaly = true
   ORDER BY date DESC LIMIT 10"

# 7. Nessie — check catalog branches
curl http://localhost:19120/api/v2/trees
```

---

## ML Anomaly Detection

The `ml_anomaly_detection` Airflow DAG runs daily after the backfill and:

1. **Reads** `gold_volatility_index` + `silver_ohlcv` via Trino
2. **Volume Z-score**: flags symbols where volume > 2.5σ above 30-day mean
3. **Price IQR**: flags daily returns outside Q1 ± 1.5×IQR over 90-day window
4. **Writes** results to `iceberg.gold.gold_anomaly_flags` (queryable via Trino)

Enable in Airflow UI: **DAGs → ml_anomaly_detection → Active**

---

## dbt Model Dependency Chain

```
sources (bronze_ticks, bronze_daily_ohlcv — MinIO / Nessie)
  └── silver_ticks_cleaned, silver_ohlcv, silver_symbols
        └── gold_intraday_vwap, gold_sector_performance
        └── gold_volatility_index
              └── gold_anomaly_flags, gold_52w_levels
```

---

## Troubleshooting

**Airflow scheduler not starting:**
Make sure `AIRFLOW__CORE__FERNET_KEY` is set in `.env`. Generate with:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**MinIO bucket not created:**
The MinIO healthcheck auto-creates the `stocks` bucket. If it fails, create it manually:
```bash
docker exec minio mc alias set local http://localhost:9000 admin password
docker exec minio mc mb local/stocks
```

**Trino "Table not found":**
Nessie must be healthy and the Iceberg table must have been written at least once.
Check Nessie is reachable: `curl http://localhost:19120/api/v2/config`

**Nessie failing to start:**
Nessie depends on MongoDB. Verify MongoDB is healthy:
```bash
docker compose ps mongo
```

**dbt "Schema does not exist":**
Without `dbt/macros/generate_schema_name.sql`, dbt-trino prefixes all schemas
with the target schema. Verify the macro file exists at `dbt/macros/generate_schema_name.sql`.

**yfinance returning no data:**
Yahoo Finance may throttle bulk historical requests. The DAG retries automatically.
For symbols with no `.SR` data on a given date (weekend, market holiday), a warning
is logged and the date is skipped gracefully.
