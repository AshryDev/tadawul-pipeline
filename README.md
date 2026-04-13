# Tadawul Real-Time Market Analytics

A dual-mode financial analytics platform for Saudi Arabia's stock exchange (Tadawul).
Combines a real-time Kafka/Spark streaming pipeline with a historical Airflow/dbt batch
lakehouse — both landing in the same Apache Iceberg tables on AWS S3.

## Architecture

```
                       ┌─────────────────────────────────────────┐
                       │          REAL-TIME PIPELINE              │
  Polygon.io API ──►  Kafka Producer  ──►  Kafka (tadawul.ticks)
  (or simulation)                              │
                                         Spark Structured Streaming
                                               │
                                         Iceberg bronze_ticks (S3)
                                               │
                                          Grafana Dashboard ◄── PostgreSQL (anomaly write-back)
                       └─────────────────────────────────────────┘

                       ┌─────────────────────────────────────────┐
                       │          BATCH PIPELINE                  │
  Polygon.io API ──►  Airflow DAG  ──►  Iceberg bronze_daily_ohlcv (S3)
                           │
                        dbt run (via Trino)
                           │
                    Silver: silver_ohlcv, silver_ticks_cleaned, silver_symbols
                    Gold:   gold_intraday_vwap, gold_sector_performance,
                            gold_volatility_index, gold_anomaly_flags, gold_52w_levels
                           │
                      Apache Superset ◄── Trino (unified query engine)
                       └─────────────────────────────────────────┘
```

**Infrastructure:**
- Local (Docker): Kafka, Spark, Airflow, Trino, Grafana, PostgreSQL, Superset
- Cloud (AWS free tier): S3 (table storage) + Glue Catalog (Iceberg metastore)

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

---

## Prerequisites

- **Docker Desktop** 24+ (with Compose v2)
- **AWS account** — free tier is sufficient
  - Create an S3 bucket (e.g. `tadawul-lakehouse`) in `me-south-1`
  - Create an IAM user with `AmazonS3FullAccess` + `AWSGlueConsoleFullAccess`
  - Note the access key and secret key
- **Polygon.io** free API key — sign up at polygon.io
- **Python 3.11** (only needed to run the producer locally; not required for Docker services)
- **curl** (for downloading Spark JARs)

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
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=me-south-1
S3_BUCKET_NAME=tadawul-lakehouse
POLYGON_API_KEY=...
AIRFLOW__CORE__FERNET_KEY=   # see generation command in .env.example
AIRFLOW__WEBSERVER__SECRET_KEY=
SUPERSET_SECRET_KEY=
```

---

## Step 2 — Download Spark JARs

These JARs are required by the Spark Structured Streaming consumer. They are
**not included in the repo** (too large; see `.gitignore`).

```bash
mkdir -p spark/jars

# Iceberg Spark runtime (Spark 3.5, Scala 2.12)
curl -L -o spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

# Iceberg AWS bundle (AWS SDK v2 — includes Glue + S3FileIO)
curl -L -o spark/jars/iceberg-aws-bundle-1.4.3.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.3/iceberg-aws-bundle-1.4.3.jar
```

> **Warning:** Do NOT add `aws-java-sdk-bundle` (SDK v1). It conflicts with
> `iceberg-aws-bundle` which uses AWS SDK v2.

---

## Step 3 — Start Infrastructure

Start the core services first and wait for them to be healthy:

```bash
# Start Zookeeper + Kafka + PostgreSQL first
docker compose up -d zookeeper kafka postgres
docker compose ps   # wait until all show "healthy"

# Create Kafka topics (one-shot container, exits when done)
docker compose up kafka-setup

# Start everything else
docker compose up -d
```

Check all services are up:

```bash
docker compose ps
```

Expected healthy services:
- `zookeeper` → port 2181
- `kafka` → port 9092
- `postgres` → port 5432
- `spark-master` → port 8081 (Spark UI)
- `spark-worker` → port 8082
- `airflow-webserver` → port 8080
- `airflow-scheduler`
- `trino` → port 8082 (Trino UI)
- `grafana` → port 3000
- `superset` → port 8088

---

## Step 4 — Initialize Airflow Connections

Open the Airflow UI at **http://localhost:8080** (admin / admin).

Navigate to **Admin → Connections** and add:

**Trino connection:**
```
Conn ID:   trino_default
Conn Type: Trino
Host:      trino
Port:      8080
Schema:    iceberg
```

**PostgreSQL connection:**
```
Conn ID:   postgres_default
Conn Type: Postgres
Host:      postgres
Port:      5432
Schema:    airflow
Login:     airflow
Password:  airflow
```

Alternatively, add via CLI:
```bash
docker exec airflow-scheduler airflow connections add trino_default \
  --conn-type trino --conn-host trino --conn-port 8080 --conn-schema iceberg

docker exec airflow-scheduler airflow connections add postgres_default \
  --conn-type postgres --conn-host postgres --conn-port 5432 \
  --conn-schema airflow --conn-login airflow --conn-password airflow
```

---

## Step 5 — Run the Historical Backfill

In the Airflow UI:
1. Navigate to **DAGs → backfill_ohlcv**
2. Toggle the DAG to **Active**
3. The DAG is set to `catchup=True` with `start_date=2021-01-01`, so Airflow
   will automatically schedule runs for every day from 2021-01-01 to today.

> **Note:** Polygon.io free tier returns approximately 2 years of historical data.
> Runs for dates older than that will log a warning and skip gracefully —
> they will not fail.

Monitor progress in the Airflow UI. Each daily run:
1. Fetches OHLCV for all 10 symbols from Polygon
2. Writes to `iceberg.bronze.bronze_daily_ohlcv`
3. Runs `dbt run --select silver+ gold+`

---

## Step 6 — Start the Kafka Producer

The producer polls Polygon.io every 3 seconds when the market is open
(Sun–Thu 10:00–15:00 Riyadh time). Outside market hours it falls back to
realistic random-walk simulation.

**Option A — run locally (recommended for development):**
```bash
cd producer
pip install -r requirements.txt
python kafka_producer.py
```

**Option B — run in Docker:**
```bash
docker run --rm --network tadawul_tadawul_net \
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

The consumer writes to `iceberg.bronze.bronze_ticks` every 10 seconds.
Monitor the Spark UI at **http://localhost:8081**.

Verify data is landing in Iceberg:
```bash
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.bronze.bronze_ticks"
```

---

## Step 8 — Import Grafana Dashboard

1. Open Grafana at **http://localhost:3000** (admin / admin)
2. Navigate to **Connections → Data Sources → Add data source**
3. Add a **PostgreSQL** datasource:
   - Host: `postgres:5432`
   - Database: `airflow`
   - User: `airflow` / Password: `airflow`
   - SSL: disable
4. Navigate to **Dashboards → New → Import**
5. Create panels querying `ml_anomalies` for real-time anomaly alerts:
   ```sql
   SELECT
     detected_at AS time,
     symbol,
     anomaly_type,
     score,
     is_anomaly::int AS flagged
   FROM ml_anomalies
   WHERE $__timeFilter(detected_at)
   ORDER BY detected_at DESC
   ```

---

## Step 9 — Connect Superset to Trino

1. Open Superset at **http://localhost:8088**
2. Initialize Superset admin on first run:
   ```bash
   docker exec superset superset db upgrade
   docker exec superset superset fab create-admin \
     --username admin --password admin \
     --firstname Admin --lastname User \
     --email admin@tadawul.local
   docker exec superset superset init
   ```
3. Navigate to **Settings → Database Connections → + Database**
4. Select **Trino** and enter:
   ```
   sqlalchemy_uri: trino://trino:8080/iceberg
   ```
5. Navigate to **SQL Lab** and run a test query:
   ```sql
   SELECT
     symbol,
     date,
     ROUND(annualized_vol * 100, 2) AS vol_pct
   FROM iceberg.gold.gold_volatility_index
   ORDER BY date DESC, vol_pct DESC
   LIMIT 20
   ```

---

## Step 10 — Verify End-to-End

Run these checks to confirm all layers are populated:

```bash
# 1. Kafka — live tick stream
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tadawul.ticks --max-messages 3

# 2. Bronze — Spark writes (real-time)
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

# 7. PostgreSQL — ML write-back
docker exec postgres psql -U airflow -c \
  "SELECT symbol, date, anomaly_type, score FROM ml_anomalies ORDER BY detected_at DESC LIMIT 5"
```

---

## ML Anomaly Detection

The `ml_anomaly_detection` Airflow DAG runs daily after the backfill and:

1. **Reads** `gold_volatility_index` + `silver_ohlcv` via Trino
2. **Volume Z-score**: flags symbols where volume > 2.5σ above 30-day mean
3. **Price IQR**: flags daily returns outside Q1 ± 1.5×IQR over 90-day window
4. **Writes** to `iceberg.gold.gold_anomaly_flags` (queryable via Trino/Superset)
5. **Writes** to PostgreSQL `ml_anomalies` (for Grafana real-time alerts)

Enable in Airflow UI: **DAGs → ml_anomaly_detection → Active**

---

## dbt Models

Run dbt manually from inside the Airflow container:
```bash
docker exec airflow-scheduler bash -c \
  "dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"

# Run tests
docker exec airflow-scheduler bash -c \
  "dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"

# Check source freshness
docker exec airflow-scheduler bash -c \
  "dbt source freshness --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
```

---

## Port Reference

| Service | URL |
|---------|-----|
| Airflow UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8081 |
| Trino UI | http://localhost:8082 |
| Grafana | http://localhost:3000 |
| Superset | http://localhost:8088 |
| PostgreSQL | localhost:5432 |
| Kafka | localhost:9092 |

---

## Troubleshooting

**Airflow scheduler not starting:**
Make sure `AIRFLOW__CORE__FERNET_KEY` is set in `.env`. Generate with:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**Spark "Access Denied" on S3:**
Both credential paths must be configured — `spark.hadoop.fs.s3a.*` (Hadoop) AND
`spark.sql.catalog.glue_catalog.s3.*` (Iceberg S3FileIO). Both are set in
`streaming_consumer.py`. Verify your IAM user has `s3:PutObject` and `glue:CreateTable`.

**Trino "Table not found":**
The Glue database must be created before Trino can see it. The first Spark micro-batch
runs `CREATE DATABASE IF NOT EXISTS glue_catalog.bronze`. Wait for the consumer to process
at least one batch, then retry.

**dbt "Schema does not exist":**
Without the `macros/generate_schema_name.sql` macro, dbt-trino prefixes all schemas
with the target schema. Verify the macro file exists at `dbt/macros/generate_schema_name.sql`.

**Polygon.io returning empty results for historical dates:**
Free tier returns approximately 2 years of history. The backfill DAG logs a warning
for older dates but does not fail. Data availability begins around 2023 for free accounts.
