# Crypto Data Engineering Pipeline — Project Roadmap

**Architecture:** Source → Lake → Warehouse → Mart
**Stack:** Open-source, local-first, production-ready patterns
**Goal:** End-to-end portfolio project with CI/CD, tests, monitoring, and QA

---

## 1. Architecture Overview

Your reference diagram maps to this stack:


| Layer                 | Tool                                              | Role                                 |
| --------------------- | ------------------------------------------------- | ------------------------------------ |
| **SOURCE**            | CoinGecko + Binance public APIs                   | Raw crypto market data               |
| **LAKE**              | Delta Lake on MinIO (S3-compatible)               | Bronze + Silver tables               |
| **WAREHOUSE**         | Delta Lake (Gold tables)                          | Curated, modeled, query-ready        |
| **MART**              | DuckDB                                            | Fast analytical serving per use-case |
| **Transformations**   | PySpark + Delta                                   | Bronze → Silver → Gold               |
| **Orchestration**     | Apache Airflow                                    | DAG scheduling, retries, SLAs        |
| **Quality**           | Great Expectations + pytest                       | Data + code tests                    |
| **CI/CD**             | GitHub Actions                                    | Lint, test, build on push            |
| **Monitoring**        | Airflow + structured logs + Prometheus (optional) | Observability                        |
| **Container runtime** | Docker Compose                                    | Local dev environment                |


### Why this stack?

- **MinIO** gives you the S3 API locally — same `boto3`/`s3a://` code will work in AWS later.
- **Delta Lake** gives you ACID transactions, time travel, schema evolution, and `MERGE` — the non-negotiable features of a modern lakehouse.
- **PySpark** is the industry-standard transformation engine and what you'll use at any scale.
- **DuckDB** reads Delta tables directly via its `delta` extension, so the "mart" becomes lightning-fast analytical queries without an expensive warehouse.
- **Airflow** is still the most widely adopted orchestrator and what 80% of job listings require.

---

## 2. Data Sources

### Primary: CoinGecko Demo API

- **Free**, 30 calls/min, 10,000 calls/month
- No credit card required — just sign up for a Demo API key
- **Best endpoints for ingestion:**
  - `/coins/markets` — top N coins snapshot (price, mcap, volume) — ideal for daily batch
  - `/coins/{id}/market_chart` — historical OHLC & volume
  - `/coins/{id}` — metadata (descriptions, links, categories)
  - `/exchanges` — exchange list + metadata
  - `/global` — global crypto market stats
- Docs: [https://docs.coingecko.com/](https://docs.coingecko.com/)

### Secondary: Binance Public API

- **Free, no auth** for market data (REST + WebSocket)
- Great for high-frequency data: klines (candles), order book snapshots, recent trades
- **Best endpoints:**
  - `/api/v3/klines` — historical candlestick data (1m to 1M intervals)
  - `/api/v3/ticker/24hr` — 24h rolling window stats per symbol
  - `/api/v3/exchangeInfo` — trading pairs metadata
- Docs: [https://developers.binance.com/docs/binance-spot-api-docs](https://developers.binance.com/docs/binance-spot-api-docs)

### Backup / enrichment options

- **CryptoCompare** — historical OHLCV, social stats (free tier: 100k calls/month)
- **CoinPaprika** — free, no key, good for social/community metrics
- **Alpha Vantage** — free tier includes crypto (5 calls/min)
- **Kaiko / Messari** — institutional-grade, paid only

### Recommended data scope for the project

- ~50 top coins by market cap
- Daily snapshots + hourly klines for top 10
- Exchange metadata (static, refreshed weekly)
- Global market stats (daily)

This keeps you well within free tiers while giving enough volume for realistic PySpark workloads.

---

## 3. Repository Structure

```
crypto-pipeline/
├── .github/
│   └── workflows/
│       ├── ci.yml              # lint, type-check, unit tests
│       └── data-quality.yml    # scheduled GE checks
├── airflow/
│   ├── dags/
│   │   ├── ingest_coingecko.py
│   │   ├── ingest_binance.py
│   │   ├── transform_bronze_to_silver.py
│   │   ├── transform_silver_to_gold.py
│   │   └── build_marts.py
│   ├── plugins/
│   └── requirements.txt
├── src/
│   ├── ingestion/
│   │   ├── coingecko_client.py
│   │   ├── binance_client.py
│   │   └── writers.py          # raw JSON → MinIO (Bronze)
│   ├── transformations/
│   │   ├── bronze_to_silver/
│   │   │   ├── coins_markets.py
│   │   │   └── klines.py
│   │   ├── silver_to_gold/
│   │   │   ├── dim_coins.py
│   │   │   ├── fact_prices_daily.py
│   │   │   └── fact_klines_hourly.py
│   │   └── utils/
│   │       ├── spark_session.py
│   │       └── delta_helpers.py
│   ├── marts/
│   │   ├── build_duckdb_marts.py
│   │   └── sql/
│   │       ├── mart_top_movers.sql
│   │       └── mart_volatility.sql
│   └── quality/
│       ├── expectations/       # Great Expectations suites
│       └── checks.py
├── tests/
│   ├── unit/
│   │   ├── test_coingecko_client.py
│   │   └── test_transformations.py
│   └── integration/
│       └── test_end_to_end.py
├── infrastructure/
│   ├── docker-compose.yml
│   ├── Dockerfile.airflow
│   └── minio-init.sh
├── configs/
│   ├── coins.yaml              # which coins to track
│   └── spark-defaults.conf
├── docs/
│   ├── architecture.md
│   ├── runbook.md
│   └── data-contracts.md
├── .env.example
├── .gitignore
├── pyproject.toml              # or requirements.txt
├── Makefile                    # make up / make test / make lint
└── README.md
```

---

## 4. Phased Implementation Plan

### Phase 0 — Foundations (Day 1)

1. Create GitHub repo (public, for portfolio visibility)
2. Sign up for **CoinGecko Demo API key** (free, instant)
3. Install locally: Docker Desktop, Python 3.11+, `uv` or `poetry`, `pre-commit`
4. Initialize repo structure above
5. Set up `pre-commit` hooks: `ruff`, `black`, `mypy`, `nbstripout`
6. Write initial `README.md` with architecture diagram

**Deliverable:** Empty repo that passes `pre-commit run --all-files`

---

### Phase 1 — Local Infrastructure (Days 2–3)

Write `docker-compose.yml` with these services:


| Service             | Image                               | Purpose                    |
| ------------------- | ----------------------------------- | -------------------------- |
| `minio`             | `minio/minio`                       | S3-compatible object store |
| `minio-init`        | `minio/mc`                          | Bucket bootstrapping       |
| `postgres`          | `postgres:15`                       | Airflow metadata DB        |
| `airflow-init`      | custom                              | DB init + user creation    |
| `airflow-webserver` | custom                              | UI on :8080                |
| `airflow-scheduler` | custom                              | DAG scheduling             |
| `airflow-worker`    | custom (with PySpark + delta-spark) | Task execution             |


**Bootstrap MinIO buckets:**

- `bronze` — raw landings
- `silver` — cleaned Delta tables
- `gold` — curated Delta tables
- `marts` — DuckDB files

**Key config:**

- PySpark 3.5+ with `delta-spark==3.2.0` (version alignment is critical)
- `spark.hadoop.fs.s3a.endpoint=http://minio:9000`
- `spark.hadoop.fs.s3a.path.style.access=true`

**Deliverable:** `make up` spins up everything, Airflow UI accessible at localhost:8080, MinIO console at localhost:9001.

---

### Phase 2 — Ingestion: Source → Bronze (Days 4–6)

1. Build `CoinGeckoClient` with:
  - Rate limit handling (token bucket — stay under 30/min)
  - Retries with exponential backoff (`tenacity`)
  - Response pagination helpers
2. Build `BinanceClient` similarly (no auth needed)
3. Write ingestion functions that land **raw JSON responses as-is** into MinIO:
  ```
   s3a://bronze/coingecko/coins_markets/ingestion_date=2026-04-18/hour=14/data.json
   s3a://bronze/binance/klines/symbol=BTCUSDT/interval=1h/date=2026-04-18/data.json
  ```
4. **Bronze rule:** store data exactly as received. No transformation. Partition by ingestion time.

**Why raw?** Re-playability. If your transformation has a bug, you can rebuild Silver/Gold from Bronze without re-hitting the APIs.

**Deliverable:** Running `python -m src.ingestion.coingecko_client` lands JSON in MinIO.

---

### Phase 3 — Bronze → Silver with PySpark (Days 7–10)

For each source, write a PySpark job that:

1. Reads raw JSON from Bronze
2. Applies schema (explicit `StructType`, not inference)
3. Flattens nested fields
4. Casts types properly (prices as `decimal(38,18)`, timestamps as `timestamp`)
5. Deduplicates on natural keys
6. Adds audit columns: `_ingested_at`, `_source`, `_batch_id`
7. Writes **Delta tables** partitioned appropriately

**Example table: `silver.coins_markets`**

```
Partitioned by: snapshot_date (DATE)
Columns: coin_id, symbol, name, current_price, market_cap,
         total_volume, price_change_24h, snapshot_ts, _ingested_at
```

**Use Delta MERGE** for upserts so re-runs are idempotent:

```python
delta_table.alias("tgt").merge(
    updates.alias("src"),
    "tgt.coin_id = src.coin_id AND tgt.snapshot_date = src.snapshot_date"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**Deliverable:** PySpark job rebuilds all Silver tables idempotently from Bronze.

---

### Phase 4 — Silver → Gold (Days 11–14)

This is the "warehouse" layer — modeled, business-ready tables.

**Star schema:**

- `dim_coin` (SCD Type 2 for name/category changes)
- `dim_exchange`
- `dim_date`
- `fact_prices_daily` (one row per coin per day)
- `fact_klines_hourly` (one row per symbol per hour)
- `fact_market_snapshot` (global metrics per day)

**Key PySpark patterns to practice:**

- Window functions for returns, moving averages, volatility
- `MERGE INTO` for SCD2
- `OPTIMIZE` + `ZORDER BY coin_id` on large Gold tables
- `VACUUM` with 7-day retention

**Deliverable:** Gold Delta tables queryable via `spark.read.format("delta").load(...)`.

---

### Phase 5 — Mart Layer with DuckDB (Days 15–16)

DuckDB reads Delta directly — this is the magic. Three options, pick one or combine:

**Option A (recommended for learning): Materialized DuckDB files per use case**

```python
import duckdb
con = duckdb.connect('marts/team_trading.duckdb')
con.execute("INSTALL delta; LOAD delta;")
con.execute("""
    CREATE OR REPLACE TABLE top_movers AS
    SELECT * FROM delta_scan('s3://gold/fact_prices_daily')
    WHERE snapshot_date >= current_date - 7
    ORDER BY abs(pct_change_24h) DESC
""")
```

**Option B: On-the-fly querying of Delta from DuckDB** (no materialization, always fresh)

**Option C: Export to Parquet** for maximum portability

Build 2–3 marts that match your diagram:

- `team_1.duckdb` — daily price + volume summary
- `team_2.duckdb` — hourly klines for top 10 coins
- `use_case_1.duckdb` — volatility & correlation analysis

**Deliverable:** DuckDB files that an analyst could open in DBeaver or query from a notebook.

---

### Phase 6 — Orchestration with Airflow (Days 17–19)

**DAG design (five DAGs, not one monolith):**

1. `**ingest_coingecko`** — hourly schedule, tasks per endpoint
2. `**ingest_binance**` — hourly, parallel per symbol
3. `**bronze_to_silver**` — runs after both ingestions, PySpark via `PythonOperator` or `SparkSubmitOperator`
4. `**silver_to_gold**` — runs after silver completes
5. `**build_marts**` — runs after gold, produces DuckDB files

**Patterns to implement:**

- **Dataset-driven scheduling** (Airflow 2.4+): Silver DAG triggers on Bronze Dataset updates
- Retries: 3 with exponential backoff
- SLAs: alert if ingestion > 15 min
- `on_failure_callback` → Slack or email
- Task groups to keep DAGs readable
- `@task.branch` for conditional logic (e.g., skip if API down)

**Airflow Variables / Connections:**

- Store CoinGecko API key as Airflow Variable (encrypted)
- MinIO credentials as S3 Connection

**Deliverable:** Full pipeline runs end-to-end from the Airflow UI on a schedule.

---

### Phase 7 — Data Quality & Testing (Days 20–22)

**Three test layers:**

**a) Unit tests (pytest)** — pure Python logic, mocked API calls

```
tests/unit/test_coingecko_client.py
tests/unit/test_transformations.py  # spark sessions via pytest-spark
```

**b) Data quality tests (Great Expectations)**

- Expectations per Silver/Gold table: non-null, unique keys, value ranges, row counts
- Run as Airflow task post-transformation
- Fail the DAG on critical check failures

**Example expectations for `fact_prices_daily`:**

- `coin_id` is never null
- `current_price` > 0
- `snapshot_date` within last 2 days
- Row count within ±20% of 7-day average

**c) Integration tests** — spin up ephemeral containers, run mini pipeline end-to-end, assert Gold tables are correct.

**Deliverable:** `make test` runs all three layers. GitHub Actions runs unit + integration on every PR.

---

### Phase 8 — CI/CD with GitHub Actions (Days 23–24)

`**.github/workflows/ci.yml`:**

```yaml
on: [push, pull_request]
jobs:
  lint:         # ruff, black, mypy
  unit-test:    # pytest tests/unit
  integration:  # docker compose up + pytest tests/integration
  build:        # docker image build + push to GHCR (on main)
```

**Branch protection:**

- `main` requires all CI green + 1 review
- PRs auto-lint and comment coverage delta

**Optional but valuable:**

- `data-quality.yml` — scheduled GE run against prod data, opens issue on failure
- Dependabot for Python deps
- Release-please for semantic versioning

**Deliverable:** Green CI badge in README. Every PR blocks bad code.

---

### Phase 9 — Monitoring & Observability (Days 25–26)

**Minimal (sufficient for portfolio):**

- Structured JSON logs (`structlog`) with correlation IDs across DAG runs
- Airflow email/Slack on failure
- DAG-level metrics in Airflow UI (duration trends, success rate)

**Stretch goals (if you want to flex):**

- **Prometheus + Grafana** via `airflow-exporter` — dashboards for DAG duration, task failures, queue depth
- **Statsd** metrics from PySpark jobs (row counts, duration)
- **OpenLineage + Marquez** — data lineage across your whole pipeline (very impressive on a resume)

**Key dashboards to build:**

- Pipeline health (success rate, freshness per table)
- Data volumes (rows landed per day per source)
- Quality (GE pass rate over time)

**Deliverable:** A Grafana screenshot for the README showing live pipeline health.

---

### Phase 10 — Documentation & Portfolio Polish (Days 27–28)

**Must-haves:**

- `README.md` — architecture diagram (replicate your reference image!), quickstart, tech choices rationale, screenshots
- `docs/architecture.md` — detailed design decisions
- `docs/runbook.md` — "how to debug when X breaks"
- `docs/data-contracts.md` — schema + SLA per Gold table
- Recorded demo video (Loom, 3–5 min) — shows Airflow UI, MinIO, a DuckDB query

**For recruiters:**

- Pin the repo on your GitHub profile
- Blog post on Medium/dev.to explaining design decisions
- LinkedIn post with the architecture diagram

---

## 5. Medallion Layer Contract (Quick Reference)


| Layer      | Format             | Purpose                         | Retention   | Schema enforcement |
| ---------- | ------------------ | ------------------------------- | ----------- | ------------------ |
| **Bronze** | Raw JSON / Parquet | Exactly as received, replayable | 90 days     | None               |
| **Silver** | Delta              | Typed, deduped, clean           | 1 year      | Strict             |
| **Gold**   | Delta              | Modeled, business-ready         | Indefinite  | Strict + SCD       |
| **Mart**   | DuckDB / Parquet   | Fast serving per consumer       | Rebuildable | Strict             |


---

## 6. Realistic Timeline


| Pace                          | Duration   |
| ----------------------------- | ---------- |
| Full-time focus               | 3–4 weeks  |
| Nights & weekends (10 hrs/wk) | 7–9 weeks  |
| Light evenings (5 hrs/wk)     | 3–4 months |


Don't rush. The point is **learning depth**, not speed.

---

## 7. Your First Week — Concrete Checklist

- Create GitHub repo `crypto-pipeline`
- Sign up for CoinGecko Demo API key
- Scaffold the directory structure from Section 3
- Write `docker-compose.yml` with MinIO + Postgres + Airflow
- Verify `make up` → Airflow UI loads, MinIO buckets exist
- Write `CoinGeckoClient` with one working endpoint
- Write first ingestion DAG that lands JSON in Bronze
- Write one pytest unit test
- Set up pre-commit + GitHub Actions skeleton
- Push everything, confirm CI runs

If you complete this week, the rest is execution on a well-understood spine.

---

## 8. Critical Gotchas to Avoid

1. **PySpark + Delta version mismatch** — use the exact compatibility matrix from Delta's docs. Wrong versions = cryptic errors.
2. **Don't commit API keys** — use `.env` + Airflow Variables, add `.env` to `.gitignore` from day 1.
3. **Always partition Delta tables** — unpartitioned tables become painful fast. Partition Bronze by ingestion date, Silver/Gold by business date.
4. **Run `OPTIMIZE` and `VACUUM`** — Delta's small-file problem is real; schedule maintenance DAGs weekly.
5. **Idempotency is non-negotiable** — every task must be safe to re-run. Use MERGE, not INSERT. Use deterministic partitions.
6. **Schema on write, not read** — always define explicit StructTypes in PySpark; inference leads to silent type drift.
7. **Rate limit respectfully** — a 429 from CoinGecko is embarrassing. Use `tenacity` + sleep logic.
8. **Don't skip tests** — adding them retroactively is 5× more painful than writing alongside.

---

## 9. What Makes This Portfolio-Worthy

Most "data engineering portfolio" projects stop at "I pulled an API into Postgres." This one differentiates you because it has:

- A **real lakehouse** (Delta Lake), not just a DB dump
- **PySpark** transformations (the industry standard)
- **Medallion architecture** (what every modern data team uses)
- **Orchestration** with proper DAG design (not a cron job)
- **Data quality** gates (not just "hope it works")
- **CI/CD + tests** (separates juniors from mid-level)
- **Monitoring** (the thing nobody in portfolios has)
- **Documentation** that proves you can think about systems

Employers can see from this that you could step into a working team and ship on day one.

---

## 10. Stretch Goals (After MVP)

- Swap PySpark local mode for a real **Spark cluster** (docker-compose with Spark master + workers)
- Add **streaming ingestion** via Binance WebSocket → Kafka → Spark Structured Streaming
- Migrate from MinIO to **real AWS S3** and from local Airflow to **MWAA** or **Astronomer** — same code, cloud-deployed
- Add **dbt** on top of Gold tables for SQL-based transformations (shows you can mix paradigms)
- Add a **Streamlit dashboard** on top of DuckDB marts — bonus frontend chops
- Implement **OpenLineage** for end-to-end data lineage visualization

---

**Good luck.** Start with Phase 0 today, even if it's just creating the repo and signing up for CoinGecko. Momentum beats planning.