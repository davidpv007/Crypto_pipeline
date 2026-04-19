# Crypto Pipeline

A production-grade ELT pipeline that ingests real-time and historical 
cryptocurrency data from CoinGecko, processes it through a Bronze → Silver → Gold 
medallion architecture, and serves analytical marts via DuckDB.

## Architecture

CoinGecko API ──► MinIO (Bronze) ──► Spark (Silver) ──► Delta (Gold) ──► DuckDB (Marts)

## Stack

- **Orchestration:** Apache Airflow
- **Ingestion:** CoinGecko API (Demo Plan)
- **Storage:** MinIO (object store)
- **Processing:** Apache Spark + Delta Lake
- **Serving:** DuckDB
- **Quality:** Great Expectations
- **Infra:** Docker Compose

## Quick Start

```bash
cp .env.example .env      
make up                     
make test                   
make lint                  
```

## Project Structure

- `src/ingestion/` — CoinGecko API client and raw writers
- `src/transformations/` — Bronze → Silver → Gold logic
- `src/marts/` — DuckDB mart builders
- `airflow/dags/` — pipeline orchestration
- `infrastructure/` — Docker Compose + MinIO setup
- `tests/` — unit and integration tests