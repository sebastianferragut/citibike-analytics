# NYC Citibike Product Analytics Pipeline

A local-first SQL and data engineering project that turns official NYC Citibike trip archives into a clean PostgreSQL analytics dataset. The pipeline reads raw CSV files with DuckDB, normalizes schema differences, writes Parquet files for columnar workflows, loads curated tables into PostgreSQL, and exposes SQL-first quality checks and product analytics.

## Project Overview

This project is designed to demonstrate:

- SQL fundamentals: DDL, DML, joins, CTEs, aggregations, and window functions
- Data ingestion from raw compressed trip files
- Data quality validation against a curated schema
- Core product analytics queries for ridership, stations, and rider behavior proxies
- Python + SQL integration for orchestration and reporting

## Dataset Source

- Official Citi Bike trip data: [https://s3.amazonaws.com/tripdata/index.html](https://s3.amazonaws.com/tripdata/index.html)
- Verified 2025 archive pattern: `202501-citibike-tripdata.zip` through `202512-citibike-tripdata.zip`
- Scope: all available 2025 monthly files

The 2025 public trip feed uses the modern ride-level schema (`ride_id`, `rideable_type`, `started_at`, `ended_at`, station fields, coordinates, and `member_casual`). The ingestion script also defensively maps older Citi Bike columns such as `starttime`, `stoptime`, `tripduration`, and `usertype` into the standardized trip model.

## Architecture

```text
Official Citi Bike ZIP archives
        |
        v
  Extract raw CSV files locally
        |
        v
DuckDB reads CSV files from data/extracted/
        |
        v
Normalize columns + union files + derive trip duration
        |
        +--> write Parquet files to data/parquet/
        |
        v
Load stations + trips into PostgreSQL
        |
        v
Run SQL data quality checks and analytics queries
```

## Repository Structure

```text
citibike-analytics/
├── README.md
├── docker-compose.yml
├── data/
│   ├── extracted/
│   ├── parquet/
│   └── raw/
├── notebooks/
│   └── citibike_analysis.ipynb
├── scripts/
│   ├── ingest.py
│   └── report.py
└── sql/
    ├── 01_schema.sql
    ├── 02_data_quality.sql
    └── 03_analytics.sql
```

## Workflow: CSV -> Parquet -> PostgreSQL

1. Download the official 2025 monthly ZIP archives into `data/raw/`
2. Extract every CSV from each archive into `data/extracted/`
3. Read the extracted CSV files with DuckDB
4. Normalize column names and data types into a single curated trip shape
5. Write normalized Parquet files into `data/parquet/`
6. Load deduplicated `stations` and curated `trips` into PostgreSQL
7. Run SQL quality checks and analytics queries

## Local Setup

### 1. Start PostgreSQL

```bash
docker compose up -d
```

### 2. Create a project virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Python dependencies

```bash
python -m pip install duckdb psycopg2-binary jupyter ipykernel pytz matplotlib
```

### 4. Run ingestion

```bash
python scripts/ingest.py --download
```

If the archives are already present locally, you can skip the download step:

```bash
python scripts/ingest.py
```

### 5. Print a terminal summary

```bash
python scripts/report.py
```

### 6. Explore the notebook

```bash
jupyter notebook notebooks/citibike_analysis.ipynb
```

### 7. Run the analytics SQL directly

Because `sql/03_analytics.sql` lives on the host machine rather than inside the PostgreSQL container, run it with shell redirection:

```bash
docker compose exec -T postgres psql -U citibike -d citibike < sql/03_analytics.sql
```

If you want to save the output while reviewing it:

```bash
docker compose exec -T postgres psql -U citibike -d citibike < sql/03_analytics.sql | tee analytics_results.txt
```

## PostgreSQL Defaults

The provided `docker-compose.yml` uses:

- Database: `citibike`
- User: `citibike`
- Password: `citibike`
- Host port: `5432`

The Python scripts read the same values from environment variables when needed:

- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

## SQL Concepts Demonstrated

- `CREATE TABLE`, indexes, primary keys, and foreign keys
- `INSERT` into a quality log table
- `GROUP BY`, aggregates, and filtered aggregations
- `JOIN`s between trips and stations
- CTEs for readability and metric staging
- Window functions such as `LAG`
- `DATE_TRUNC`, `EXTRACT`, and `PERCENTILE_CONT`

## Query Optimization Notes

- The curated `trips` schema includes a derived local timestamp column, `started_at_est`, and the analytics queries remain portable by computing local time inside SQL when needed.
- The `trips` table is indexed on `ride_id`, `started_at`, `started_at_est`, `member_casual`, `start_station_id`, and `end_station_id` in [sql/01_schema.sql](sql/01_schema.sql).
- A composite index on `(started_at, member_casual)` supports common time-series and segment filters.
- The ingestion flow converts raw CSV into Parquet before loading PostgreSQL, which reduces repeated CSV parsing for reruns and notebook work.
- The loader can reuse existing Parquet outputs for faster rebuilds of the PostgreSQL tables.
- `sql/03_analytics.sql` stages daily rider-proxy activity once and reuses that cached result for daily, weekly, and monthly active-user calculations.
- `scripts/report.py` now follows the same idea by caching rider-day activity and normalized station IDs in temp tables before printing the terminal summary.
- The terminal report intentionally keeps to summary-level output, while the churn proxy remains an optional heavy query in the SQL layer.

If you want to inspect performance directly, the notebook includes `EXPLAIN ANALYZE` examples for core queries.

## Analytics Notes and Assumptions

- Public 2025 Citi Bike data does not include a persistent user identifier.
- Daily, weekly, monthly, and churn-style user metrics therefore use a documented rider proxy built from membership type, rideable type, and station pair.
- Distinct rider-proxy counts are calculated independently for each time grain, so weekly and monthly proxy counts are not additive and should be interpreted as separate engagement views rather than rollups of one another.
- Weekly active users are labeled by local week start date to keep year-boundary output readable.
- The curated schema stores `started_at_est` as a derived New York local timestamp, and time-based analytics are also written so they can compute `America/New_York` local time inline when needed.
- Station rankings and imbalance are grouped at a normalized station-ID level and display a readable station name, which keeps joins stable while collapsing formatting-only ID variants such as `5980.1` vs `5980.10`.
- Older columns like `usertype` are mapped into `member_casual` (`Subscriber -> member`, `Customer -> casual`).
- Older-only fields such as `gender` and `birth_year` are not loaded into the curated schema because the 2025 scope does not require them for the requested analytics.
- Curated trip loads deduplicate `ride_id`, filter invalid durations, and enforce required fields before data reaches the analytics tables.

## Summary of Insights

The included analytics focus on product questions:

- How active-user demand builds and falls across the 2025 year
- When demand concentrates across days of week and hours of day
- Which stations act as the biggest trip origins and destinations
- Which stations accumulate inflow or outflow imbalance
- How member and casual usage differs in volume and trip duration
- How to approximate reactivation and 30-day churn without a direct user identifier

At a high level, the current 2025 results show:

- Monthly active-user proxy demand rises from winter into summer, peaks in August, stays strong through September, and tapers in late fall and early winter.
- Month-over-month active-user growth is strongest in the spring ramp, then moderates and turns negative as the network moves back into late fall and winter.
- Hourly demand is strongest in the late afternoon and early evening once trips are converted to New York local time, with 5 PM standing out as the busiest hour in the refreshed run.
- Start demand is concentrated in a relatively small set of Manhattan hub stations, while station inflow and outflow imbalances are noticeable but not dominated by a single runaway outlier.
- Member riders account for most total trips, while casual rides are materially longer on average.

## Storage Note

The official 2025 Citi Bike archives are large. The 12 ZIP files alone are roughly 8.8 GB compressed, and the extracted CSV plus Parquet outputs require materially more disk space.