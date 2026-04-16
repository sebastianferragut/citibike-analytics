from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

import duckdb
import psycopg2


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
EXTRACTED_DIR = DATA_DIR / "extracted"
PARQUET_DIR = DATA_DIR / "parquet"
SQL_DIR = ROOT_DIR / "sql"
TRIPS_PARQUET_PATH = PARQUET_DIR / "trips_2025.parquet"
STATIONS_PARQUET_PATH = PARQUET_DIR / "stations_2025.parquet"

ARCHIVE_BASE_URL = "https://tripdata.s3.amazonaws.com"
ARCHIVE_NAMES = [f"2025{month:02d}-citibike-tripdata.zip" for month in range(1, 13)]

TRIP_COLUMNS = [
    "ride_id",
    "rideable_type",
    "started_at",
    "started_at_est",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
    "trip_duration_seconds",
]

STATION_COLUMNS = [
    "station_id",
    "station_name",
    "station_lat",
    "station_lng",
]

TIMESTAMP_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest 2025 Citi Bike CSV data into PostgreSQL through DuckDB."
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download the official 2025 Citi Bike ZIP archives into data/raw/ before ingestion.",
    )
    parser.add_argument(
        "--skip-parquet",
        action="store_true",
        help="Skip writing normalized Parquet outputs to data/parquet/.",
    )
    return parser.parse_args()


def ensure_directories() -> None:
    for directory in (RAW_DIR, EXTRACTED_DIR, PARQUET_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def get_postgres_config() -> dict[str, str]:
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": os.getenv("PGPORT", "5432"),
        "dbname": os.getenv("PGDATABASE", "citibike"),
        "user": os.getenv("PGUSER", "citibike"),
        "password": os.getenv("PGPASSWORD", "citibike"),
    }


def download_archives() -> list[Path]:
    downloaded_archives: list[Path] = []

    for archive_name in ARCHIVE_NAMES:
        destination = RAW_DIR / archive_name
        if destination.exists():
            print(f"Skipping download for existing archive: {destination.name}")
            downloaded_archives.append(destination)
            continue

        archive_url = f"{ARCHIVE_BASE_URL}/{archive_name}"
        print(f"Downloading {archive_name} ...")
        urllib.request.urlretrieve(archive_url, destination)
        downloaded_archives.append(destination)

    return downloaded_archives


def available_archives() -> list[Path]:
    archives = sorted(RAW_DIR.glob("2025*-citibike-tripdata.zip"))
    return archives


def extract_archives(archives: list[Path]) -> list[Path]:
    extracted_csvs: list[Path] = []

    for archive_path in archives:
        extract_root = EXTRACTED_DIR / archive_path.stem
        extract_root.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(archive_path) as zip_file:
            members = [member for member in zip_file.namelist() if member.lower().endswith(".csv")]
            missing_members = [member for member in members if not (extract_root / member).exists()]

            if missing_members:
                print(f"Extracting {archive_path.name} ...")
                zip_file.extractall(extract_root)
            else:
                print(f"Skipping extraction for {archive_path.name}; CSV files already exist.")

        extracted_csvs.extend(sorted(extract_root.rglob("*.csv")))

    return sorted(extracted_csvs)


def normalize_column_name(column_name: str) -> str:
    normalized = column_name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    return normalized.strip("_")


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def local_timestamp_expression(raw_expression: str | None) -> str:
    if raw_expression is None:
        return "NULL::TIMESTAMP"

    attempts = [f"TRY_STRPTIME({raw_expression}, '{fmt}')" for fmt in TIMESTAMP_FORMATS]
    attempts.append(f"TRY_CAST({raw_expression} AS TIMESTAMP)")
    return f"COALESCE({', '.join(attempts)})"


def text_expression(column_lookup: dict[str, str], *aliases: str) -> str | None:
    for alias in aliases:
        key = normalize_column_name(alias)
        if key in column_lookup:
            source_column = quote_identifier(column_lookup[key])
            return f"NULLIF(TRIM(CAST({source_column} AS VARCHAR)), '')"
    return None


def numeric_expression(column_lookup: dict[str, str], *aliases: str) -> str | None:
    raw_expression = text_expression(column_lookup, *aliases)
    if raw_expression is None:
        return None
    return f"TRY_CAST({raw_expression} AS DOUBLE)"


def build_file_select(duck_connection: duckdb.DuckDBPyConnection, csv_path: Path) -> str:
    describe_sql = """
        DESCRIBE
        SELECT *
        FROM read_csv_auto(
            ?,
            header = TRUE,
            all_varchar = TRUE,
            sample_size = -1,
            union_by_name = TRUE,
            filename = TRUE
        )
    """
    description = duck_connection.execute(describe_sql, [str(csv_path)]).fetchall()
    column_lookup = {
        normalize_column_name(column_name): column_name for column_name, *_ in description
    }

    ride_id_expr = text_expression(column_lookup, "ride_id")
    if ride_id_expr is None:
        ride_id_expr = (
            "CONCAT("
            f"{sql_literal(csv_path.stem + '_')}, "
            "LPAD(CAST(ROW_NUMBER() OVER () AS VARCHAR), 12, '0')"
            ")"
        )

    rideable_type_expr = text_expression(column_lookup, "rideable_type")
    started_raw_expr = text_expression(column_lookup, "started_at", "starttime")
    ended_raw_expr = text_expression(column_lookup, "ended_at", "stoptime")
    start_station_name_expr = text_expression(
        column_lookup, "start_station_name", "start station name"
    )
    start_station_id_expr = text_expression(
        column_lookup, "start_station_id", "start station id"
    )
    end_station_name_expr = text_expression(
        column_lookup, "end_station_name", "end station name"
    )
    end_station_id_expr = text_expression(column_lookup, "end_station_id", "end station id")
    start_lat_expr = numeric_expression(column_lookup, "start_lat", "start station latitude")
    start_lng_expr = numeric_expression(column_lookup, "start_lng", "start station longitude")
    end_lat_expr = numeric_expression(column_lookup, "end_lat", "end station latitude")
    end_lng_expr = numeric_expression(column_lookup, "end_lng", "end station longitude")
    tripduration_raw_expr = text_expression(column_lookup, "tripduration")

    member_expr = text_expression(column_lookup, "member_casual")
    if member_expr is None:
        legacy_usertype_expr = text_expression(column_lookup, "usertype")
        if legacy_usertype_expr is not None:
            member_expr = f"""
                CASE
                    WHEN LOWER({legacy_usertype_expr}) = 'subscriber' THEN 'member'
                    WHEN LOWER({legacy_usertype_expr}) = 'customer' THEN 'casual'
                    ELSE LOWER({legacy_usertype_expr})
                END
            """
        else:
            member_expr = "NULL::VARCHAR"

    started_at_est_expr = local_timestamp_expression(started_raw_expr)
    ended_at_est_expr = local_timestamp_expression(ended_raw_expr)
    started_at_expr = f"({started_at_est_expr} AT TIME ZONE 'America/New_York')"
    ended_at_expr = f"({ended_at_est_expr} AT TIME ZONE 'America/New_York')"

    return f"""
        SELECT
            ride_id,
            rideable_type,
            started_at,
            started_at_est,
            ended_at,
            start_station_name,
            start_station_id,
            end_station_name,
            end_station_id,
            start_lat,
            start_lng,
            end_lat,
            end_lng,
            member_casual,
            CASE
                WHEN tripduration_raw IS NOT NULL THEN TRY_CAST(tripduration_raw AS BIGINT)
                WHEN started_at IS NOT NULL AND ended_at IS NOT NULL
                    THEN DATEDIFF('second', started_at, ended_at)
                ELSE NULL
            END AS trip_duration_seconds
        FROM (
            SELECT
                {ride_id_expr} AS ride_id,
                {rideable_type_expr or "NULL::VARCHAR"} AS rideable_type,
                {started_at_expr} AS started_at,
                {started_at_est_expr} AS started_at_est,
                {ended_at_expr} AS ended_at,
                {start_station_name_expr or "NULL::VARCHAR"} AS start_station_name,
                {start_station_id_expr or "NULL::VARCHAR"} AS start_station_id,
                {end_station_name_expr or "NULL::VARCHAR"} AS end_station_name,
                {end_station_id_expr or "NULL::VARCHAR"} AS end_station_id,
                {start_lat_expr or "NULL::DOUBLE"} AS start_lat,
                {start_lng_expr or "NULL::DOUBLE"} AS start_lng,
                {end_lat_expr or "NULL::DOUBLE"} AS end_lat,
                {end_lng_expr or "NULL::DOUBLE"} AS end_lng,
                {member_expr} AS member_casual,
                {tripduration_raw_expr or "NULL::VARCHAR"} AS tripduration_raw
            FROM read_csv_auto(
                {sql_literal(str(csv_path))},
                header = TRUE,
                all_varchar = TRUE,
                sample_size = -1,
                union_by_name = TRUE,
                filename = TRUE
            )
        ) staged_file
    """


def build_normalized_tables(csv_files: list[Path]) -> duckdb.DuckDBPyConnection:
    if not csv_files:
        raise FileNotFoundError(
            "No CSV files were found in data/extracted/. Download and extract the archives first."
        )

    duck_connection = duckdb.connect(database=":memory:")
    duck_connection.execute("PRAGMA threads=4")

    for index, csv_path in enumerate(csv_files):
        file_select = build_file_select(duck_connection, csv_path)
        if index == 0:
            print(f"Creating normalized trip table from {csv_path.name} ...")
            duck_connection.execute(f"CREATE TABLE normalized_trips AS {file_select}")
        else:
            print(f"Appending normalized rows from {csv_path.name} ...")
            duck_connection.execute(f"INSERT INTO normalized_trips {file_select}")

    duck_connection.execute(
        """
        CREATE OR REPLACE TABLE normalized_stations AS
        WITH station_points AS (
            SELECT
                start_station_id AS station_id,
                start_station_name AS station_name,
                start_lat AS station_lat,
                start_lng AS station_lng
            FROM normalized_trips
            WHERE start_station_id IS NOT NULL

            UNION ALL

            SELECT
                end_station_id AS station_id,
                end_station_name AS station_name,
                end_lat AS station_lat,
                end_lng AS station_lng
            FROM normalized_trips
            WHERE end_station_id IS NOT NULL
        )
        SELECT
            station_id,
            COALESCE(MAX(station_name), station_id) AS station_name,
            AVG(station_lat) AS station_lat,
            AVG(station_lng) AS station_lng
        FROM station_points
        GROUP BY station_id
        """
    )

    return duck_connection


def write_parquet(duck_connection: duckdb.DuckDBPyConnection) -> None:
    for parquet_file in (TRIPS_PARQUET_PATH, STATIONS_PARQUET_PATH):
        if parquet_file.exists():
            parquet_file.unlink()

    duck_connection.execute(
        f"COPY normalized_trips TO {sql_literal(str(TRIPS_PARQUET_PATH))} "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    duck_connection.execute(
        f"COPY normalized_stations TO {sql_literal(str(STATIONS_PARQUET_PATH))} "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )

    print(f"Wrote Parquet outputs to {PARQUET_DIR}")


def parquet_outputs_exist() -> bool:
    return TRIPS_PARQUET_PATH.exists() and STATIONS_PARQUET_PATH.exists()


def build_tables_from_parquet() -> duckdb.DuckDBPyConnection:
    duck_connection = duckdb.connect(database=":memory:")
    duck_connection.execute("PRAGMA threads=4")
    duck_connection.execute(
        f"""
        CREATE VIEW normalized_trips AS
        SELECT *
        FROM read_parquet({sql_literal(str(TRIPS_PARQUET_PATH))})
        """
    )
    duck_connection.execute(
        f"""
        CREATE VIEW normalized_stations AS
        SELECT *
        FROM read_parquet({sql_literal(str(STATIONS_PARQUET_PATH))})
        """
    )
    print(f"Using existing Parquet outputs from {PARQUET_DIR} ...")
    return duck_connection


def initialize_schema(pg_connection: psycopg2.extensions.connection) -> None:
    schema_sql = (SQL_DIR / "01_schema.sql").read_text()

    with pg_connection.cursor() as cursor:
        # Recreate the curated schema on each ingestion run so new columns,
        # constraints, and indexes are applied consistently.
        cursor.execute("DROP TABLE IF EXISTS trips CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS data_quality_log CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS stations CASCADE;")
        cursor.execute(schema_sql)

    pg_connection.commit()


def copy_query_to_postgres(
    duck_connection: duckdb.DuckDBPyConnection,
    pg_connection: psycopg2.extensions.connection,
    select_query: str,
    table_name: str,
    columns: list[str],
) -> int:
    copy_sql = (
        f"COPY {table_name} ({', '.join(columns)}) "
        "FROM STDIN WITH (FORMAT CSV)"
    )
    count_query = f"SELECT COUNT(*) FROM ({select_query}) source_rows"

    with tempfile.NamedTemporaryFile(
        mode="w+b",
        suffix=f"_{table_name}.csv",
        delete=False,
    ) as temp_file:
        temp_csv_path = Path(temp_file.name)

    try:
        print(f"Exporting {table_name} rows to {temp_csv_path} ...")
        duck_connection.execute(
            f"""
            COPY (
                {select_query}
            ) TO {sql_literal(str(temp_csv_path))}
            (FORMAT CSV, HEADER FALSE)
            """
        )

        with pg_connection.cursor() as cursor:
            with temp_csv_path.open("r", encoding="utf-8", newline="") as csv_file:
                cursor.copy_expert(copy_sql, csv_file)

        pg_connection.commit()
    finally:
        if temp_csv_path.exists():
            temp_csv_path.unlink()

    return duck_connection.execute(count_query).fetchone()[0]


def run_data_quality_sql(pg_connection: psycopg2.extensions.connection) -> None:
    quality_sql = (SQL_DIR / "02_data_quality.sql").read_text()
    insert_marker = "INSERT INTO data_quality_log"
    _, insert_sql = quality_sql.split(insert_marker, 1)

    with pg_connection.cursor() as cursor:
        cursor.execute(insert_marker + insert_sql)
    pg_connection.commit()


def main() -> int:
    args = parse_args()
    ensure_directories()

    if args.download:
        download_archives()

    archives = available_archives()
    if not archives:
        print(
            "No 2025 archives found in data/raw/. "
            "Run `python3 scripts/ingest.py --download` or add the files manually."
        )
        return 1

    csv_files = extract_archives(archives)
    if args.skip_parquet and parquet_outputs_exist():
        duck_connection = build_tables_from_parquet()
    else:
        duck_connection = build_normalized_tables(csv_files)
        if not args.skip_parquet:
            write_parquet(duck_connection)

    pg_connection = psycopg2.connect(**get_postgres_config())
    try:
        initialize_schema(pg_connection)

        station_count = copy_query_to_postgres(
            duck_connection,
            pg_connection,
            """
            SELECT station_id, station_name, station_lat, station_lng
            FROM normalized_stations
            ORDER BY station_id
            """,
            "stations",
            STATION_COLUMNS,
        )

        trip_count = copy_query_to_postgres(
            duck_connection,
            pg_connection,
            f"""
            SELECT {", ".join(TRIP_COLUMNS)}
            FROM (
                SELECT
                    {", ".join(TRIP_COLUMNS)},
                    ROW_NUMBER() OVER (
                        PARTITION BY ride_id
                        ORDER BY started_at, ended_at
                    ) AS ride_id_rank
                FROM normalized_trips
                WHERE ride_id IS NOT NULL
                  AND started_at IS NOT NULL
                  AND started_at_est IS NOT NULL
                  AND ended_at IS NOT NULL
                  AND member_casual IS NOT NULL
                  AND trip_duration_seconds IS NOT NULL
                  AND trip_duration_seconds > 0
                  AND trip_duration_seconds < 86400
                  AND ended_at >= started_at
            ) curated_trips
            WHERE ride_id_rank = 1
            """,
            "trips",
            TRIP_COLUMNS,
        )

        run_data_quality_sql(pg_connection)
    finally:
        pg_connection.close()
        duck_connection.close()

    print(f"Loaded {station_count:,} stations into PostgreSQL.")
    print(f"Loaded {trip_count:,} trips into PostgreSQL.")
    print("Data quality checks ran and logged summary counts to data_quality_log.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
