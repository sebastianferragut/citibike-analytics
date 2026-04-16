from __future__ import annotations

import os

import psycopg2


def get_postgres_config() -> dict[str, str]:
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": os.getenv("PGPORT", "5432"),
        "dbname": os.getenv("PGDATABASE", "citibike"),
        "user": os.getenv("PGUSER", "citibike"),
        "password": os.getenv("PGPASSWORD", "citibike"),
    }


def fetch_rows(cursor: psycopg2.extensions.cursor, query: str) -> tuple[list[str], list[tuple]]:
    cursor.execute(query)
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    return columns, rows


def prepare_temp_tables(cursor: psycopg2.extensions.cursor) -> None:
    cursor.execute(
        """
        CREATE TEMP TABLE rider_day_activity AS
        SELECT
            (started_at AT TIME ZONE 'America/New_York')::date AS activity_day,
            MD5(
                CONCAT_WS(
                    '|',
                    COALESCE(member_casual, 'unknown'),
                    COALESCE(rideable_type, 'unknown'),
                    COALESCE(start_station_id, 'unknown'),
                    COALESCE(end_station_id, 'unknown')
                )
            ) AS rider_proxy_id
        FROM trips
        WHERE started_at AT TIME ZONE 'America/New_York' >= TIMESTAMP '2025-01-01 00:00:00'
          AND started_at AT TIME ZONE 'America/New_York' < TIMESTAMP '2026-01-01 00:00:00'
        GROUP BY 1, 2
        """
    )
    cursor.execute(
        """
        CREATE TEMP TABLE normalized_stations AS
        SELECT
            CASE
                WHEN s.station_id ~ '^[0-9]+(\\.[0-9]+)?$' THEN
                    TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM s.station_id))
                ELSE s.station_id
            END AS station_id,
            MAX(s.station_name) AS station_name
        FROM stations s
        GROUP BY 1
        """
    )
    cursor.execute(
        """
        CREATE TEMP TABLE normalized_trips AS
        SELECT
            CASE
                WHEN t.start_station_id ~ '^[0-9]+(\\.[0-9]+)?$' THEN
                    TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM t.start_station_id))
                ELSE t.start_station_id
            END AS normalized_start_station_id,
            CASE
                WHEN t.end_station_id ~ '^[0-9]+(\\.[0-9]+)?$' THEN
                    TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM t.end_station_id))
                ELSE t.end_station_id
            END AS normalized_end_station_id,
            t.start_station_name,
            t.end_station_name
        FROM trips t
        """
    )


def print_table(title: str, columns: list[str], rows: list[tuple]) -> None:
    print(f"\n{title}")
    print("-" * len(title))

    if not rows:
        print("No rows returned.")
        return

    widths = []
    for index, column in enumerate(columns):
        column_width = len(column)
        value_width = max(len(str(row[index])) for row in rows)
        widths.append(max(column_width, value_width))

    header = " | ".join(column.ljust(widths[index]) for index, column in enumerate(columns))
    divider = "-+-".join("-" * width for width in widths)
    print(header)
    print(divider)

    for row in rows:
        print(" | ".join(str(value).ljust(widths[index]) for index, value in enumerate(row)))


def print_section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    print("NYC Citi Bike Product Analytics Summary")
    print("Active user metrics use a documented rider proxy because the public 2025 feed has no persistent user_id.\n")

    with psycopg2.connect(**get_postgres_config()) as connection:
        with connection.cursor() as cursor:
            prepare_temp_tables(cursor)

            print_section("Daily Metrics")
            daily_query_specs = [
                (
                    "Recent Daily Active Users (Proxy)",
                    """
                    SELECT
                        activity_day,
                        COUNT(*) AS daily_active_users_proxy
                    FROM rider_day_activity
                    GROUP BY 1
                    ORDER BY 1 DESC
                    LIMIT 10
                    """,
                ),
                (
                    "Monthly Active Users (Proxy)",
                    """
                    SELECT
                        DATE_TRUNC('month', activity_day)::date AS activity_month,
                        COUNT(DISTINCT rider_proxy_id) AS monthly_active_users_proxy
                    FROM rider_day_activity
                    GROUP BY 1
                    ORDER BY 1
                    """,
                ),
                (
                    "Trips by Hour (NYC Local Time)",
                    """
                    WITH base AS (
                        SELECT
                            started_at AT TIME ZONE 'America/New_York' AS started_at_est
                        FROM trips
                        WHERE started_at AT TIME ZONE 'America/New_York' >= TIMESTAMP '2025-01-01 00:00:00'
                          AND started_at AT TIME ZONE 'America/New_York' < TIMESTAMP '2026-01-01 00:00:00'
                    )
                    SELECT
                        EXTRACT(HOUR FROM started_at_est) AS trip_hour,
                        LPAD(CAST(EXTRACT(HOUR FROM started_at_est) AS VARCHAR), 2, '0') || ':00' AS trip_hour_label,
                        COUNT(*) AS trip_count
                    FROM base
                    GROUP BY 1, 2
                    ORDER BY 1
                    """,
                ),
            ]

            for title, query in daily_query_specs:
                columns, rows = fetch_rows(cursor, query)
                print_table(title, columns, rows)

            print_section("Station Insights")
            station_query_specs = [
                (
                    "Top Start Stations",
                    """
                    SELECT
                        t.normalized_start_station_id AS station_id,
                        MAX(COALESCE(s.station_name, t.start_station_name)) AS station_name,
                        COUNT(*) AS trip_count
                    FROM normalized_trips t
                    LEFT JOIN normalized_stations s
                        ON t.normalized_start_station_id = s.station_id
                    WHERE t.normalized_start_station_id IS NOT NULL
                    GROUP BY t.normalized_start_station_id
                    ORDER BY trip_count DESC
                    LIMIT 10
                    """,
                ),
                (
                    "Station Inflow vs Outflow Imbalance",
                    """
                    WITH inflow AS (
                        SELECT
                            t.normalized_end_station_id AS station_id,
                            MAX(COALESCE(s.station_name, t.end_station_name)) AS station_name,
                            COUNT(*) AS inbound_trips
                        FROM normalized_trips t
                        LEFT JOIN normalized_stations s
                            ON t.normalized_end_station_id = s.station_id
                        WHERE t.normalized_end_station_id IS NOT NULL
                        GROUP BY 1
                    ),
                    outflow AS (
                        SELECT
                            t.normalized_start_station_id AS station_id,
                            MAX(COALESCE(s.station_name, t.start_station_name)) AS station_name,
                            COUNT(*) AS outbound_trips
                        FROM normalized_trips t
                        LEFT JOIN normalized_stations s
                            ON t.normalized_start_station_id = s.station_id
                        WHERE t.normalized_start_station_id IS NOT NULL
                        GROUP BY 1
                    )
                    SELECT
                        COALESCE(i.station_id, o.station_id) AS station_id,
                        COALESCE(i.station_name, o.station_name) AS station_name,
                        COALESCE(i.inbound_trips, 0) AS inbound_trips,
                        COALESCE(o.outbound_trips, 0) AS outbound_trips,
                        COALESCE(i.inbound_trips, 0) - COALESCE(o.outbound_trips, 0) AS net_inflow
                    FROM inflow i
                    FULL OUTER JOIN outflow o
                        ON i.station_id = o.station_id
                    ORDER BY ABS(COALESCE(i.inbound_trips, 0) - COALESCE(o.outbound_trips, 0)) DESC
                    LIMIT 10
                    """,
                ),
            ]

            for title, query in station_query_specs:
                columns, rows = fetch_rows(cursor, query)
                print_table(title, columns, rows)

            print_section("User Behavior")
            user_query_specs = [
                (
                    "Member vs Casual Usage",
                    """
                    SELECT
                        member_casual,
                        COUNT(*) AS trip_count,
                        ROUND(AVG(trip_duration_seconds) / 60.0, 2) AS avg_duration_minutes,
                        ROUND(
                            (
                                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY trip_duration_seconds) / 60.0
                            )::numeric,
                            2
                        ) AS median_duration_minutes
                    FROM trips
                    WHERE member_casual IS NOT NULL
                      AND trip_duration_seconds IS NOT NULL
                      AND trip_duration_seconds > 0
                      AND trip_duration_seconds < 86400
                    GROUP BY member_casual
                    ORDER BY trip_count DESC
                    """,
                ),
            ]

            for title, query in user_query_specs:
                columns, rows = fetch_rows(cursor, query)
                print_table(title, columns, rows)


if __name__ == "__main__":
    main()
