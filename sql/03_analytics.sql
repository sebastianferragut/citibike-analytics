-- Core Citi Bike analytics queries.
-- Public 2025 Citi Bike data does not expose a persistent rider_id,
-- so active-user and churn queries use a documented rider proxy.
\set QUIET 1

-- Session-level settings to keep local runs more stable on large trip volumes.
SET max_parallel_workers_per_gather = 0;
SET work_mem = '256MB';

-- Cache rider-day activity once so daily, weekly, and monthly proxy queries
-- do not repeatedly recompute the same local-time bucketing and MD5 logic.
DROP TABLE IF EXISTS rider_day_activity;

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
GROUP BY 1, 2;
\unset QUIET

-- Daily active users proxy
-- Business question: how many rider proxies are active on a typical day?
SELECT
    activity_day,
    COUNT(*) AS daily_active_users_proxy
FROM rider_day_activity
GROUP BY 1
ORDER BY 1;

-- Weekly active users proxy
-- Business question: how many rider proxies are active in each local week?
-- Week start dates keep the output readable at year boundaries.
-- Weekly and monthly proxy counts are calculated independently, so they are not additive.
SELECT
    DATE_TRUNC('week', activity_day)::date AS activity_week_start,
    COUNT(DISTINCT rider_proxy_id) AS weekly_active_users_proxy
FROM rider_day_activity
GROUP BY 1
ORDER BY 1;

-- Monthly active users proxy
-- Business question: how does rider engagement change across the 2025 year?
SELECT
    DATE_TRUNC('month', activity_day)::date AS activity_month,
    COUNT(DISTINCT rider_proxy_id) AS monthly_active_users_proxy
FROM rider_day_activity
GROUP BY 1
ORDER BY 1;

-- Month-over-month active user change
-- Business question: when does rider engagement accelerate or slow down?
WITH monthly_active_users AS (
    SELECT
        DATE_TRUNC('month', activity_day)::date AS activity_month,
        COUNT(DISTINCT rider_proxy_id) AS monthly_active_users_proxy
    FROM rider_day_activity
    GROUP BY 1
)
SELECT
    activity_month,
    monthly_active_users_proxy,
    LAG(monthly_active_users_proxy) OVER (
        ORDER BY activity_month
    ) AS prior_month_active_users_proxy,
    monthly_active_users_proxy
    - LAG(monthly_active_users_proxy) OVER (
        ORDER BY activity_month
    ) AS month_over_month_change,
    ROUND(
        (
            monthly_active_users_proxy::numeric
            / NULLIF(
                LAG(monthly_active_users_proxy) OVER (
                    ORDER BY activity_month
                ),
                0
            )
        ) - 1,
        4
    ) AS month_over_month_growth_rate
FROM monthly_active_users
ORDER BY activity_month;

-- Peak hours
-- Business question: when does trip demand peak during the local day?
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
ORDER BY 1;

-- Trip duration percentiles in minutes
-- Business question: what does a typical trip length look like after excluding extreme outliers?
SELECT
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY trip_duration_seconds) / 60.0 AS p50_minutes,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY trip_duration_seconds) / 60.0 AS p90_minutes,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY trip_duration_seconds) / 60.0 AS p95_minutes
FROM trips
WHERE trip_duration_seconds IS NOT NULL
  AND trip_duration_seconds > 0
  AND trip_duration_seconds < 86400;

-- Top start stations
-- Business question: which stations generate the most ride starts?
WITH normalized_trips AS (
    SELECT
        CASE
            WHEN t.start_station_id ~ '^[0-9]+(\.[0-9]+)?$' THEN
                TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM t.start_station_id))
            ELSE t.start_station_id
        END AS station_id,
        t.start_station_name
    FROM trips t
    WHERE t.start_station_id IS NOT NULL
),
normalized_stations AS (
    SELECT
        CASE
            WHEN s.station_id ~ '^[0-9]+(\.[0-9]+)?$' THEN
                TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM s.station_id))
            ELSE s.station_id
        END AS station_id,
        MAX(s.station_name) AS station_name
    FROM stations s
    GROUP BY 1
)
SELECT
    t.station_id,
    MAX(COALESCE(s.station_name, t.start_station_name)) AS station_name,
    COUNT(*) AS trip_count
FROM normalized_trips t
LEFT JOIN normalized_stations s
    ON t.station_id = s.station_id
GROUP BY t.station_id
ORDER BY trip_count DESC
LIMIT 20;

-- Top end stations
-- Business question: which stations receive the most ride endings?
WITH normalized_trips AS (
    SELECT
        CASE
            WHEN t.end_station_id ~ '^[0-9]+(\.[0-9]+)?$' THEN
                TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM t.end_station_id))
            ELSE t.end_station_id
        END AS station_id,
        t.end_station_name
    FROM trips t
    WHERE t.end_station_id IS NOT NULL
),
normalized_stations AS (
    SELECT
        CASE
            WHEN s.station_id ~ '^[0-9]+(\.[0-9]+)?$' THEN
                TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM s.station_id))
            ELSE s.station_id
        END AS station_id,
        MAX(s.station_name) AS station_name
    FROM stations s
    GROUP BY 1
)
SELECT
    t.station_id,
    MAX(COALESCE(s.station_name, t.end_station_name)) AS station_name,
    COUNT(*) AS trip_count
FROM normalized_trips t
LEFT JOIN normalized_stations s
    ON t.station_id = s.station_id
GROUP BY t.station_id
ORDER BY trip_count DESC
LIMIT 20;

-- Station inflow vs outflow imbalance
-- Business question: which stations are likely to need the most rebalancing?
WITH normalized_trips AS (
    SELECT
        CASE
            WHEN t.start_station_id ~ '^[0-9]+(\.[0-9]+)?$' THEN
                TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM t.start_station_id))
            ELSE t.start_station_id
        END AS normalized_start_station_id,
        CASE
            WHEN t.end_station_id ~ '^[0-9]+(\.[0-9]+)?$' THEN
                TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM t.end_station_id))
            ELSE t.end_station_id
        END AS normalized_end_station_id,
        t.start_station_name,
        t.end_station_name
    FROM trips t
),
normalized_stations AS (
    SELECT
        CASE
            WHEN s.station_id ~ '^[0-9]+(\.[0-9]+)?$' THEN
                TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM s.station_id))
            ELSE s.station_id
        END AS station_id,
        MAX(s.station_name) AS station_name
    FROM stations s
    GROUP BY 1
),
inflow AS (
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
LIMIT 20;

-- Member vs casual usage comparison
-- Business question: how do subscriber-like and casual usage patterns differ?
SELECT
    member_casual,
    COUNT(*) AS trip_count,
    ROUND(AVG(trip_duration_seconds) / 60.0, 2) AS avg_duration_minutes,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY trip_duration_seconds) / 60.0 AS median_duration_minutes
FROM trips
WHERE member_casual IS NOT NULL
  AND trip_duration_seconds IS NOT NULL
  AND trip_duration_seconds > 0
  AND trip_duration_seconds < 86400
GROUP BY member_casual
ORDER BY trip_count DESC;