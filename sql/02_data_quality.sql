-- Data quality checks for the curated Citi Bike schema.

-- 1. NULL values in required analytic columns
SELECT 'ride_id' AS check_name, COUNT(*) AS issue_count
FROM trips
WHERE ride_id IS NULL
UNION ALL
SELECT 'started_at_nulls' AS check_name, COUNT(*) AS issue_count
FROM trips
WHERE started_at IS NULL
UNION ALL
SELECT 'ended_at_nulls' AS check_name, COUNT(*) AS issue_count
FROM trips
WHERE ended_at IS NULL
UNION ALL
SELECT 'member_casual_nulls' AS check_name, COUNT(*) AS issue_count
FROM trips
WHERE member_casual IS NULL;

-- 2. Negative or zero trip durations
SELECT
    COUNT(*) AS invalid_duration_rows
FROM trips
WHERE trip_duration_seconds IS NOT NULL
  AND trip_duration_seconds <= 0;

-- 3. Future timestamps
SELECT
    COUNT(*) AS future_started_at_rows
FROM trips
WHERE started_at > NOW();

-- 4. Station IDs in trips that do not exist in stations
SELECT
    COUNT(*) AS missing_start_station_rows
FROM trips t
LEFT JOIN stations s
    ON t.start_station_id = s.station_id
WHERE t.start_station_id IS NOT NULL
  AND s.station_id IS NULL;

SELECT
    COUNT(*) AS missing_end_station_rows
FROM trips t
LEFT JOIN stations s
    ON t.end_station_id = s.station_id
WHERE t.end_station_id IS NOT NULL
  AND s.station_id IS NULL;

-- 5. Duplicate ride_id values
SELECT
    ride_id,
    COUNT(*) AS duplicate_count
FROM trips
WHERE ride_id IS NOT NULL
GROUP BY ride_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, ride_id;

-- Optional: insert summary counts into data_quality_log
INSERT INTO data_quality_log (check_name, issue_count, details)
WITH checks AS (
    SELECT 'ride_id_nulls' AS check_name,
           COUNT(*) AS issue_count,
           'Trips missing ride_id values.' AS details
    FROM trips
    WHERE ride_id IS NULL

    UNION ALL

    SELECT 'started_at_nulls' AS check_name,
           COUNT(*) AS issue_count,
           'Trips missing started_at timestamps.' AS details
    FROM trips
    WHERE started_at IS NULL

    UNION ALL

    SELECT 'ended_at_nulls' AS check_name,
           COUNT(*) AS issue_count,
           'Trips missing ended_at timestamps.' AS details
    FROM trips
    WHERE ended_at IS NULL

    UNION ALL

    SELECT 'member_casual_nulls' AS check_name,
           COUNT(*) AS issue_count,
           'Trips missing member_casual values.' AS details
    FROM trips
    WHERE member_casual IS NULL

    UNION ALL

    SELECT 'invalid_trip_durations' AS check_name,
           COUNT(*) AS issue_count,
           'Trips with trip_duration_seconds <= 0.' AS details
    FROM trips
    WHERE trip_duration_seconds IS NOT NULL
      AND trip_duration_seconds <= 0

    UNION ALL

    SELECT 'future_started_at' AS check_name,
           COUNT(*) AS issue_count,
           'Trips with started_at in the future.' AS details
    FROM trips
    WHERE started_at > NOW()

    UNION ALL

    SELECT 'missing_start_station_reference' AS check_name,
           COUNT(*) AS issue_count,
           'Trip rows with a start_station_id not found in stations.' AS details
    FROM trips t
    LEFT JOIN stations s
        ON t.start_station_id = s.station_id
    WHERE t.start_station_id IS NOT NULL
      AND s.station_id IS NULL

    UNION ALL

    SELECT 'missing_end_station_reference' AS check_name,
           COUNT(*) AS issue_count,
           'Trip rows with an end_station_id not found in stations.' AS details
    FROM trips t
    LEFT JOIN stations s
        ON t.end_station_id = s.station_id
    WHERE t.end_station_id IS NOT NULL
      AND s.station_id IS NULL

    UNION ALL

    SELECT 'duplicate_ride_ids' AS check_name,
           COUNT(*) AS issue_count,
           'Distinct ride_id values that appear more than once.' AS details
    FROM (
        SELECT ride_id
        FROM trips
        WHERE ride_id IS NOT NULL
        GROUP BY ride_id
        HAVING COUNT(*) > 1
    ) duplicates
)
SELECT check_name, issue_count, details
FROM checks;

