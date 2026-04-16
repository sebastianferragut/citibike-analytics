CREATE TABLE IF NOT EXISTS stations (
    station_id TEXT PRIMARY KEY,
    station_name TEXT NOT NULL,
    station_lat DOUBLE PRECISION,
    station_lng DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS data_quality_log (
    check_id BIGSERIAL PRIMARY KEY,
    check_name TEXT NOT NULL,
    issue_count BIGINT NOT NULL,
    details TEXT,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trips (
    trip_id BIGSERIAL PRIMARY KEY,
    ride_id TEXT NOT NULL UNIQUE,
    rideable_type TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    started_at_est TIMESTAMP NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL,
    start_station_name TEXT,
    start_station_id TEXT,
    end_station_name TEXT,
    end_station_id TEXT,
    start_lat DOUBLE PRECISION,
    start_lng DOUBLE PRECISION,
    end_lat DOUBLE PRECISION,
    end_lng DOUBLE PRECISION,
    member_casual TEXT NOT NULL,
    trip_duration_seconds BIGINT CHECK (trip_duration_seconds > 0),
    CONSTRAINT chk_trips_ended_after_started
        CHECK (ended_at >= started_at),
    CONSTRAINT fk_trips_start_station
        FOREIGN KEY (start_station_id) REFERENCES stations (station_id) ON DELETE SET NULL,
    CONSTRAINT fk_trips_end_station
        FOREIGN KEY (end_station_id) REFERENCES stations (station_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_trips_started_at
    ON trips (started_at);

CREATE INDEX IF NOT EXISTS idx_trips_started_at_est
    ON trips (started_at_est);

CREATE INDEX IF NOT EXISTS idx_trips_member_casual
    ON trips (member_casual);

CREATE INDEX IF NOT EXISTS idx_trips_started_at_member_casual
    ON trips (started_at, member_casual);

CREATE INDEX IF NOT EXISTS idx_trips_start_station_id
    ON trips (start_station_id);

CREATE INDEX IF NOT EXISTS idx_trips_end_station_id
    ON trips (end_station_id);
