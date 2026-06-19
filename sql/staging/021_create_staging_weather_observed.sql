CREATE SCHEMA IF NOT EXISTS staging;


CREATE TABLE IF NOT EXISTS staging.weather_observed (

    observed_staging_id BIGSERIAL PRIMARY KEY,
    
    city VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,

    observation_datetime TIMESTAMPTZ NOT NULL,

    observed_temperature DOUBLE PRECISION,

    observed_rain DOUBLE PRECISION,

    raw_payload_hash TEXT NOT NULL,

    raw_ingested_at TIMESTAMPTZ NOT NULL,

    created_at TIMESTAMPTZ
        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_observed_staging_record
    UNIQUE (
        latitude,
        longitude,
        observation_datetime
    )
);


CREATE INDEX IF NOT EXISTS idx_observed_datetime
ON staging.weather_observed (
    observation_datetime
);


CREATE INDEX IF NOT EXISTS idx_observed_coordinates
ON staging.weather_observed (
    latitude,
    longitude
);


CREATE INDEX IF NOT EXISTS idx_observed_raw_ingested_at
ON staging.weather_observed (
    raw_ingested_at
);
