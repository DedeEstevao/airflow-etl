CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.weather_observed (

    observed_id BIGSERIAL PRIMARY KEY,

    city  VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,

    payload JSONB NOT NULL,

    payload_hash TEXT NOT NULL,

    dag_run_id TEXT,

    ingested_at TIMESTAMPTZ
        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    source_name TEXT
        DEFAULT 'open-meteo',

    CONSTRAINT uq_open_meteo_observed_payload
        UNIQUE (latitude, longitude, payload_hash) 
);


CREATE INDEX IF NOT EXISTS idx_weather_observed_ingested_at
    ON raw.weather_observed (ingested_at);
