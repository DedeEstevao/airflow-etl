CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.weather_forecast (

    forecast_id BIGSERIAL PRIMARY KEY,

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

    CONSTRAINT uq_raw_weather_forecast_payload
        UNIQUE (latitude, longitude, payload_hash)

);

CREATE INDEX IF NOT EXISTS idx_raw_ingested_at
    ON raw.weather_forecast (ingested_at);
