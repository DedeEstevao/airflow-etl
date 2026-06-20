CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.weather_forecast (

    id BIGSERIAL PRIMARY KEY,

    city VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,

    model_run_datetime TIMESTAMPTZ NOT NULL,
    forecast_datetime TIMESTAMPTZ NOT NULL,

    raw_payload_hash TEXT NOT NULL,

    temperature DOUBLE PRECISION,
    precipitation_probability DOUBLE PRECISION,
    rain DOUBLE PRECISION,

    raw_ingested_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP

);

CREATE UNIQUE INDEX 
    IF NOT EXISTS uq_staging_open_meteo_forecast
    ON staging.weather_forecast (
        latitude,
        longitude,
        forecast_datetime,
        model_run_datetime
);

CREATE INDEX IF NOT EXISTS idx_staging_model_run
    ON staging.weather_forecast (model_run_datetime);

CREATE INDEX IF NOT EXISTS idx_staging_forecast_datetime
    ON staging.weather_forecast (forecast_datetime);

