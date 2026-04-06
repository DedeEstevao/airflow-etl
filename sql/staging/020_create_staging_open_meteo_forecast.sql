
CREATE TABLE IF NOT EXISTS staging.open_meteo_forecast (
    id BIGSERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    latitude NUMERIC(8,5) NOT NULL,
    longitude NUMERIC(8,5) NOT NULL,

    model_run_datetime TIMESTAMPTZ NOT NULL,
    forecast_datetime TIMESTAMPTZ NOT NULL,

    raw_payload_hash TEXT NOT NULL,

    temperature NUMERIC(5,2),
    precipitation NUMERIC(5,2),

    raw_ingested_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_staging_open_meteo
ON staging.open_meteo_forecast (
    latitude,
    longitude,
    forecast_datetime,
    model_run_datetime
);

CREATE INDEX IF NOT EXISTS idx_staging_model_run
ON staging.open_meteo_forecast (model_run_datetime);

CREATE INDEX IF NOT EXISTS idx_staging_forecast_datetime
ON staging.open_meteo_forecast (forecast_datetime);

