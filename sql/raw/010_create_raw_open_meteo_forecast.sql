CREATE TABLE IF NOT EXISTS raw.open_meteo_forecast (

    id BIGSERIAL PRIMARY KEY,

    city  VARCHAR(100) NOT NULL,
    latitude NUMERIC NOT NULL,
    longitude NUMERIC NOT NULL,

    payload JSONB NOT NULL,

    payload_hash TEXT NOT NULL,

    ingested_at TIMESTAMPTZ DEFAULT NOW(),

    dag_run_id TEXT

);

CREATE UNIQUE INDEX IF NOT EXISTS uq_open_meteo_payload_hash
ON raw.open_meteo_forecast (latitude, longitude, payload_hash);

CREATE INDEX IF NOT EXISTS idx_raw_ingested_at
ON raw.open_meteo_forecast (ingested_at);

CREATE INDEX IF NOT EXISTS idx_raw_location
ON raw.open_meteo_forecast (latitude, longitude);

