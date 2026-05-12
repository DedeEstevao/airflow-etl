CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.model_metadata (

    id BIGSERIAL PRIMARY KEY,

    model_name TEXT NOT NULL,

    model_version TEXT NOT NULL,

    trained_at TIMESTAMPTZ NOT NULL,

    training_rows INTEGER,

    mae DOUBLE PRECISION,

    rmse DOUBLE PRECISION,

    r2 DOUBLE PRECISION,

    model_path TEXT,

    min_forecast_datetime TIMESTAMPTZ,

    max_forecast_datetime TIMESTAMPTZ,

    is_active BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_model_metadata_active
ON analytics.model_metadata(is_active);

CREATE INDEX IF NOT EXISTS idx_model_metadata_trained_at
ON analytics.model_metadata(trained_at);
