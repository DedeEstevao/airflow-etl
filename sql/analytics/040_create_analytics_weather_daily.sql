CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.weather_daily (

    id BIGSERIAL PRIMARY KEY,

    city TEXT NOT NULL,

    latitude DOUBLE PRECISION NOT NULL,

    longitude DOUBLE PRECISION NOT NULL,

    forecast_date DATE NOT NULL,

    avg_temperature DOUBLE PRECISION,

    min_temperature DOUBLE PRECISION,

    max_temperature DOUBLE PRECISION,

    avg_precipitation_probability DOUBLE PRECISION,

    total_rain DOUBLE PRECISION,
    
    max_rain DOUBLE PRECISION,

    model_run_datetime TIMESTAMPTZ,

    created_at TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT uq_weather_daily
    UNIQUE (
        latitude,
        longitude,
        forecast_date
    )
);

-- =====================================================
-- INDEXES
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_weather_daily_city
ON analytics.weather_daily(latitude, longitude);

CREATE INDEX IF NOT EXISTS idx_weather_daily_forecast_date
ON analytics.weather_daily(forecast_date);

CREATE INDEX IF NOT EXISTS idx_weather_daily_model_run
ON analytics.weather_daily(model_run_datetime);
