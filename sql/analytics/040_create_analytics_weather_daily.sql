CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.weather_daily (
    city TEXT,
    date DATE,
    avg_temperature DOUBLE PRECISION,
    min_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    avg_precipitation_probability DOUBLE PRECISION,
    model_run_datetime TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT uq_weather_daily UNIQUE (city, date)
);