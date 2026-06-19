CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.weather_forecast (
    id BIGSERIAL PRIMARY KEY,
    
    city TEXT,

    latitude DOUBLE PRECISION NOT NULL,

    longitude DOUBLE PRECISION NOT NULL,

    forecast_datetime TIMESTAMPTZ NOT NULL,

    temperature DOUBLE PRECISION NOT NULL,

    precipitation_probability DOUBLE PRECISION NOT NULL,

    rain DOUBLE PRECISION NOT NULL,

    model_run_datetime TIMESTAMPTZ NOT NULL,

    inserted_at TIMESTAMPTZ DEFAULT now(),

    updated_at TIMESTAMPTZ,

    CONSTRAINT uq_mart_weather_forecast UNIQUE (
        latitude,
        longitude,
        forecast_datetime
    )
);
