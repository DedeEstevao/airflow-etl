CREATE TABLE IF NOT EXISTS mart.open_meteo_forecast (
    city TEXT,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    forecast_datetime TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    precipitation_probability DOUBLE PRECISION,
    model_run_datetime TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ,

    CONSTRAINT uq_weather_forecast UNIQUE (
        latitude,
        longitude,
        forecast_datetime
    )
);
