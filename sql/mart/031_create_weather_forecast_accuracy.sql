CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.weather_forecast_accuracy (
    id BIGSERIAL PRIMARY KEY,

    city TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,

    model_run_datetime TIMESTAMPTZ NOT NULL,
    forecast_datetime TIMESTAMPTZ NOT NULL,

    -- Lead time (ESSENCIAL para análise de forecast)
    lead_time_hours INT GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (forecast_datetime - model_run_datetime)) / 3600
    ) STORED,

    -- Forecast (predição)
    predicted_temperature DOUBLE PRECISION,
    predicted_rain DOUBLE PRECISION,
    precipitation_probability DOUBLE PRECISION,

    -- Observado
    observed_temperature DOUBLE PRECISION,
    observed_rain DOUBLE PRECISION,

    -- Erros (temperatura)
    temperature_error DOUBLE PRECISION GENERATED ALWAYS AS (
        predicted_temperature - observed_temperature
    ) STORED,

    absolute_temperature_error DOUBLE PRECISION GENERATED ALWAYS AS (
        ABS(predicted_temperature - observed_temperature)
    ) STORED,

    -- Erros (chuva)
    rain_error DOUBLE PRECISION GENERATED ALWAYS AS (
        predicted_rain - observed_rain
    ) STORED,

    absolute_rain_error DOUBLE PRECISION GENERATED ALWAYS AS (
        ABS(predicted_rain - observed_rain)
    ) STORED,

    -- controle de ingestão
    raw_forecast_ingested_at TIMESTAMPTZ,
    raw_observed_ingested_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_forecast_accuracy UNIQUE (
        latitude,
        longitude,
        forecast_datetime,
        model_run_datetime
    )
);
