CREATE SCHEMA IF NOT EXISTS analytics;


CREATE TABLE IF NOT EXISTS analytics.weather_predictions  (

    prediction_id BIGSERIAL PRIMARY KEY,

    city VARCHAR(100),

    latitude NUMERIC(8,4),
    longitude NUMERIC(8,4),

    forecast_datetime TIMESTAMPTZ,

    predicted_temperature NUMERIC(10,2),

    api_temperature NUMERIC(10,2),

    prediction_error NUMERIC(10,2),

    model_version VARCHAR(50),

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_weather_prediction
    UNIQUE (
        latitude,
        longitude,
        forecast_datetime,
        model_version
    )
);
