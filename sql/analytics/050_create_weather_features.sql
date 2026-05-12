CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.weather_features (

    id BIGSERIAL PRIMARY KEY,

    -- =====================================================
    -- IDENTIFICAÇÃO
    -- =====================================================

    city TEXT NOT NULL,

    latitude DOUBLE PRECISION NOT NULL,

    longitude DOUBLE PRECISION NOT NULL,

    forecast_datetime TIMESTAMPTZ NOT NULL,

    -- =====================================================
    -- FEATURES BASE
    -- =====================================================

    temperature DOUBLE PRECISION,

    precipitation DOUBLE PRECISION,

    -- =====================================================
    -- FEATURES TEMPORAIS
    -- =====================================================

    hour INTEGER,

    hour_sin DOUBLE PRECISION,

    hour_cos DOUBLE PRECISION,

    -- =====================================================
    -- LAGS TEMPERATURA
    -- =====================================================

    temp_lag_1 DOUBLE PRECISION,

    temp_lag_2 DOUBLE PRECISION,

    temp_lag_3 DOUBLE PRECISION,

    temp_lag_6 DOUBLE PRECISION,

    -- =====================================================
    -- LAGS PRECIPITAÇÃO
    -- =====================================================

    precip_lag_1 DOUBLE PRECISION,

    precip_lag_2 DOUBLE PRECISION,

    -- =====================================================
    -- DIFERENÇAS
    -- =====================================================

    temp_diff DOUBLE PRECISION,

    -- =====================================================
    -- MÉDIAS MÓVEIS
    -- =====================================================

    temp_roll_mean_3 DOUBLE PRECISION,

    temp_roll_mean_6 DOUBLE PRECISION,

    -- =====================================================
    -- FEATURES DERIVADAS
    -- =====================================================

    rain_flag INTEGER,

    temp_x_rain DOUBLE PRECISION,

    -- =====================================================
    -- TARGET SUPERVISIONADO
    -- =====================================================

    target DOUBLE PRECISION,

    -- =====================================================
    -- METADATA
    -- =====================================================

    created_at TIMESTAMPTZ DEFAULT now(),

    -- =====================================================
    -- CONSTRAINTS
    -- =====================================================

    CONSTRAINT uq_weather_features
    UNIQUE (
        city,
        forecast_datetime
    )
);

-- =====================================================
-- INDEXES
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_weather_features_city
ON analytics.weather_features(city);

CREATE INDEX IF NOT EXISTS idx_weather_features_forecast_datetime
ON analytics.weather_features(forecast_datetime);

CREATE INDEX IF NOT EXISTS idx_weather_features_city_datetime
ON analytics.weather_features(
    city,
    forecast_datetime
);