CREATE TABLE IF NOT EXISTS raw.open_meteo_forecast (

    id           BIGSERIAL PRIMARY KEY,

    latitude     NUMERIC (8,5) NOT NULL,
    longitude    NUMERIC (8,5) NOT NULL,

    payload      JSONB NOT NULL,

    ingested_at TIMESTAMP WITHOUT TIME ZONE
        DEFAULT CURRENT_TIMESTAMP
);
