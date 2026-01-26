CREATE TABLE IF NOT EXISTS raw.orders (
    id           BIGSERIAL PRIMARY KEY,
    payload      JSONB NOT NULL,
    extracted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    source       TEXT NOT NULL
);
