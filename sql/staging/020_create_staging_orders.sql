CREATE TABLE IF NOT EXISTS staging.orders (
    order_id     BIGINT PRIMARY KEY,
    customer_id  BIGINT NOT NULL,
    status       TEXT NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    created_at   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_orders_updated_at
ON staging.orders (updated_at);
