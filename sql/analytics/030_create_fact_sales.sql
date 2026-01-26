CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    order_id     BIGINT PRIMARY KEY,
    customer_id  BIGINT NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    created_at   TIMESTAMP NOT NULL
);
