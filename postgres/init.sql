CREATE USER orders WITH PASSWORD 'orders';
CREATE DATABASE orders OWNER orders;

\connect orders

SET ROLE orders;
SET TIME ZONE 'UTC';

CREATE SCHEMA raw AUTHORIZATION orders;
CREATE SCHEMA staging AUTHORIZATION orders;
CREATE SCHEMA analytics AUTHORIZATION orders;

CREATE TABLE raw.orders (
    order_id BIGINT PRIMARY KEY,
    ordered_at TIMESTAMPTZ NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL
);

CREATE TABLE staging.orders (
    order_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    ordered_at TIMESTAMPTZ NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (order_date, order_id)
);

-- Example input data for the previous completed UTC day.
-- CURRENT_DATE is used only to seed the demo, not in transformations.
INSERT INTO raw.orders (
    order_id,
    ordered_at,
    total_amount
)
VALUES
    (1, (CURRENT_DATE - 1) + TIME '09:00', 100.00),
    (2, (CURRENT_DATE - 1) + TIME '12:00',  50.00),
    (3, (CURRENT_DATE - 1) + TIME '18:00',  25.00),

    -- Today's order should not be loaded by yesterday's run.
    (4, CURRENT_DATE + TIME '09:00', 999.00);