SELECT
    order_date,
    COUNT(*) AS order_count,
    SUM(total_amount)::NUMERIC(18, 2) AS total_revenue
FROM {{ source('staging', 'orders') }}
GROUP BY order_date