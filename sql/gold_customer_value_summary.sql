DROP TABLE IF EXISTS gold."Customer_Value_Summary" CASCADE;
CREATE TABLE gold."Customer_Value_Summary" AS
SELECT
    c.customer_id,
    c.customer_name,
    c.signup_date,
    -- Customer Lifetime Value (LTV) Metrics
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_total) AS total_spend,
    AVG(o.order_total) AS avg_order_value,
    -- Customer Activity Metrics
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM
    silver."Customers" c
JOIN
    silver."Orders" o ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id,
    c.customer_name,
    c.signup_date;