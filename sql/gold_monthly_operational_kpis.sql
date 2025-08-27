DROP TABLE IF EXISTS gold."Monthly_Operational_KPIs" CASCADE;
CREATE TABLE gold."Monthly_Operational_KPIs" AS
SELECT
    EXTRACT(YEAR FROM s.dispatch_date) AS performance_year,
    EXTRACT(MONTH FROM s.dispatch_date) AS performance_month,
    -- Business Metrics
    SUM(o.order_total) AS total_revenue,
    COUNT(s.shipment_id) AS total_shipments,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    -- Performance KPIs
    AVG(EXTRACT(EPOCH FROM (s.delivery_date - s.dispatch_date)) / 3600.0) AS avg_delivery_hours,
    -- Calculate On-Time Rate (delivered within 72 hours)
    AVG(CASE WHEN (s.delivery_date - s.dispatch_date) <= INTERVAL '72 hours' THEN 1.0 ELSE 0.0 END) AS on_time_delivery_rate,
    -- Calculate Failure Rate
    AVG(CASE WHEN s.status = 'Failed' THEN 1.0 ELSE 0.0 END) AS failed_shipment_rate
FROM
    silver."Shipments" s
JOIN
    silver."Orders" o ON s.order_id = o.order_id
GROUP BY
    performance_year,
    performance_month
ORDER BY
    performance_year,
    performance_month;