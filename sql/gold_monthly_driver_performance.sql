DROP TABLE IF EXISTS gold."Monthly_Driver_Performance";
CREATE TABLE gold."Monthly_Driver_Performance" AS
SELECT
    d.driver_id,
    d.driver_name,
    EXTRACT(YEAR FROM s.dispatch_date) AS performance_year,
    EXTRACT(MONTH FROM s.dispatch_date) AS performance_month,
    COUNT(s.shipment_id) AS total_shipments,
    -- On-time is defined here as delivered within 72 hours
    SUM(CASE WHEN (s.delivery_date - s.dispatch_date) <= INTERVAL '72 hours' THEN 1 ELSE 0 END) AS on_time_shipments,
    -- CORRECTED: Calculate average delivery time in HOURS as a numeric value
    AVG(EXTRACT(EPOCH FROM (s.delivery_date - s.dispatch_date)) / 3600.0) AS avg_delivery_hours
FROM
    silver."Shipments" s
JOIN
    silver."Drivers" d ON s.driver_id = d.driver_id
GROUP BY
    d.driver_id,
    d.driver_name,
    performance_year,
    performance_month;