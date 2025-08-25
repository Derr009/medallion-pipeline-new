DROP TABLE IF EXISTS gold."Vehicle_Utilization_Summary";
CREATE TABLE gold."Vehicle_Utilization_Summary" AS
SELECT
    v.vehicle_type,
    EXTRACT(YEAR FROM s.dispatch_date) AS usage_year,
    EXTRACT(MONTH FROM s.dispatch_date) AS usage_month,
    COUNT(s.shipment_id) AS total_shipments,
    -- CORRECTED: Calculate average delivery time in HOURS as a numeric value
    AVG(EXTRACT(EPOCH FROM (s.delivery_date - s.dispatch_date)) / 3600.0) AS avg_delivery_hours
FROM
    silver."Shipments" s
JOIN
    silver."Vehicles" v ON s.vehicle_id = v.vehicle_id
GROUP BY
    v.vehicle_type,
    usage_year,
    usage_month;