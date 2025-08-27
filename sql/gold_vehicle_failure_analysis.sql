DROP TABLE IF EXISTS gold."Vehicle_Failure_Analysis" CASCADE;
CREATE TABLE gold."Vehicle_Failure_Analysis" AS
SELECT
    v.vehicle_type,
    EXTRACT(YEAR FROM s.dispatch_date) AS failure_year,
    EXTRACT(MONTH FROM s.dispatch_date) AS failure_month,
    -- Failure Metrics
    COUNT(s.shipment_id) AS count_of_failed_shipments,
    COUNT(DISTINCT s.driver_id) AS unique_drivers_involved
FROM
    silver."Shipments" s
JOIN
    silver."Vehicles" v ON s.vehicle_id = v.vehicle_id
-- Filter for only the failed shipments
WHERE
    s.status = 'Failed'
GROUP BY
    v.vehicle_type,
    failure_year,
    failure_month;