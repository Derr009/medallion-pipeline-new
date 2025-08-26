DROP TABLE IF EXISTS silver."Shipments" CASCADE;
CREATE TABLE silver."Shipments" AS
SELECT
    s.shipment_id::INTEGER,
    s.order_id::INTEGER,
    s.driver_id::INTEGER,
    s.vehicle_id::INTEGER,
    s.dispatch_date::TIMESTAMP,
    s.delivery_date::TIMESTAMP,
    CASE
        WHEN LOWER(s.status) IN ('delivered', 'd', 'completed') THEN 'Delivered'
        WHEN LOWER(s.status) IN ('in transit', 'in_transit', 'processing') THEN 'In Transit'
        ELSE 'Failed'
    END AS status
FROM
    bronze."Shipments" s
JOIN
    silver."Orders" o ON s.order_id = o.order_id::VARCHAR
JOIN
    silver."Drivers" d ON s.driver_id = d.driver_id::VARCHAR
JOIN
    silver."Vehicles" v ON s.vehicle_id = v.vehicle_id::VARCHAR
WHERE
    s.dispatch_date::TIMESTAMP <= s.delivery_date::TIMESTAMP;