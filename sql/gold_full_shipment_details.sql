DROP TABLE IF EXISTS gold."Full_Shipment_Details";
CREATE TABLE gold."Full_Shipment_Details" AS
SELECT
    s.shipment_id,
    s.status AS shipment_status,
    s.dispatch_date,
    s.delivery_date,
    -- CORRECTED: Calculate delivery duration in HOURS as a numeric value
    EXTRACT(EPOCH FROM (s.delivery_date - s.dispatch_date)) / 3600.0 AS delivery_hours,
    o.order_id,
    o.order_date,
    o.order_total,
    c.customer_id,
    c.customer_name,
    c.delivery_address,
    TRIM(SPLIT_PART(c.delivery_address, ',', 2)) AS customer_city,
    d.driver_id,
    d.driver_name,
    v.vehicle_id,
    v.license_plate,
    v.vehicle_type
FROM
    silver."Shipments" s
JOIN
    silver."Orders" o ON s.order_id = o.order_id
JOIN
    silver."Customers" c ON o.customer_id = c.customer_id
JOIN
    silver."Drivers" d ON s.driver_id = d.driver_id
JOIN
    silver."Vehicles" v ON s.vehicle_id = v.vehicle_id;