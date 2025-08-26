DROP TABLE IF EXISTS silver."Vehicles" CASCADE;
CREATE TABLE silver."Vehicles" AS
WITH ranked_vehicles AS (
    SELECT
        vehicle_id,
        TRIM(license_plate) AS license_plate,
        CASE
            WHEN LOWER(TRIM(vehicle_type)) LIKE '%van%' THEN 'Van'
            WHEN LOWER(TRIM(vehicle_type)) LIKE '%truck%' THEN 'Truck'
            WHEN LOWER(TRIM(vehicle_type)) LIKE '%motorcycle%' THEN 'Motorcycle'
            ELSE 'Other'
        END AS vehicle_type,
        ROW_NUMBER() OVER(PARTITION BY vehicle_id ORDER BY license_plate DESC) as rn
    FROM
        bronze."Vehicles"
)
SELECT
    vehicle_id::INTEGER,
    license_plate,
    vehicle_type
FROM
    ranked_vehicles
WHERE
    rn = 1;