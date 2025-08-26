DROP TABLE IF EXISTS silver."Drivers" CASCADE;
CREATE TABLE silver."Drivers" AS
WITH ranked_drivers AS (
    SELECT
        driver_id,
        TRIM(driver_name) AS driver_name,
        REGEXP_REPLACE(contact_number, '[^0-9]', '', 'g') AS contact_number,
        ROW_NUMBER() OVER(PARTITION BY driver_id ORDER BY driver_name DESC) as rn
    FROM
        bronze."Drivers"
    WHERE
        LOWER(driver_name) NOT LIKE '%test%'
)
SELECT
    driver_id::INTEGER,
    driver_name,
    contact_number
FROM
    ranked_drivers
WHERE
    rn = 1;