DROP TABLE IF EXISTS silver."Customers";
CREATE TABLE silver."Customers" AS
WITH ranked_customers AS (
    SELECT
        customer_id,
        INITCAP(TRIM(customer_name)) AS customer_name,
        -- CORRECTED: Added REPLACE() to remove all spaces from the email
        REPLACE(LOWER(TRIM(email)), ' ', '') AS email,
        delivery_address,
        TO_DATE(signup_date, 'YYYY-MM-DD') AS signup_date,
        ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY TO_DATE(signup_date, 'YYYY-MM-DD') DESC) as rn
    FROM
        bronze."Customers"
    WHERE
        -- This check will now work on the cleaned email
        REPLACE(LOWER(TRIM(email)), ' ', '') LIKE '%@%.%'
        AND LOWER(email) NOT LIKE '%invalid%'
)
SELECT
    customer_id::INTEGER,
    customer_name,
    email,
    delivery_address,
    signup_date
FROM
    ranked_customers
WHERE
    rn = 1;