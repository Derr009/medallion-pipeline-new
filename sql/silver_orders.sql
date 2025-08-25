DROP TABLE IF EXISTS silver."Orders";
CREATE TABLE silver."Orders" AS
WITH cleaned_orders AS (
    SELECT
        order_id,
        customer_id,
        order_total,
        -- Use a CASE statement to handle different date formats safely
        CASE
            -- Format: YYYY-MM-DD
            WHEN order_date ~ '^\d{4}-\d{2}-\d{2}$'
                THEN TO_DATE(order_date, 'YYYY-MM-DD')
            -- Format: DD-Mon-YYYY (e.g., 25-Aug-2025)
            WHEN order_date ~ '^\d{2}-[A-Za-z]{3}-\d{4}$'
                THEN TO_DATE(order_date, 'DD-Mon-YYYY')
            -- Format: MM/DD/YYYY (check if first part is a valid month)
            WHEN order_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND SPLIT_PART(order_date, '/', 1)::INTEGER BETWEEN 1 AND 12
                THEN TO_DATE(order_date, 'MM/DD/YYYY')
            -- Format: DD/MM/YYYY (check if second part is a valid month)
            WHEN order_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND SPLIT_PART(order_date, '/', 2)::INTEGER BETWEEN 1 AND 12
                THEN TO_DATE(order_date, 'DD/MM/YYYY')
            ELSE NULL -- If none of the formats match, it's truly invalid
        END AS cleaned_order_date
    FROM
        bronze."Orders"
)
SELECT
    o.order_id::INTEGER,
    o.customer_id::INTEGER,
    o.cleaned_order_date AS order_date,
    REPLACE(REGEXP_REPLACE(o.order_total, '[^0-9.]', '', 'g'), ',', '')::DECIMAL(10, 2) AS order_total
FROM
    cleaned_orders o
-- Data Quality (FK) Check: Ensure the customer exists
JOIN
    silver."Customers" c ON o.customer_id = c.customer_id::VARCHAR
-- Data Quality Check: Filter out rows where date conversion failed
WHERE
    o.cleaned_order_date IS NOT NULL;