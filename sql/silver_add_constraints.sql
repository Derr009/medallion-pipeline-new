-- This script adds formal constraints to the Silver layer tables.
-- It should be run AFTER the Silver tables have been successfully built.

-- Step 1: Add Primary Keys
-- A Primary Key ensures every row is unique and cannot be NULL.
ALTER TABLE silver."Customers" ADD PRIMARY KEY (customer_id);
ALTER TABLE silver."Orders" ADD PRIMARY KEY (order_id);
ALTER TABLE silver."Shipments" ADD PRIMARY KEY (shipment_id);
ALTER TABLE silver."Drivers" ADD PRIMARY KEY (driver_id);
ALTER TABLE silver."Vehicles" ADD PRIMARY KEY (vehicle_id);

-- Step 2: Add Foreign Keys
-- A Foreign Key ensures that a value in one table must exist in another.
ALTER TABLE silver."Orders" ADD CONSTRAINT fk_customer
    FOREIGN KEY (customer_id) REFERENCES silver."Customers" (customer_id);

ALTER TABLE silver."Shipments" ADD CONSTRAINT fk_order
    FOREIGN KEY (order_id) REFERENCES silver."Orders" (order_id);

ALTER TABLE silver."Shipments" ADD CONSTRAINT fk_driver
    FOREIGN KEY (driver_id) REFERENCES silver."Drivers" (driver_id);

ALTER TABLE silver."Shipments" ADD CONSTRAINT fk_vehicle
    FOREIGN KEY (vehicle_id) REFERENCES silver."Vehicles" (vehicle_id);