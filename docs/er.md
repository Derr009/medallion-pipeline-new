erDiagram
    %% Bronze Layer
    subgraph "Bronze Layer (Raw Data - All TEXT)"
        Customers_B[bronze.Customers]
        Orders_B[bronze.Orders]
        Shipments_B[bronze.Shipments]
        Drivers_B[bronze.Drivers]
        Vehicles_B[bronze.Vehicles]
    end

    %% Silver Layer
    subgraph "Silver Layer (Cleaned & Constrained)"
        Customers {
            INTEGER customer_id PK
            VARCHAR customer_name
            VARCHAR email
            VARCHAR delivery_address
            DATE signup_date
            VARCHAR driver_display_name
        }
        Orders {
            INTEGER order_id PK
            INTEGER customer_id FK
            DATE order_date
            DECIMAL order_total
        }
        Shipments {
            INTEGER shipment_id PK
            INTEGER order_id FK
            INTEGER driver_id FK
            INTEGER vehicle_id FK
            TIMESTAMP dispatch_date
            TIMESTAMP delivery_date
            VARCHAR status
        }
        Drivers {
            INTEGER driver_id PK
            VARCHAR driver_name
            VARCHAR contact_number
        }
        Vehicles {
            INTEGER vehicle_id PK
            VARCHAR license_plate
            VARCHAR vehicle_type
        }
    end

    %% Gold Layer
    subgraph "Gold Layer (Analytics Ready)"
        Monthly_Driver_Performance
        Vehicle_Failure_Analysis
        Customer_Value_Summary
        Full_Shipment_Details
    end

    %% Relationships in Silver Layer
    Customers ||--|{ Orders : "places"
    Orders ||--|| Shipments : "fulfills"
    Drivers ||--|{ Shipments : "assigned to"
    Vehicles ||--|{ Shipments : "used for"

    %% Data Flow from Silver to Gold
    Customers --> Customer_Value_Summary
    Orders --> Customer_Value_Summary
    Drivers --> Monthly_Driver_Performance
    Vehicles --> Vehicle_Failure_Analysis
    Shipments --o Monthly_Driver_Performance
    Shipments --o Vehicle_Failure_Analysis
    Shipments --o Full_Shipment_Details
    Orders --o Full_Shipment_Details
    Customers --o Full_Shipment_Details
    Drivers --o Full_Shipment_Details
    Vehicles --o Full_Shipment_Details
