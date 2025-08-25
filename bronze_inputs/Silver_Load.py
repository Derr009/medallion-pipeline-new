import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import re

# --- SUPABASE CREDENTIALS ---
DB_USER = "postgres.nywyaehimrbijnzsbouy"
DB_PASSWORD = "Nineleaps009"
DB_HOST = "aws-1-us-east-2.pooler.supabase.com"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_SCHEMA = "public"

# --- DATABASE CONNECTION ---
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- HELPER FUNCTIONS ---

def fetch_bronze(table_name):
    df = pd.read_sql(f"SELECT * FROM {DB_SCHEMA}.{table_name}", engine)
    return df

def is_valid_email(email):
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return bool(re.match(pattern, str(email)))

def clean_and_validate(df, table_name):
    df_copy = df.copy()
    dq_records = []

    # Trim string columns
    for col in df_copy.select_dtypes(include="object").columns:
        df_copy[col] = df_copy[col].astype(str).str.strip()

    # Validate IDs
    id_col = [c for c in df_copy.columns if "_id" in c]
    for col in id_col:
        invalid_ids = df_copy[df_copy[col].isnull() | (df_copy[col] == '')]
        for idx, row in invalid_ids.iterrows():
            dq_records.append({"table_name": table_name, "row_number": idx, "column_name": col, "issue_type": "missing_id"})
        df_copy = df_copy[df_copy[col].notnull() & (df_copy[col] != '')]

    # Validate emails
    if "email" in df_copy.columns:
        invalid_email_rows = df_copy[~df_copy["email"].apply(is_valid_email)]
        for idx, row in invalid_email_rows.iterrows():
            dq_records.append({"table_name": table_name, "row_number": idx, "column_name": "email", "issue_type": "invalid_email"})
        df_copy = df_copy[df_copy["email"].apply(is_valid_email)]

    # Phone validation
    if "phone" in df_copy.columns:
        invalid_phone = df_copy[~df_copy["phone"].str.match(r'^\d{10,15}$')]
        for idx, row in invalid_phone.iterrows():
            dq_records.append({"table_name": table_name, "row_number": idx, "column_name": "phone", "issue_type": "invalid_phone"})
        df_copy = df_copy[df_copy["phone"].str.match(r'^\d{10,15}$')]

    # Numeric columns
    num_cols = [c for c in df_copy.select_dtypes(include=["int64","float64"]).columns]
    for col in num_cols:
        invalid_num = df_copy[df_copy[col].isnull()]
        for idx, row in invalid_num.iterrows():
            dq_records.append({"table_name": table_name, "row_number": idx, "column_name": col, "issue_type": "missing_numeric"})
        df_copy[col] = df_copy[col].fillna(0)

    # Date columns
    date_cols = ["ship_date", "order_date", "delivery_date"]
    for col in date_cols:
        if col in df_copy.columns:
            df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
            invalid_date = df_copy[df_copy[col].isnull()]
            for idx, row in invalid_date.iterrows():
                dq_records.append({"table_name": table_name, "row_number": idx, "column_name": col, "issue_type": "invalid_date"})
            df_copy = df_copy[df_copy[col].notnull()]

    return df_copy, dq_records

def append_silver(df, table_name, pk_columns, dq_records):
    # Remove duplicates based on PKs
    pk_cols_str = ", ".join(pk_columns)
    query = f"SELECT {pk_cols_str} FROM {DB_SCHEMA}.{table_name}"
    existing_silver = pd.read_sql(query, engine)
    if not existing_silver.empty:
        df = df.merge(existing_silver, on=pk_columns, how="left", indicator=True)
        df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

    # Append valid rows
    if not df.empty:
        df.to_sql(table_name, engine, schema=DB_SCHEMA, if_exists='append', index=False)
        print(f"✅ {table_name} appended with {len(df)} rows")
    else:
        print(f"ℹ️ No new valid rows to append for {table_name}")

    # Log DQ issues
    if dq_records:
        dq_df = pd.DataFrame(dq_records)
        dq_df["batch_timestamp"] = datetime.now()
        dq_df.to_sql("dq_log_silver", engine, schema=DB_SCHEMA, if_exists='append', index=False)
        print(f"📄 Logged {len(dq_records)} data quality issues for {table_name}")

# --- SILVER TABLES AND PKs ---
silver_tables = {
    "silver_drivers": ["driver_id"],
    "silver_customers": ["customer_id"],
    "silver_vehicles": ["vehicle_id"],
    "silver_shipments": ["shipment_id"],
    "silver_orders": ["order_id"]
}

# --- PROCESS ---
for table, pk_cols in silver_tables.items():
    bronze_table = table.replace("silver", "bronze")
    df_bronze = fetch_bronze(bronze_table)
    df_clean, dq_logs = clean_and_validate(df_bronze, table)
    append_silver(df_clean, table, pk_cols, dq_logs)

print("✅ Incremental Silver ETL with DQ checks complete!")
