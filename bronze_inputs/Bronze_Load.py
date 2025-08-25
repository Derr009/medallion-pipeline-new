import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from sqlalchemy import create_engine, inspect
from datetime import datetime

# --- CONFIG ---
SERVICE_ACCOUNT_FILE = "capstone-467705-65af793df23b.json"
SPREADSHEET_ID = "1X9hLdpy_aHhlxeVbkre9VnCf4MoVO0FU1AF_l1nOCk4"

# --- SUPABASE CREDENTIALS ---
DB_USER = "postgres.nywyaehimrbijnzsbouy"
DB_PASSWORD = "Nineleaps009"
DB_HOST = "aws-1-us-east-2.pooler.supabase.com"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_SCHEMA = "public"

# --- AUTHENTICATION ---
scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
client = gspread.authorize(creds)

# --- DATABASE CONNECTION ---
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
inspector = inspect(engine)

# --- PRIMARY KEYS FOR EACH SHEET ---
pk_mapping = {
    "Drivers": "driver_id",
    "Customers": "customer_id",
    "Vehicles": "vehicle_id",
    "Shipments": "shipment_id",
    "Orders": "order_id"
}

# --- HELPER FUNCTION TO FETCH SHEET ---
def fetch_sheet(sheet_name):
    worksheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    # --- TYPE CORRECTIONS ---
    if sheet_name == "Vehicles" and "capacity_kg" in df.columns:
        df["capacity_kg"] = pd.to_numeric(df["capacity_kg"], errors='coerce').fillna(0).astype(int)
    if sheet_name in ["Drivers", "Customers"] and "phone" in df.columns:
        df["phone"] = df["phone"].astype(str)

    # Explicit date conversions
    date_columns = ["ship_date", "order_date", "delivery_date"]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    return df

# --- HELPER FUNCTION TO APPEND ONLY NEW ROWS ---
def append_new_rows(df, table_name, pk_column):
    df_copy = df.copy()

    # Check if table exists
    if table_name not in inspector.get_table_names(schema=DB_SCHEMA):
        # Table does not exist -> create
        df_copy.to_sql(table_name, engine, schema=DB_SCHEMA, if_exists='fail', index=False)
        print(f"🟢 {table_name} created with {len(df_copy)} rows")
        new_rows_count = len(df_copy)
    else:
        # Table exists -> fetch existing PKs
        existing_pks = pd.read_sql(f"SELECT {pk_column} FROM {DB_SCHEMA}.{table_name}", engine)
        df_copy = df_copy[~df_copy[pk_column].isin(existing_pks[pk_column])]
        if df_copy.empty:
            print(f"ℹ️ No new rows to append for {table_name}")
            return
        # Append only new rows
        df_copy.to_sql(table_name, engine, schema=DB_SCHEMA, if_exists='append', index=False)
        print(f"✅ {table_name} appended with {len(df_copy)} new rows")
        new_rows_count = len(df_copy)

    # --- ETL logging ---
    checksum = int(pd.util.hash_pandas_object(df_copy).sum())
    log_df = pd.DataFrame({
        "table_name": [table_name],
        "batch_timestamp": [datetime.now()],
        "row_count": [new_rows_count],
        "checksum": [checksum]
    })
    log_df["checksum"] = log_df["checksum"].astype("int64")
    log_df.to_sql("etl_log_bronze", engine, schema=DB_SCHEMA, if_exists='append', index=False)
    print(f"📄 ETL log updated for {table_name}")

# --- LOAD ALL SHEETS ---
sheets = ["Drivers", "Customers", "Vehicles", "Shipments", "Orders"]

for sheet in sheets:
    df = fetch_sheet(sheet)
    append_new_rows(df, f"bronze_{sheet.lower()}", pk_mapping[sheet])

print("✅ Bronze Layer incremental load complete!")
