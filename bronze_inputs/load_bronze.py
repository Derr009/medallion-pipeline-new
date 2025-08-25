import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from sqlalchemy import create_engine
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
DB_SCHEMA = "public"  # schema where tables exist

# --- AUTHENTICATION ---
scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
client = gspread.authorize(creds)

# --- DATABASE CONNECTION ---
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- HELPER FUNCTION TO FETCH SHEET ---
def fetch_sheet(sheet_name):
    worksheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    # --- TYPE CORRECTIONS ---
    if sheet_name == "Vehicles":
        df["capacity_kg"] = pd.to_numeric(df["capacity_kg"], errors='coerce').fillna(0).astype(int)
    if sheet_name in ["Drivers", "Customers"]:
        df["phone"] = df["phone"].astype(str)

    # Explicit date conversions
    date_columns = ["ship_date", "order_date", "delivery_date"]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    return df

# --- HELPER FUNCTION TO APPEND DATA TO SQL WITH LOGGING ---
def append_to_sql(df, table_name):
    df_copy = df.copy()

    # Append to existing table
    df_copy.to_sql(
        table_name,
        engine,
        schema=DB_SCHEMA,
        if_exists='append',
        index=False
    )

    # Compute row count and checksum
    # Compute row count and checksum
    row_count = len(df_copy)
    checksum = int(pd.util.hash_pandas_object(df_copy).sum())  # cast uint64 → int64
    print(f"✅ {table_name} appended with {row_count} rows, checksum: {checksum}")

    # Log ETL batch
    # --- Log the ETL batch ---
    log_df = pd.DataFrame({
        "table_name": [table_name],
        "batch_timestamp": [datetime.now()],
        "row_count": [row_count],
        "checksum": [int(checksum)]  # ensure it's Python int
    })

    # Force the dtype of checksum to int64
    log_df["checksum"] = log_df["checksum"].astype("int64")

    log_df.to_sql(
        "etl_log_bronze",
        engine,
        schema=DB_SCHEMA,
        if_exists='append',
        index=False
    )
    print(f"📄 ETL log updated for {table_name}")


# --- LIST OF TABS TO LOAD ---
sheets = ["Drivers", "Customers", "Vehicles", "Shipments", "Orders"]

for sheet in sheets:
    df = fetch_sheet(sheet)
    append_to_sql(df, f"bronze_{sheet.lower()}")

print("✅ Bronze Layer append complete!")
