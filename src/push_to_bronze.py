import os
import gspread
import hashlib
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# --- 1. CONFIGURATION & INITIALIZATION ---

# Define project paths relative to this script's location
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
BRONZE_INPUTS_DIR = BASE_DIR / "bronze_inputs"
CONFIG_DIR = BASE_DIR / "config"

# Create necessary directories
LOG_DIR.mkdir(exist_ok=True)
BRONZE_INPUTS_DIR.mkdir(exist_ok=True)
CONFIG_DIR.mkdir(exist_ok=True)

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "etl_bronze.log"),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
SHEET_NAME = os.getenv("GSPREAD_SHEET_NAME")
CREDENTIALS_PATH = CONFIG_DIR / "credentials.json"

# List of tables to be processed
TABLE_NAMES = ["Customers", "Orders", "Shipments", "Drivers", "Vehicles"]


# --- 2. HELPER FUNCTIONS ---

def get_db_engine():
    """Creates and returns a SQLAlchemy engine for PostgreSQL."""
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        with engine.connect():
            logging.info("Successfully connected to the PostgreSQL database.")
        return engine
    except OperationalError as e:
        logging.error(f"Could not connect to the database. Error: {e}")
        raise


def create_bronze_schema(engine):
    """Ensures the 'bronze' schema exists in the database."""
    with engine.connect() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        connection.commit()
        logging.info("Schema 'bronze' created or already exists.")


def calculate_checksum(file_path):
    """Calculates the SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


# --- 3. ETL CORE FUNCTIONS ---

def extract_from_gsheets():
    """
    Extracts data from all worksheets in a Google Sheet and saves them as CSV files,
    overwriting any existing files.
    """
    logging.info("--- Starting EXTRACT step: Full extraction from Google Sheets ---")
    try:
        gc = gspread.service_account(filename=CREDENTIALS_PATH)
        spreadsheet = gc.open(SHEET_NAME)
        logging.info(f"Successfully connected to Google Sheet: '{SHEET_NAME}'")

        for table_name in TABLE_NAMES:
            try:
                worksheet = spreadsheet.worksheet(table_name)
                df = pd.DataFrame(worksheet.get_all_records())

                output_path = BRONZE_INPUTS_DIR / f"{table_name}.csv"
                df.to_csv(output_path, index=False)

                checksum = calculate_checksum(output_path)
                logging.info(
                    f"  - Extracted '{table_name}' to CSV. "
                    f"Rows: {len(df)}, Checksum: {checksum[:8]}..."
                )
            except gspread.WorksheetNotFound:
                logging.error(f"  - Worksheet '{table_name}' not found in the Google Sheet.")
            except Exception as e:
                logging.error(f"  - Failed to extract '{table_name}'. Error: {e}")

        logging.info("--- EXTRACT step completed successfully. ---")

    except Exception as e:
        logging.error(f"An error occurred during the extraction phase. Error: {e}")
        raise


def load_to_bronze(engine):
    """
    Loads CSV files into the 'bronze' schema using a 'replace' strategy for all tables.
    This ensures the Bronze layer is always a perfect mirror of the source files.
    """
    logging.info("--- Starting LOAD step: CSV to Bronze Layer (Full Replace) ---")
    create_bronze_schema(engine)

    for table_name in TABLE_NAMES:
        csv_path = BRONZE_INPUTS_DIR / f"{table_name}.csv"
        if not csv_path.exists():
            logging.warning(f"  - CSV file for '{table_name}' not found. Skipping load.")
            continue

        try:
            # Read all columns as string to prevent type errors from dirty data
            df = pd.read_csv(csv_path, dtype=str)

            df.to_sql(
                name=table_name,
                con=engine,
                schema='bronze',
                if_exists='replace',  # Use 'replace' for all tables
                index=False
            )

            logging.info(
                f"  - Loaded {len(df)} rows into 'bronze.{table_name}' using 'replace' strategy."
            )
        except Exception as e:
            logging.error(f"  - Failed to load '{table_name}' to bronze schema. Error: {e}")

    logging.info("--- LOAD step completed successfully. ---")


# --- 4. MAIN ORCHESTRATOR ---

def main():
    """Main function to orchestrate the ETL process."""
    logging.info("=" * 50)
    logging.info("=== Starting Bronze Layer Full Refresh Pipeline Run ===")

    try:
        extract_from_gsheets()
        db_engine = get_db_engine()
        load_to_bronze(db_engine)
        logging.info("Pipeline run finished successfully.")

    except Exception as e:
        logging.critical(f"Pipeline run failed. Error: {e}")

    finally:
        logging.info("=" * 50)


if __name__ == "__main__":
    main()
