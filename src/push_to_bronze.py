#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import hashlib
import logging
from pathlib import Path

import pandas as pd
import gspread
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from google.oauth2.service_account import Credentials

# --- 1. CONFIGURATION & INITIALIZATION ---

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
BRONZE_INPUTS_DIR = BASE_DIR / "bronze_inputs"
CONFIG_DIR = BASE_DIR / "config"

LOG_DIR.mkdir(exist_ok=True)
BRONZE_INPUTS_DIR.mkdir(exist_ok=True)
CONFIG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "etl_bronze.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")

SHEET_ID = os.getenv("GSPREAD_SHEET_ID")  # prefer sheet ID
CREDENTIALS_PATH = CONFIG_DIR / "capstone-467705-3c3a1f211475.json"

TABLE_NAMES = ["Customers", "Orders", "Shipments", "Drivers", "Vehicles"]

# --- 2. HELPER FUNCTIONS ---


def get_db_engine():
    """Create SQLAlchemy engine for PostgreSQL."""
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        with engine.connect():
            logging.info("Connected to PostgreSQL database.")
        return engine
    except OperationalError as e:
        logging.error(f"DB connection failed: {e}")
        raise


def create_bronze_schema(engine):
    """Ensure bronze schema exists."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        logging.info("Schema 'bronze' exists or created.")


def calculate_checksum(file_path: Path) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(4096), b""):
            sha256.update(block)
    return sha256.hexdigest()


def build_gspread_client():
    """Build a gspread client with service account credentials."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]
    creds = Credentials.from_service_account_file(
        str(CREDENTIALS_PATH),
        scopes=scopes
    )

    # Standard secure gspread client
    gc = gspread.authorize(creds)
    return gc


def open_spreadsheet(gc):
    """Open spreadsheet by ID """
    if not SHEET_ID:
        raise ValueError("Set GSPREAD_SHEET_ID in .env")
    logging.info(f"Opening Google Sheet by ID.")
    return gc.open_by_key(SHEET_ID)


# --- 3. ETL CORE FUNCTIONS ---


def extract_from_gsheets():
    """Extract all tables from Google Sheets to CSVs."""
    logging.info("--- Starting EXTRACT step ---")
    try:
        gc = build_gspread_client()
        spreadsheet = open_spreadsheet(gc)

        for table_name in TABLE_NAMES:
            try:
                ws = spreadsheet.worksheet(table_name)
                df = pd.DataFrame(ws.get_all_records())

                output_path = BRONZE_INPUTS_DIR / f"{table_name}.csv"
                df.to_csv(output_path, index=False)
                checksum = calculate_checksum(output_path)

                logging.info(
                    f"Extracted '{table_name}' → CSV, rows: {len(df)}, checksum: {checksum[:8]}..."
                )
            except gspread.WorksheetNotFound:
                logging.error(f"Worksheet '{table_name}' not found.")
            except Exception as e:
                logging.error(f"Failed to extract '{table_name}': {e}")

        logging.info("--- EXTRACT completed ---")
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise


def load_to_bronze(engine):
    """Load CSVs into bronze schema using replace strategy."""
    logging.info("--- Starting LOAD step ---")
    create_bronze_schema(engine)

    for table_name in TABLE_NAMES:
        csv_path = BRONZE_INPUTS_DIR / f"{table_name}.csv"
        if not csv_path.exists():
            logging.warning(f"CSV for '{table_name}' not found. Skipping.")
            continue

        try:
            df = pd.read_csv(csv_path, dtype=str)
            df.to_sql(
                name=table_name,
                con=engine,
                schema="bronze",
                if_exists="replace",
                index=False
            )
            logging.info(f"Loaded {len(df)} rows into bronze.{table_name}")
        except Exception as e:
            logging.error(f"Failed to load '{table_name}': {e}")

    logging.info("--- LOAD completed ---")


# --- 4. MAIN ORCHESTRATOR ---


def main():
    logging.info("=" * 50)
    logging.info("=== Starting Bronze Layer Full Refresh Pipeline Run ===")
    try:
        extract_from_gsheets()
        engine = get_db_engine()
        load_to_bronze(engine)
        logging.info("Pipeline finished successfully.")
    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")
    finally:
        logging.info("=" * 50)


if __name__ == "__main__":
    main()
