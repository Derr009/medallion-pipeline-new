import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# --- Configuration ---
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
SQL_DIR = BASE_DIR / "sql"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "gold_build.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")


def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
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


def execute_gold_script(engine, filepath):
    """Executes a single SQL script to build a gold table."""
    table_name = filepath.stem
    logging.info(f"  - Building gold table: {table_name}...")
    try:
        with open(filepath, 'r') as file:
            sql_script = file.read()

        with engine.connect() as connection:
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
            connection.execute(text(sql_script))
            # --- FIX: The line below was removed as it's handled automatically ---
            # connection.commit()
        logging.info(f"    - Successfully built {table_name}.")
    except Exception as e:
        logging.error(f"    - Failed to execute {filepath}. Error: {e}")
        raise


def build_gold_layer(engine):
    """Executes all SQL scripts to build the Gold layer."""
    logging.info("--- Starting build of GOLD Layer ---")

    gold_scripts = [
        "gold_monthly_driver_performance.sql",
        "gold_vehicle_utilization_summary.sql",
        "gold_full_shipment_details.sql"
    ]

    for filename in gold_scripts:
        filepath = SQL_DIR / filename
        if filepath.exists():
            execute_gold_script(engine, filepath)
        else:
            logging.error(f"  - SQL file not found: {filepath}")

    logging.info("--- GOLD Layer build completed. ---")


def main():
    """Main function to orchestrate the Gold layer build."""
    logging.info("=" * 50)
    logging.info("=== Starting Gold Layer Build Process ===")
    try:
        db_engine = get_db_engine()
        build_gold_layer(db_engine)
        logging.info("Gold layer build finished successfully.")
    except Exception as e:
        logging.critical(f"Gold layer build failed. Error: {e}")
    finally:
        logging.info("=" * 50)


if __name__ == "__main__":
    main()
