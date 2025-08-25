import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# --- 1. CONFIGURATION & INITIALIZATION ---

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
SQL_DIR = BASE_DIR / "sql"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "silver_build.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")


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


# --- 3. SILVER LAYER CORE FUNCTIONS (MODIFIED) ---

def execute_sql_from_file(engine, filepath, table_name_lower):
    """Executes a SQL script and logs row counts for DQ checks."""

    # --- MODIFICATION: Capitalize the table name for queries ---
    table_name_cased = table_name_lower.capitalize()
    logging.info(f"  - Building silver table: {table_name_cased}...")
    # --- END MODIFICATION ---

    try:
        with open(filepath, 'r') as file:
            sql_script = file.read()

        with engine.connect() as connection:
            # --- MODIFICATION: Use quoted, case-sensitive table names ---
            bronze_count_query = f'SELECT COUNT(*) FROM bronze."{table_name_cased}";'
            bronze_count = connection.execute(text(bronze_count_query)).scalar()

            connection.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
            connection.execute(text(sql_script))
            connection.commit()

            silver_count_query = f'SELECT COUNT(*) FROM silver."{table_name_cased}";'
            silver_count = connection.execute(text(silver_count_query)).scalar()
            # --- END MODIFICATION ---

            rows_rejected = bronze_count - silver_count
            logging.info(
                f"    - DQ Check for {table_name_cased}: "
                f"Bronze rows: {bronze_count}, Silver rows: {silver_count}, "
                f"Rejected rows: {rows_rejected}"
            )

    except Exception as e:
        logging.error(f"    - Failed to execute {filepath}. Error: {e}")
        raise


def build_silver_layer(engine):
    """Executes all SQL scripts in the /sql directory to build the Silver layer."""
    logging.info("--- Starting build of SILVER Layer ---")

    sql_execution_order = [
        "silver_drivers.sql",
        "silver_vehicles.sql",
        "silver_customers.sql",
        "silver_orders.sql",
        "silver_shipments.sql"
    ]

    for filename in sql_execution_order:
        filepath = SQL_DIR / filename
        table_name_lower = filename.split('.')[0].replace('silver_', '')
        if filepath.exists():
            execute_sql_from_file(engine, filepath, table_name_lower)
        else:
            logging.error(f"  - SQL file not found: {filepath}")

    logging.info("--- SILVER Layer build completed. ---")


# --- 4. MAIN ORCHESTRATOR ---

def main():
    """Main function to orchestrate the Silver layer build."""
    logging.info("=" * 50)
    logging.info("=== Starting Silver Layer Build Process ===")

    try:
        db_engine = get_db_engine()
        build_silver_layer(db_engine)
        logging.info("Silver layer build finished successfully.")

    except Exception as e:
        logging.critical(f"Silver layer build failed. Error: {e}")

    finally:
        logging.info("=" * 50)


if __name__ == "__main__":
    main()
