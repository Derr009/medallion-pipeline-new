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
        logging.FileHandler(LOG_DIR / "add_constraints.log"),
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


def apply_constraints(engine):
    """Executes the SQL script to add constraints."""
    constraints_file = SQL_DIR / "silver_add_constraints.sql"
    logging.info(f"Applying constraints from {constraints_file}...")

    try:
        with open(constraints_file, 'r') as file:
            sql_script = file.read()

        with engine.connect() as connection:
            # We split the script into individual statements to run them one by one
            for statement in sql_script.split(';'):
                if statement.strip():  # Ensure we don't run empty statements
                    connection.execute(text(statement))
            connection.commit()
        logging.info("Successfully applied all constraints.")
    except Exception as e:
        logging.error(f"Failed to apply constraints. Error: {e}")
        raise


def main():
    """Main function to orchestrate the process."""
    logging.info("=" * 50)
    logging.info("=== Starting Add Constraints Process ===")
    try:
        db_engine = get_db_engine()
        apply_constraints(db_engine)
    except Exception as e:
        logging.critical(f"Process failed. Error: {e}")
    finally:
        logging.info("=" * 50)


if __name__ == "__main__":
    main()