import subprocess
import logging
from pathlib import Path
import sys

# --- 1. CONFIGURATION & INITIALIZATION ---

# Defining project paths relative to THIS script's location (the project root)
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
SRC_DIR = BASE_DIR / "src"  # The other scripts are in the src folder

# Creating logs directory
LOG_DIR.mkdir(exist_ok=True)

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "main_pipeline.log"),
        logging.StreamHandler()
    ]
)


# --- 2. ORCHESTRATION LOGIC ---

def run_script(script_name):
    """Runs a given Python script from the /src folder."""
    # Correctly build the full path to the script inside /src
    script_path = SRC_DIR / script_name
    logging.info(f"--- Running script: {script_name} ---")

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info(f"Successfully completed {script_name}.")
        if result.stdout:
            logging.info(f"Output from {script_name}:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"--- SCRIPT FAILED: {script_name} ---")
        logging.error(f"Return Code: {e.returncode}")
        logging.error(f"Output:\n{e.stdout}")
        logging.error(f"Error Output:\n{e.stderr}")
        return False
    except FileNotFoundError:
        logging.error(f"--- SCRIPT NOT FOUND: {script_name} at {script_path} ---")
        return False


def main():
    """Main function to run the entire ETL pipeline in sequence."""
    logging.info("==================================================")
    logging.info("=== Starting Full End-to-End Medallion Pipeline Run ===")

    pipeline_steps = [
        "push_to_bronze.py",          # Step 1: Ingest to Bronze
        "push_to_silver.py",            # Step 2: Clean and build Silver
        "add_constraints.py",         # Step 3: Add constraints to Silver
        "build_gold.py"               # Step 4: Build Gold analytics tables
    ]

    for step in pipeline_steps:
        success = run_script(step)
        if not success:
            logging.critical("Pipeline halted due to a failed step.")
            break

    logging.info("=== Full End-to-End Medallion Pipeline Run Finished ===")
    logging.info("==================================================")


if __name__ == "__main__":
    main()