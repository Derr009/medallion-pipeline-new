import schedule
import time
import subprocess
import logging
from pathlib import Path
import sys

# --- 1. CONFIGURATION & INITIALIZATION ---

# Define project paths
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
SRC_DIR = BASE_DIR / "src"

# Create logs directory
LOG_DIR.mkdir(exist_ok=True)

# Setup basic logging for the scheduler itself
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - SCHEDULER - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "scheduler.log"),
        logging.StreamHandler()
    ]
)

# --- 2. JOB DEFINITION ---

def run_pipeline_job():
    """
    Defines the job to be run, which is executing the main.py script.
    """
    main_script_path = SRC_DIR / "etl.py"
    logging.info(f"Triggering pipeline run for: {main_script_path}")

    try:
        # Use sys.executable to ensure the correct Python interpreter is used
        subprocess.run(
            [sys.executable, str(main_script_path)],
            check=True # This will raise an exception if the script fails
        )
        logging.info("Pipeline run job completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Pipeline run failed with return code {e.returncode}.")
    except FileNotFoundError:
        logging.error(f"MAIN SCRIPT NOT FOUND at {main_script_path}. Please check the path.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

# --- 3. SCHEDULING ---

# CORRECTED: Schedule the job to run every day at 7:00 AM
schedule.every().day.at("07:00").do(run_pipeline_job)

logging.info("Scheduler started. Waiting for the scheduled time to run the pipeline...")
print("Scheduler is running. Press Ctrl+C to exit.")

# --- 4. RUN LOOP ---

while True:
    # Checks whether a scheduled task is pending to run or not
    schedule.run_pending()
    time.sleep(1) # Wait for one second before checking again
