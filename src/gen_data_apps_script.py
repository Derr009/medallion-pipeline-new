import os
import logging
import requests
from pathlib import Path
from dotenv import load_dotenv

# --- 1. CONFIGURATION & INITIALIZATION ---
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "data_generation.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()
WEB_APP_URL = os.getenv("APPS_SCRIPT_WEB_APP_URL")


# --- 2. MAIN FUNCTION ---
def trigger_web_app():
    """Triggers the Apps Script by sending a POST request to its Web App URL."""
    logging.info("--- Triggering Google Apps Script Web App ---")

    if not WEB_APP_URL:
        logging.error("APPS_SCRIPT_WEB_APP_URL not found in .env file. Cannot proceed.")
        return

    try:
        # --- SSL VERIFICATION FIX ---
        # WARNING: This disables security checks. Only use on trusted networks.
        # The verify=False parameter tells requests to ignore SSL certificate validation.
        response = requests.post(WEB_APP_URL, timeout=30, verify=False)
        # --- END OF FIX ---

        # Check if the request was successful
        response.raise_for_status()

        # Log the response from the Apps Script
        logging.info(f"Successfully triggered script. Response: {response.json()}")

    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while calling the Web App URL: {e}")


if __name__ == "__main__":
    trigger_web_app()
