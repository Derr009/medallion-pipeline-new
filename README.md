# End-to-End Logistics Data Pipeline (Ekart)

## 1. Project Overview

This project implements a complete, end-to-end data pipeline for **Ekart**, a logistics and fulfillment company.
It follows the **Medallion Architecture** to ingest, process, and analyze shipment data, transforming raw, messy source data into clean, analytics-ready tables that power a business intelligence dashboard.

The pipeline simulates a real-world scenario, starting with raw operational data generated in **Google Sheets**, processing it through **Bronze, Silver, and Gold layers** in a **PostgreSQL** database, and culminating in a **Metabase dashboard** for business analysis.

---

## 2. Medallion Architecture

The pipeline is structured into three distinct layers:

### Bronze Layer (Raw Ingestion)

* Direct, unaltered copy of source data.
* Acts as a historical archive, capturing raw data "as-is," including inconsistencies and errors.

### Silver Layer (Cleaned & Conformed)

* Single source of truth.
* Data from Bronze layer is cleaned, standardized, de-duplicated, and validated.
* Contains master tables with enforced data types and logical integrity.

### Gold Layer (Business Aggregates)

* Analytics-ready layer.
* Contains pre-aggregated and denormalized tables optimized for fast querying by BI tools.

### Data Flow Diagram

```
[Google Sheets] --> [Python ETL] --> [Bronze Layer (PostgreSQL)]
                     --> [SQL Transforms] --> [Silver Layer (PostgreSQL)]
                     --> [SQL Aggregates] --> [Gold Layer (PostgreSQL)]
                     --> [Metabase Dashboard]
```

---

## 3. Tech Stack

* **Data Source:** Google Sheets & Google Apps Script
* **Programming Language:** Python 3.10+
* **Database:** PostgreSQL
* **ETL & Orchestration:** Python (Pandas, SQLAlchemy, Psycopg2)
* **BI & Dashboarding:** Metabase
* **Automation:** Google Apps Script Triggers, Cron (for local scheduling)
* **Version Control:** Git & GitHub

---

## 4. Project Structure

```
/
├── bronze_inputs/      # Temporary staging for raw CSV files during extraction
├── config/             # Configuration files, including Google Cloud credentials
├── docs/               # Project documentation (Data Dictionary, Lineage, etc.)
├── logs/               # Log files for all pipeline runs
├── sql/                # SQL scripts for Silver and Gold layer transformations
├── src/                # Python source code for the ETL pipeline
├── .env                # Environment variables for credentials (DO NOT COMMIT)
└── README.md           # Project README
```

---

## 5. Setup and Installation

### Prerequisites

* Python 3.10 or higher
* PostgreSQL installed and running
* Docker Desktop (for running Metabase)

### Step 1: Clone the Repository

```bash
git clone https://github.com/Derr009/medallion-pipeline-new
cd medallion-pipeline-new.ipynb
```

### Step 2: Set Up Python Environment

```bash
# Create a virtual environment
python3 -m venv venv

# Activate the environment
source venv/bin/activate

# Install required libraries
pip install -r requirements.txt
```

### Step 3: Configure PostgreSQL

* Create a new database in PostgreSQL (e.g., `ekart_pipeline`).
* Ensure you have a user with privileges to create schemas and tables.

### Step 4: Configure Environment Variables

Create a file named `.env` in the project root and add:

```env
# PostgreSQL Credentials
POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_db_password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ekart_pipeline

# Google Sheets & Apps Script
GSPREAD_SHEET_NAME="Your Google Sheet Name"
APPS_SCRIPT_WEB_APP_URL="Your_Apps_Script_Web_App_URL"
```

### Step 5: Google Cloud & Apps Script Setup

* Create a **Google Cloud Service Account** and download `credentials.json` to `/config`.
* Enable the following APIs in Google Cloud:

  * Google Drive API
  * Google Sheets API
  * Google Apps Script API
* Share your source Google Sheet with the `client_email` from `credentials.json`.
* Set up a standalone **Google Apps Script** for data generation and deploy as a Web App with "Anyone" access.

---

## 6. How to Run the Pipeline

The pipeline is orchestrated by `main.py`.
Run the full process (generate data, ingest Bronze, build Silver, add constraints, build Gold) using:

```bash
python src/main.py
```

---

## 7. Dashboard Access

The business intelligence dashboard is built with **Metabase**.

### Start Metabase using Docker

```bash
docker run -d -p 3000:3000 --name metabase metabase/metabase
```

### Access in Browser

Open: [http://localhost:3000](http://localhost:3000)

### Connect to Database

* Use host: `host.docker.internal`
* Provide PostgreSQL credentials
* Build charts and dashboards by querying tables in the **Gold** schema.

---

## 8. Notes

* Logs are stored in `/logs`.
* Raw inputs are in `/bronze_inputs/`.
* Ensure `.env` is **never committed**.
