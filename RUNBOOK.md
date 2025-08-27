# Runbook: Ekart Logistics Data Pipeline

## 1. Overview

This runbook provides the **standard operating procedures (SOP)** for executing and monitoring the end-to-end Ekart logistics data pipeline. The pipeline ingests raw data from Google Sheets, processes it through Bronze, Silver, and Gold layers in a PostgreSQL database, and prepares it for analysis.

This document covers both **manual execution** for testing and development, as well as **automated production runs**.

---

## 2. Prerequisites

Before running the pipeline, ensure the following conditions are met:

* **Environment:** Python virtual environment (`venv`) is activated.
* **Dependencies:** All required Python packages from `requirements.txt` are installed.
* **Database:** PostgreSQL server is running and accessible.
* **Credentials:**

  * `.env` file is present in the project root and correctly configured with database and Google Sheets credentials.
  * `config/credentials.json` service account key file is in place.
* **Source Data:** The source Google Sheet is accessible to the service account's `client_email`.

---

## 3. Manual Execution (Step-by-Step)

For development, testing, or manually triggering a full pipeline refresh. Run all commands from the project root after activating the virtual environment (`source venv/bin/activate`).

### Step 1: Trigger Data Generation

Calls the Google Apps Script Web App to append a new batch of 10,000 dirty rows to the source Google Sheets.

```bash
python src/trigger_data_generation.py
```

### Step 2: Build Bronze Layer

Extracts the full, updated dataset from Google Sheets and performs a full replace of the tables in the bronze schema.

```bash
python src/push_to_bronze.py
```

### Step 3: Build Silver Layer

Executes SQL transformations to clean, validate, and create the silver tables from the bronze data.

```bash
python src/build_silver.py
```

### Step 4: Add Constraints to Silver Layer

Applies formal PRIMARY KEY and FOREIGN KEY constraints to the newly created silver tables.

```bash
python src/add_constraints.py
```

### Step 5: Build Gold Layer

Executes the final SQL scripts to create aggregated and denormalized gold tables for analytics.

```bash
python src/build_gold.py
```

### Step 6: Verify the Run

After all scripts finish, check logs and the database to confirm a successful run.

* **Check Logs:** Open the log files in the `/logs` directory (e.g., `data_generation.log`, `silver_build.log`) for any CRITICAL errors.
* **Check Database:** Connect to your PostgreSQL database and verify that the tables in the gold schema have been created and populated.

---

## 4. Automated Execution

The data generation part of the pipeline is designed to run automatically.

* **Data Generation:** A time-driven trigger is configured in the standalone Google Apps Script project.
  This trigger automatically runs the `generateAllData` function every hour, ensuring a continuous stream of new raw data is available in the Google Sheet.

---

## 5. Monitoring and Logging

* **Individual Script Logs:** Each Python script generates a log file within `/logs` (e.g., `etl_bronze.log`, `silver_build.log`, `gold_build.log`). These logs contain detailed, timestamped entries for each step.
* **Data Generation Log:** The success or failure of the automated Apps Script trigger can be monitored in the **Executions** section of the Apps Script editor online.

---

## 6. Troubleshooting Common Issues

| Error Symptom                                                  | Likely Cause                                                                              | Solution                                                                                                                                                         |
| -------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SSL Certificate Errors in Logs                                 | Corporate firewall or proxy blocking Google's APIs                                        | Python scripts include a built-in (insecure) workaround. For a permanent fix, set `REQUESTS_CA_BUNDLE` environment variable with the company's root certificate. |
| 401 or 404 Errors from Apps Script                             | Permissions issue or incorrect URL in `.env`                                              | Re-deploy the Apps Script as a Web App with "Anyone" access. Verify `APPS_SCRIPT_WEB_APP_URL` in `.env`.                                                         |
| relation "bronze.TableName" does not exist                     | Table names in the database do not match SQL scripts (case-sensitive)                     | Ensure all table names in PostgreSQL are lowercase. Update SQL scripts to use double quotes if needed, e.g., `bronze."Customers"`.                               |
| Pipeline fails at Silver Layer with DependentObjectsStillExist | `add_constraints.py` was run previously, blocking DROP TABLE commands due to foreign keys | Ensure all `DROP TABLE` commands in `silver_*.sql` include `CASCADE`, e.g., `DROP TABLE IF EXISTS silver."Customers" CASCADE;`.                                  |

---
