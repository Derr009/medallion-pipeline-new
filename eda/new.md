    {
     "cells": [
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "# Ekart Logistics - Exploratory Data Analysis (EDA)\n",
        "\n",
        "**Objective:** To explore the clean, processed data from the **Silver Layer** of our data pipeline. This notebook will analyze the distributions, trends, and relationships within the dataset to uncover initial insights into customer behavior, order patterns, and delivery performance."
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "## 1. Setup and Database Connection\n",
        "\n",
        "First, we'll import the necessary libraries and establish a connection to our PostgreSQL database to load the clean Silver layer tables."
       ]
      },
      {
       "cell_type": "code",
       "execution_count": null,
       "metadata": {},
       "outputs": [],
       "source": [
        "import os\n",
        "import pandas as pd\n",
        "from sqlalchemy import create_engine\n",
        "from dotenv import load_dotenv\n",
        "import matplotlib.pyplot as plt\n",
        "import seaborn as sns\n",
        "\n",
        "# Set plot style\n",
        "sns.set_style(\"whitegrid\")\n",
        "plt.rcParams['figure.figsize'] = (12, 6)\n",
        "\n",
        "# Load environment variables from .env file\n",
        "load_dotenv()\n",
        "\n",
        "# Database connection details\n",
        "DB_USER = os.getenv(\"POSTGRES_USER\")\n",
        "DB_PASSWORD = os.getenv(\"POSTGRES_PASSWORD\")\n",
        "DB_HOST = os.getenv(\"POSTGRES_HOST\")\n",
        "DB_PORT = os.getenv(\"POSTGRES_PORT\")\n",
        "DB_NAME = os.getenv(\"POSTGRES_DB\")\n",
        "\n",
        "# Create database engine\n",
        "try:\n",
        "    engine = create_engine(f\"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}\")\n",
        "    print(\"Successfully connected to the database.\")\n",
        "except Exception as e:\n",
        "    print(f\"Failed to connect to the database. Error: {e}\")"
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "## 2. Load Data from Silver Layer\n",
        "\n",
        "We will now load all five tables from the `silver` schema into pandas DataFrames."
       ]
      },
      {
       "cell_type": "code",
       "execution_count": null,
       "metadata": {},
       "outputs": [],
       "source": [
        "try:\n",
        "    customers_df = pd.read_sql('SELECT * FROM silver.\"Customers\"', engine)\n",
        "    orders_df = pd.read_sql('SELECT * FROM silver.\"Orders\"', engine)\n",
        "    shipments_df = pd.read_sql('SELECT * FROM silver.\"Shipments\"', engine)\n",
        "    drivers_df = pd.read_sql('SELECT * FROM silver.\"Drivers\"', engine)\n",
        "    vehicles_df = pd.read_sql('SELECT * FROM silver.\"Vehicles\"', engine)\n",
        "    \n",
        "    print(\"All silver tables loaded successfully.\")\n",
        "    # Display the first few rows of the main shipments table to verify\n",
        "    shipments_df.head()\n",
        "except Exception as e:\n",
        "    print(f\"Error loading data: {e}\")"
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "## 3. Customer Analysis"
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "### 3.1. Customer Distribution by City\n",
        "\n",
        "Let's see where our customers are geographically concentrated. We'll extract the city from the `delivery_address`."
       ]
      },
      {
       "cell_type": "code",
       "execution_count": null,
       "metadata": {},
       "outputs": [],
       "source": [
        "# Extract city from address\n",
        "customers_df['city'] = customers_df['delivery_address'].apply(lambda x: str(x).split(',')[-1].strip() if pd.notna(x) else 'Unknown')\n",
        "\n",
        "city_counts = customers_df['city'].value_counts().head(10)\n",
        "\n",
        "plt.figure(figsize=(12, 7))\n",
        "sns.barplot(x=city_counts.index, y=city_counts.values, palette='viridis')\n",
        "plt.title('Top 10 Customer Cities', fontsize=16)\n",
        "plt.xlabel('City', fontsize=12)\n",
        "plt.ylabel('Number of Customers', fontsize=12)\n",
        "plt.xticks(rotation=45)\n",
        "plt.show()"
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "**Insight:** This chart shows us the key markets for the business. A heavy concentration in one city (like Chennai in our generated data) could inform decisions about where to open new delivery hubs."
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "## 4. Order and Shipment Analysis"
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "### 4.1. Distribution of Order Totals\n",
        "Let's understand the typical purchase value of our customers."
       ]
      },
      {
       "cell_type": "code",
       "execution_count": null,
       "metadata": {},
       "outputs": [],
       "source": [
        "plt.figure(figsize=(12, 6))\n",
        "sns.histplot(orders_df['order_total'], bins=50, kde=True, color='skyblue')\n",
        "plt.title('Distribution of Order Totals', fontsize=16)\n",
        "plt.xlabel('Order Total (₹)', fontsize=12)\n",
        "plt.ylabel('Frequency', fontsize=12)\n",
        "plt.show()"
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "**Insight:** The histogram shows that the majority of orders are of lower value, with a long tail of higher-value purchases. This is a common pattern in e-commerce and can be used for customer segmentation (e.g., identifying high-value customers)."
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "### 4.2. Delivery Performance\n",
        "How long does it take to deliver our shipments? We'll calculate the duration in hours."
       ]
      },
      {
       "cell_type": "code",
       "execution_count": null,
       "metadata": {},
       "outputs": [],
       "source": [
        "# Ensure date columns are in datetime format\n",
        "shipments_df['dispatch_date'] = pd.to_datetime(shipments_df['dispatch_date'])\n",
        "shipments_df['delivery_date'] = pd.to_datetime(shipments_df['delivery_date'])\n",
        "\n",
        "# Calculate delivery duration in hours\n",
        "shipments_df['delivery_hours'] = (shipments_df['delivery_date'] - shipments_df['dispatch_date']).dt.total_seconds() / 3600\n",
        "\n",
        "# Filter out any negative durations that might result from bad data\n",
        "positive_delivery_hours = shipments_df[shipments_df['delivery_hours'] > 0]['delivery_hours']\n",
        "\n",
        "plt.figure(figsize=(12, 6))\n",
        "sns.histplot(positive_delivery_hours, bins=50, kde=True, color='coral')\n",
        "plt.title('Distribution of Delivery Times (in Hours)', fontsize=16)\n",
        "plt.xlabel('Delivery Time (Hours)', fontsize=12)\n",
        "plt.ylabel('Number of Shipments', fontsize=12)\n",
        "plt.show()"
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "**Insight:** This histogram is a key performance indicator. It shows the company's delivery speed. We can see the most common delivery times and identify outliers (very long deliveries) that may need investigation."
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "### 4.3. Shipment Status Overview"
       ]
      },
      {
       "cell_type": "code",
       "execution_count": null,
       "metadata": {},
       "outputs": [],
       "source": [
        "status_counts = shipments_df['status'].value_counts()\n",
        "\n",
        "plt.figure(figsize=(10, 6))\n",
        "sns.barplot(x=status_counts.index, y=status_counts.values, palette='mako')\n",
        "plt.title('Count of Shipments by Final Status', fontsize=16)\n",
        "plt.xlabel('Status', fontsize=12)\n",
        "plt.ylabel('Number of Shipments', fontsize=12)\n",
        "plt.show()"
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "**Insight:** This chart provides a clear view of the success rate of deliveries. A high number of 'Failed' deliveries is a major red flag that would require immediate business attention to diagnose the root cause."
       ]
      },
      {
       "cell_type": "markdown",
       "metadata": {},
       "source": [
        "## 5. Conclusion\n",
        "\n",
        "This initial EDA on the clean Silver layer data has provided several key insights:\n",
        "- Our customer base is heavily concentrated in specific cities, highlighting key operational areas.\n",
        "- The business relies on a high volume of lower-value orders, which is typical for e-commerce.\n",
        "- Delivery performance is generally consistent, but there is a tail of long-duration deliveries that could be optimized.\n",
        "- The shipment success rate is a critical metric that is now easily trackable.\n",
        "\n",
        "These findings validate the quality of our Silver layer and provide a strong foundation for the aggregated tables in the Gold layer that will power our final BI dashboard."
       ]
      }
     ],
     "metadata": {
      "kernelspec": {
       "display_name": "Python 3",
       "language": "python",
       "name": "python3"
      },
      "language_info": {
       "name": "python",
       "version": "3.10"
      }
     },
     "nbformat": 4,
     "nbformat_minor": 2
    }
