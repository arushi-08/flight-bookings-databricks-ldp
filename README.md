# Flight Bookings Lakehouse Project (Databricks LDP)

This project demonstrates how to build a **Lakehouse architecture** on Databricks using the **Declarative Pipeline (LDP)**.  
It covers **incremental ingestion (Bronze)**, **transformations (Silver)**, and **upserts (Gold)** for a flight bookings dataset.  

---

## 📂 Project Structure
The project follows the **Medallion Architecture**:

- **Raw Layer**: Landing zone for source data in CSV format.  
- **Bronze Layer**: Incremental ingestion from raw → Delta format with schema evolution.  
- **Silver Layer**: Cleaned and transformed data using Lakehouse Declarative Pipeline (LDP).  
- **Gold Layer**: Business-ready data with **upserts** applied dynamically for analytics and reporting.  

---

## ⚡ Bronze Layer (Incremental Ingestion)

Ingest raw CSV data incrementally using **Auto Loader** (`cloudFiles`) into the Bronze Delta tables.  
Auto Loader
- Handles incremental ingestion automatically.
- Stores schema in checkpoint to handle schema drift in production.
- rescue mode allows new unexpected columns to be captured.

The ETL pipeline automatically passes the required source parameter (src) to the Bronze layer (using `SrcParameters.ipynb`), so manual input is not needed.

query example
`SELECT * FROM delta.`/Volumes/workspace/bronze/bronze_volume/customers/data` `

---

🥈 Silver Layer (Transformations with LDP)
The Lakehouse Declarative Pipeline (LDP) is used to transform the Bronze data into a clean Silver layer.
Data cleaning and standardization.
Deduplication and type enforcement.
Stored as Delta tables for further transformations.

---

🥇 Gold Layer (Dynamic Upserts)
The Gold layer contains business-ready curated datasets for reporting & analytics.
Here, we apply upserts to ensure latest values are always available.

Supports Change Data Capture (CDC) with:
Inserts for new records.
Updates for changed records.
Deletes when source marks records as deleted.

---

How to Run
- Clone this repo into Databricks Repos.
- Run the Bronze ingestion notebook
Deploy the Silver transformation pipeline (LDP).
Run the Gold pipeline to see upserted results.


