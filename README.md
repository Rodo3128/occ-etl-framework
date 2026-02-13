# High-Performance ETL Framework: Oracle Cloud to SQL Server

## 🚀 Overview
This project is a robust, production-ready ETL pipeline designed to extract complex e-commerce data from **Oracle Commerce Cloud (OCC)** and perform an intelligent **Upsert** into **Microsoft SQL Server**. 

It handles large-scale transactional data across multiple regions, ensuring data integrity, state-change detection, and automated reporting.

## 🛠 Key Engineering Features
*   **Smart Upsert Logic:** Performs a batch-based comparison to detect changes in order states, reducing database overhead and preventing duplicates (Idempotent design).
*   **Recursive Data Flattening:** Custom algorithm to transform deeply nested JSON objects from OCC API into clean, relational datasets.
*   **Resilient Authentication:** Built-in token lifecycle management with automated refresh logic for long-running extractions.
*   **Data Reliability:** Includes **Apache Parquet** persistence for local backups before SQL loading, ensuring zero data loss during network failures.

## 🧰 Tech Stack
*   **Language:** Python 3.x
*   **Data Handling:** Pandas, NumPy
*   **Database:** SQLAlchemy (MSSQL/pyodbc)
*   **Connectivity:** Requests (REST APIs)
*   **Persistence:** PyArrow (Parquet Format)

## 📋 Prerequisites
*   Environment variables configured (DB_SERVER, API_USER, etc.) via `.env`.
*   SQL Server with ODBC Driver 17.

## ⚙️ How to run
1. Clone the repo.
2. Install dependencies: `pip install -r requirements.txt`.
3. Set your credentials in a `.env` file.
4. Run `python main.py`.