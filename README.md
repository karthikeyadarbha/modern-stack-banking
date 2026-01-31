# modern-stack-banking

Argus-Lakehouse is a proof-of-concept (PoC) for a Secure, Hybrid Data Lakehouse designed for the banking sector. It solves the "Data Gravity" problem by decoupling compute from storage using Apache Iceberg, allowing seamless interoperability between Databricks (Engineering), Snowflake (Serving), and DuckDB (Local Development).

It features a Unity Catalog-inspired governance model where PII (Credit Card Numbers) is masked at the storage level, ensuring compliance across all compute engines.

🏗️ Architecture
The pipeline follows a Medallion Architecture with a Data Vault modeling technique in the Silver layer.

🛠️ Getting Started (Local / Codespaces)
This project is containerized. You do not need a Databricks cluster to run the development loop.

Prerequisites :
    Docker Desktop (if running locally)
    GitHub Codespaces (Recommended: 4-core machine)
    Snowflake Free Trial (for the Gold layer serving)

Installation
Clone the Repository

Bash
git clone https://github.com/your-username/argus-lakehouse.git
cd argus-lakehouse
Initialize the Environment We use a generic Make command to build the Docker container with Java 17 and Spark 3.5.

Bash
make setup
Run the Pipeline (Simulated) This runs the PySpark job to ingest dummy data and write it to the local data/warehouse folder in Iceberg format.

Bash
python src/jobs/etl_ingest.py
Verify with DuckDB Query the generated Iceberg table instantly.

Bash
python src/debug/check_data.py

argus-lakehouse/
├── .devcontainer/
│   └── devcontainer.json      # The Codespaces config we discussed
├── .github/
│   └── workflows/             # Future CI/CD actions
├── data/                      # Local simulation of S3 buckets
│   ├── warehouse/             # Where Iceberg/Delta tables will live
│   └── landing/               # Where raw JSON/CSV files land
├── dags/                      # Airflow DAGs (Python)
├── src/
│   ├── jobs/                  # PySpark Engineering jobs
│   │   └── etl_ingest.py
│   ├── models/                # Data Vault/SQL logic
│   ├── debug/                 # DuckDB local testing scripts
│   └── utils/                 # Shared logging/config modules
├── requirements.txt           # The file above
├── README.md                  # The documentation we drafted
└── Makefile                   # Shortcuts (e.g., 'make setup')

🧪 Key Experiments
This repository demonstrates three specific architectural patterns:

Experiment A: The Iceberg Bridge

Goal: Prove Snowflake can read Databricks-generated data without COPY INTO.

Code: See src/snowflake/01_create_ext_table.sql.

Experiment B: Local Data Vault

Goal: Test Hub/Satellite hash collisions locally using DuckDB.

Code: See src/models/silver_vault_logic.sql.

Experiment C: Policy Enforcement

Goal: Demonstrate that cc_num is never exposed in clear text in the Silver layer.