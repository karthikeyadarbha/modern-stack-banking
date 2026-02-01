# modern-stack-banking

A Hybrid Data Lakehouse designed for the banking sector. It solves the "Data Gravity" problem by decoupling compute from storage using Apache Iceberg, allowing seamless interoperability between Databricks (Engineering), Kaggle Dataset / Snowflake (Serving), and DuckDB (Local Development).

# 🏦 Modern Stack Banking: Governed Fraud Detection
**An End-to-End Data Vault 2.0 & AI-Inference Framework**

This repository demonstrates a production-grade data architecture designed for financial institutions. It bridges the gap between high-performance feature engineering (**Spark/Iceberg**) and regulatory-grade explainability (**dbt/DuckDB/Llama3**).

---

## 🏗️ The Four Pillars of the Architecture

### 1. Ingestion & Feature Engineering (Silver Layer)
* **Technology:** Apache Spark & Apache Iceberg.
* **Process:** Raw transaction data is transformed into anonymized vectors.
* **Outcome:** A versioned, high-performance Silver layer preserving privacy while maintaining statistical signal.

### 2. The Data Vault 2.0 Core (Vault Layer)
* **Technology:** dbt & DuckDB.
* **Process:** Implements a scalable **Hub-and-Satellite** model.
    * **Hubs:** Immutable business keys (Transaction IDs).
    * **Satellites:** Contextual data (PCA features) and AI-generated insights.
* **Outcome:** A fully auditable history fulfilling "Right to Audit" compliance.

### 3. Statistical Integrity (The Eigen-Audit)
* **Technology:** Python, NumPy.
* **Process:** A custom audit suite calculating **Eigenvalues** of PCA features to detect model drift and ensure variance integrity.

### 4. Explainable AI (The Gold Layer)
* **Technology:** Ollama (Llama 3 / Gemma 3).
* **Process:** Maps "black-box" PCA vectors to human-readable forensic reports.
* **Outcome:** Provides plain-language justifications for fraud flags to bank auditors.

---

## 🧬 Deep Dive: Understanding the PCA Vectors

### What is a PCA Vector?
PCA "squashes" hundreds of raw banking dimensions into uncorrelated variables called **Principal Components ($V1...Vn$)**.



### The Math: Eigenvalues & Variance
* **Eigenvalues ($\lambda$):** These represent the **strength** of a signal.
* **The Audit:** Our pipeline ensures the first 5 components capture $>90\%$ of total variance, ensuring the "signal" of fraud isn't lost in the "noise" of normal spending.

### Interpreting the "V" Signals
| Component | Likely Interpretation (In Banking Context) |
| :--- | :--- |
| **V17** | **Geospatial Velocity:** High negative values often indicate "impossible travel" scenarios. |
| **V14** | **Temporal Shift:** Detects unusual spending times or sudden merchant category switches. |
| **V12** | **Profile Match:** Measures how well the transaction fits the historical behavior of the cardholder. |

---

## 🚀 Execution Guide: Step-by-Step

### 1. Environment Initialization
Ensure your Python virtual environment is active and all dependencies are installed.
```bash
source .venv/bin/activate
./.msb-poc/bin/pip install -r requirements.txt

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

Layer	Technology	Purpose	Data State
Bronze	Delta Lake / Spark	Raw Ingestion	Raw JSON/CSV
Silver	Iceberg / Spark	Feature Engineering	Anonymized PCA Vectors
Vault	dbt / DuckDB	Audit & Governance	Hashed Historical State


2. Infrastructure Setup (Ollama)
Run this once to enable the local AI "Brain."

Bash
# Install Ollama
curl -fsSL [https://ollama.com/install.sh](https://ollama.com/install.sh) | sh

# Start the Ollama server (Keep this terminal tab open)
ollama serve

# Pull the required model
ollama pull llama3

3. Build the Data Vault (dbt)
Transform the Iceberg Silver data into your Hubs and Satellites.

Bash
cd Transform
../.msb-poc/bin/dbt run --profiles-dir .

4. Validate Data Quality
Verify the integrity of Business Keys and Fraud Labels.

Bash
../.msb-poc/bin/dbt test --profiles-dir .
cd ..

5. Execute the Statistical Eigen-Audit
Verify that the PCA components are maintaining the expected signal-to-noise ratio.

Bash
# Grant execution permissions
chmod +x scripts/audit_eigen_variance.py

# Run the audit using the project python
./.venv/bin/python scripts/audit_eigen_variance.py --size 1000


6. Generate AI Forensic Reports
Bridge the Vault data with the local LLM to explain the "🚩 FRAUD" flags.

Bash
# Ensure 'ollama serve' is running
./.venv/bin/python scripts/ai_fraud_explainer.py


⚖️ Governance & Compliance
This project follows BCBS 239 principles:

Lineage: Every record includes a record_source.

Accuracy: dbt tests ensure no null labels or duplicate business keys.

Explainability: AI-flagged transactions include a forensic interpretation satellite.

Summary:

Designed and implemented a production-grade Financial Fraud Detection Platform that bridges the gap between high-performance Data Engineering and Regulatory Compliance.
Moving beyond traditional "black box" ML, this architecture establishes a Governed Data Mesh where every AI inference is treated as an auditable data product.

Key Architectural Pillars:

Immutable History (Data Vault 2.0): Leveraged dbt and DuckDB to build a scalable Hub-and-Satellite model, ensuring 100% auditability of transaction history and model versions.

Statistical Rigor (Eigen-Audit): Engineered a custom Python/NumPy pipeline to continuously validate the variance (Eigenvalues) of PCA vectors, preventing model drift before it impacts decision-making.

Explainable AI (XAI): Integrated Llama 3 (via Ollama) directly into the data pipeline. The system automatically converts abstract feature vectors into human-readable forensic narratives, satisfying "Right to Explanation" regulatory requirements (BCBS 239).

Modern Stack Integration: Orchestrated a seamless flow from Apache Iceberg (Silver Layer) to a denormalized Gold Information Mart, demonstrating end-to-end lineage from ingestion to insight.

Tech Stack: dbt DuckDB Apache Iceberg Python Llama 3 Docker/Codespaces

---

## ⚖️ Governance & Operational Excellence

This repository moves beyond experimental AI by implementing a **Dual-Domain Scripting** architecture. 

### 1. Script Taxonomy
| Domain | Folder | Purpose |
| :--- | :--- | :--- |
| **Governance** | `scripts/governance/` | Read-only audits for mathematical integrity and compliance reporting. |
| **AI Ops** | `scripts/ai_ops/` | Write-active pipelines that bridge Data Vault storage with LLM inference. |

### 2. The "Self-Healing" Compliance Loop
We address the **Right to Explanation** (a core tenet of BCBS 239 and GDPR) through an automated loop:
1.  **Detection:** The `governance_dashboard.py` identifies "Explanation Gaps" where fraud is flagged but no forensic narrative exists.
2.  **Remediation:** The `ai_fraud_explainer.py` uses a **Batch-Processor** pattern to target these gaps, generating explanations only for unexplained records.
3.  **Verification:** The dashboard is re-run to confirm 100% coverage, providing an immutable audit trail for regulators.


### 3. Execution Commands (Refactored)
```bash
# Verify Model Math
./.venv/bin/python scripts/governance/audit_eigen_variance.py

# Execute Batch AI Inference (Operationalize)
./.venv/bin/python scripts/ai_fraud_explainer.py

# Generate Final Compliance Report
./.venv/bin/python scripts/governance/governance_dashboard.py


The PCA Metadata & Scoring Process

To protect sensitive information while maintaining detection accuracy, the engineering pipeline calculates Principal Component (PC) Scores. This process starts with Standardization, where raw values like currency amounts are mean-centered and scaled so that large numbers don't unfairly bias the model. These values are then Projected onto a specific Eigenvector—a mathematical "recipe" that defines a new axis of behavior.The resulting PC Score is the specific number stored in your database; it represents the standardized distance of a transaction from the "average" behavior. While the Eigenvalue tells us the total importance of a column (its variance), the PC Score is the actual measurement used to flag outliers. For example, a score of $+5.0$ on a velocity component indicates a transaction is five standard deviations away from the norm, triggering an automatic investigation.