South African Banking Data Pipeline: Medallion Architecture
This repository implements a robust Data Engineering pipeline using a Medallion Architecture (Bronze, Silver, and Gold layers). It is specifically optimized to process 100,000+ records of banking data (Accounts and Customers) within a high-performance framework.

🚀 Architecture & Technical Stack
The pipeline follows a hybrid approach to balance data reliability and processing speed:

Bronze (Raw): Ingests raw .csv data using PySpark to ensure schema consistency and saves it into the Delta Lake format.

Silver (Enriched): Uses DuckDB for high-speed SQL joins and data cleaning (date casting and handling null credit limits). This avoids the JVM overhead and stays well within memory limits.

Gold (Curated): Aggregates data into business-ready insights, providing a geographical view of customer wealth and risk profiles across South African provinces.

🛠️ Data Strategy
Memory Management: Optimized for a 2-core / 2GB RAM constraint using DuckDB’s vectorized execution.

Key Mapping: Successfully maps accounts.customer_ref to customers.customer_id to join disparate datasets.

Data Quality: Cleanses numerical fields and standardizes date formats for downstream time-series analysis.

📁 Repository Structure
Plaintext
├── config/             # Pipeline configuration (YAML)
├── data/
│   ├── input/          # Source CSV files (Accounts, Customers)
│   └── output/         # Processed Delta tables (Bronze, Silver, Gold)
├── scripts/
│   ├── bronze.py       # Raw data ingestion
│   ├── silver.py       # Data cleaning & joining
│   └── gold.py         # Business logic & aggregation
├── requirements.txt    # Project dependencies
└── README.md           # Project documentation
⚙️ How to Run
Install Dependencies:

Bash
pip install -r requirements.txt
Execute the Pipeline:
Run the layers in sequence:

Bash
python scripts/bronze.py
python scripts/silver.py
python scripts/gold.py
📈 Business Insights
The Gold Layer generates a province_wealth_report which provides:

Total account balance per Province.

Customer count by market segment (e.g., Emerging, Professional, Middle Market).

Average risk scores to identify regional portfolio health.

Author: Thapelo Mofokeng

Role: Mechanical Engineer & Data Engineering Implementer
