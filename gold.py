# scripts/gold.py
import duckdb
from deltalake import write_deltalake
import os

# 1. Initialize DuckDB and load the Delta extension
con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

def create_gold_insights():
    print("Starting Gold Layer aggregation...")

    # 2. Define the Business Query
    # Here we aggregate the data to find total balances by Province
    # This is perfect for a dashboard or executive report.
    query = """
    SELECT 
        province,
        segment,
        COUNT(account_id) as total_accounts,
        ROUND(SUM(current_balance), 2) as total_balance,
        ROUND(AVG(risk_score), 2) as avg_risk_score
    FROM delta_scan('data/output/silver/enriched_accounts')
    GROUP BY province, segment
    ORDER BY total_balance DESC
    """

    # 3. Execute and convert to Arrow for memory efficiency
    print("Calculating province and segment insights...")
    gold_table = con.execute(query).arrow()

    # 4. Write the final result to the Gold folder
    output_path = "data/output/gold/province_wealth_report"
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    print(f"Writing Gold table to {output_path}...")
    write_deltalake(output_path, gold_table, mode="overwrite")
    print("Gold Layer processing complete.\n")

if __name__ == "__main__":
    create_gold_insights()