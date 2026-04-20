# scripts/silver.py
import duckdb
from deltalake import write_deltalake
import os

# 1. Initialize DuckDB and load the Delta extension
con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

def process_silver():
    print("Starting Silver Layer processing...")

    # 2. Define the transformation query
    # We join accounts and customers on the keys we identified
    # We also cast dates and handle the credit_limit column
    query = """
    SELECT 
        acc.account_id,
        acc.account_type,
        acc.account_status,
        acc.product_tier,
        acc.current_balance,
        CAST(acc.open_date AS DATE) as open_date,
        CAST(acc.last_activity_date AS DATE) as last_activity_date,
        CAST(acc.credit_limit AS DOUBLE) as credit_limit,
        cust.customer_id,
        cust.first_name,
        cust.last_name,
        cust.province,
        cust.segment,
        cust.risk_score,
        CAST(cust.dob AS DATE) as dob
    FROM delta_scan('data/output/bronze/accounts') AS acc
    LEFT JOIN delta_scan('data/output/bronze/customers') AS cust
      ON acc.customer_ref = cust.customer_id
    """

    # 3. Execute the query and convert to an Arrow table (memory efficient)
    print("Joining tables and cleaning data...")
    enriched_table = con.execute(query).arrow()

    # 4. Write the result to the Silver folder in Delta format
    output_path = "data/output/silver/enriched_accounts"
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    print(f"Writing Silver table to {output_path}...")
    write_deltalake(output_path, enriched_table, mode="overwrite")
    print("Silver Layer processing complete.\n")

if __name__ == "__main__":
    process_silver()