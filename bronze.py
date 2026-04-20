# scripts/bronze.py
from pyspark.sql import SparkSession
import os

# Initialize Spark with Delta Lake support
spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def ingest_to_bronze(source_path, target_path):
    print(f"Reading data from {source_path}...")
    
    # Read CSV manually from the input folder
    df = spark.read.csv(source_path, header=True, inferSchema=True)
    
    # Write to Bronze folder as a Delta table
    # We use 'overwrite' so you can run this multiple times while testing
    print(f"Writing Delta table to {target_path}...")
    df.write.format("delta").mode("overwrite").save(target_path)
    print("Ingestion complete.\n")

if __name__ == "__main__":
    # Manually defined paths based on our Step 1 & 2 setup
    INPUT_ACCOUNTS = "data/input/accounts.csv"
    INPUT_CUSTOMERS = "data/input/customers.csv"
    
    OUTPUT_BRONZE_ACCOUNTS = "data/output/bronze/accounts"
    OUTPUT_BRONZE_CUSTOMERS = "data/output/bronze/customers"

    # Execute ingestion for both files
    ingest_to_bronze(INPUT_ACCOUNTS, OUTPUT_BRONZE_ACCOUNTS)
    ingest_to_bronze(INPUT_CUSTOMERS, OUTPUT_BRONZE_CUSTOMERS)