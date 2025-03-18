import os
from bronze import ingest_data
from silver import transform_bronze_to_silver
from gold import aggregate_silver_to_gold
import logging
import pandas as pd  # Add pandas import if not already present
import sqlite3

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ETL_Pipeline")

def export_table_to_parquet(db_file: str, table_name: str, output_file: str) -> None:
    # Connect to the database and query the specified table
    conn = sqlite3.connect(db_file)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    if df.empty:
        logger.warning(f"Table '{table_name}' in {db_file} is empty. No data to export.")
    else:
        df.to_parquet(output_file, index=False)
        logger.info(f"Exported {len(df)} records from table '{table_name}' to {output_file}")

if __name__ == "__main__":
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    CSV_FILE = os.path.join(BASE_DIR, "data", "sample", "transactions.csv")
    BRONZE_DB = os.path.join(BASE_DIR, "data", "bronze_raw.db")
    SILVER_DB = os.path.join(BASE_DIR, "data", "silver_raw.db")
    GOLD_DB = os.path.join(BASE_DIR, "data", "gold_raw.db")

    logger.info("Starting ETL pipeline...")

    # Bronze Layer
    if not os.path.exists(CSV_FILE):
        logger.error(f"CSV file not found: {CSV_FILE}")
    else:
        if ingest_data(CSV_FILE, BRONZE_DB):
            logger.info("Bronze layer processing completed successfully.")

    # Silver Layer
    if transform_bronze_to_silver(BRONZE_DB, SILVER_DB):
        logger.info("Silver layer processing completed successfully.")

    # Gold Layer
    if aggregate_silver_to_gold(SILVER_DB, GOLD_DB):
        logger.info("Gold layer processing completed successfully.")

    logger.info("ETL pipeline completed.")
    
    # Exporting outputs directly to the data directory
    OUTPUT_DIR = os.path.join(BASE_DIR, "data")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    silver_output = os.path.join(OUTPUT_DIR, "silver_transactions.parquet")
    gold_output = os.path.join(OUTPUT_DIR, "gold_daily_summary.parquet")
    
    export_table_to_parquet(SILVER_DB, "silver_transactions", silver_output)
    export_table_to_parquet(GOLD_DB, "gold_daily_summary", gold_output)
    
    logger.info("All outputs have been exported as Parquet files in the data directory.")
