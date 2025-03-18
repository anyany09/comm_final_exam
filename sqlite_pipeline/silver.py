import sqlite3
import pandas as pd
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SilverLayer")

def create_silver_table(cursor):
    """
    Create the silver_transactions table if it doesn't already exist.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_transactions (
            transaction_id TEXT PRIMARY KEY,
            customer_id TEXT,
            transaction_date DATE,
            transaction_time TIME,
            amount REAL,
            transaction_type TEXT,
            merchant TEXT,
            category TEXT,
            status TEXT,
            validation_status TEXT
        )
    """)

def transform_bronze_to_silver(bronze_db: str, silver_db: str) -> bool:
    """
    Transform and validate data from the bronze layer (in bronze_db) into the silver layer stored in silver_db.
    Only new bronze records (not already in silver_transactions) will be processed.
    """
    try:
        # Connect to bronze database and load all records.
        bronze_conn = sqlite3.connect(bronze_db)
        bronze_df = pd.read_sql("SELECT * FROM bronze_transactions", bronze_conn)
        bronze_conn.close()

        if bronze_df.empty:
            logger.info("No records found in bronze layer to transform.")
            return True

        logger.info(f"Read {len(bronze_df)} records from bronze layer for silver transformation.")

        # Connect to silver database and create silver_transactions table if needed.
        silver_conn = sqlite3.connect(silver_db)
        silver_cursor = silver_conn.cursor()
        create_silver_table(silver_cursor)
        silver_conn.commit()

        # Get transaction IDs already in silver.
        try:
            existing_ids_df = pd.read_sql("SELECT transaction_id FROM silver_transactions", silver_conn)
            existing_ids = set(existing_ids_df['transaction_id'].tolist())
        except Exception:
            existing_ids = set()

        # Filter out bronze records already present in silver.
        new_bronze_df = bronze_df if not existing_ids else bronze_df[~bronze_df['transaction_id'].isin(existing_ids)]
        logger.info(f"{len(new_bronze_df)} new records will be transformed into silver layer.")

        if new_bronze_df.empty:
            logger.info("No new records to process for silver layer.")
            return True

        # Transform data: convert timestamp into date and time, and validate records.
        new_bronze_df['transaction_date'] = pd.to_datetime(new_bronze_df['timestamp']).dt.date
        new_bronze_df['transaction_time'] = pd.to_datetime(new_bronze_df['timestamp']).dt.time
        new_bronze_df['validation_status'] = 'VALID'
        new_bronze_df.loc[new_bronze_df['amount'] < 0, 'validation_status'] = 'INVALID: Negative amount'
        new_bronze_df.loc[~new_bronze_df['customer_id'].str.startswith('CUST'), 'validation_status'] = 'INVALID: Invalid customer ID'
        new_bronze_df = new_bronze_df.drop(columns=['timestamp'])

        # Insert transformed data into silver_transactions.
        new_bronze_df.to_sql('silver_transactions', silver_conn, if_exists='append', index=False)
        silver_conn.commit()
        logger.info(f"Successfully transformed {len(new_bronze_df)} new records into silver layer.")
        return True

    except Exception as e:
        logger.error(f"Error during silver layer transformation: {e}")
        return False

    finally:
        try:
            silver_conn.close()
        except:
            pass

if __name__ == "__main__":
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    CSV_FILE = os.path.join(BASE_DIR, "data", "sample", "transactions.csv")
    BRONZE_DB = os.path.join(BASE_DIR, "data", "bronze_raw.db")   # Bronze data source in data folder
    SILVER_DB = os.path.join(BASE_DIR, "data", "silver_raw.db")     # Silver data destination in data folder

    logger.info(f"Ingesting data from: {CSV_FILE} into bronze database: {BRONZE_DB}")
    if not os.path.exists(CSV_FILE):
        logger.error(f"CSV file not found: {CSV_FILE}")
    else:
        from bronze import ingest_data  # Assuming bronze.py exists and defines ingest_data
        if not ingest_data(CSV_FILE, BRONZE_DB):
            logger.error("Bronze layer ingestion failed.")
        else:
            logger.info("Bronze layer ingestion succeeded.")

    if transform_bronze_to_silver(BRONZE_DB, SILVER_DB):
        logger.info("Silver layer transformation completed successfully.")
    else:
        logger.error("Silver layer transformation failed.")
