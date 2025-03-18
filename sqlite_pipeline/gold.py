import sqlite3
import pandas as pd
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("GoldLayer")

def create_gold_table(cursor):
    """
    Create the gold_daily_summary table if it doesn't already exist.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_daily_summary (
            summary_date DATE,
            transaction_type TEXT,
            category TEXT,
            transaction_count INTEGER,
            total_amount REAL,
            avg_amount REAL,
            PRIMARY KEY (summary_date, transaction_type, category)
        )
    """)

def ensure_database_exists(db_file: str) -> None:
    """
    Ensure the database file and its directory exist.

    Args:
        db_file: Path to the SQLite database file
    """
    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    if not os.path.exists(db_file):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        create_gold_table(cursor)
        conn.commit()
        conn.close()
        logger.info(f"Database created at: {db_file}")

def aggregate_silver_to_gold(silver_db: str, gold_db: str) -> bool:
    """
    Aggregate data from the silver layer (in silver_db) into the gold layer stored in gold_db.
    """
    gold_conn = None
    try:
        # Connect to gold database and create gold_daily_summary table if needed.
        gold_conn = sqlite3.connect(gold_db)
        cursor = gold_conn.cursor()
        create_gold_table(cursor)
        gold_conn.commit()
        
        # Attach the silver database as 'silver_db'
        gold_conn.execute(f"ATTACH DATABASE '{silver_db}' AS silver_db")
        
        query = """
        SELECT 
            transaction_date AS summary_date,
            transaction_type,
            category,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_amount
        FROM silver_db.silver_transactions
        WHERE validation_status = 'VALID'
        GROUP BY transaction_date, transaction_type, category
        """
        df = pd.read_sql(query, gold_conn)
        if df.empty:
            logger.info("No new data to aggregate for gold layer.")
            return True
        
        # Insert aggregated data into gold_daily_summary table in gold_db.
        df.to_sql('gold_daily_summary', gold_conn, if_exists='append', index=False)
        gold_conn.commit()
        logger.info(f"Successfully aggregated {len(df)} records into gold layer.")
        return True

    except Exception as e:
        logger.error(f"Error during gold layer aggregation: {e}")
        return False

    finally:
        if gold_conn:
            gold_conn.close()

if __name__ == "__main__":
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    # Use data folder for gold database
    GOLD_DB = os.path.join(BASE_DIR, "data", "gold_raw.db")
    # Also specify silver database location
    SILVER_DB = os.path.join(BASE_DIR, "data", "silver_raw.db")

    logger.info(f"Aggregating data into gold database: {GOLD_DB}")

    ensure_database_exists(GOLD_DB)
    if aggregate_silver_to_gold(SILVER_DB, GOLD_DB):
        logger.info("Gold layer aggregation completed successfully.")
    else:
        logger.error("Gold layer aggregation failed.")
