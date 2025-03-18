import sqlite3
import csv
import os
import logging
from typing import Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BronzeLayer")

def create_bronze_table(cursor):
    """
    Create the bronze_transactions table if it doesn't already exist.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze_transactions (
            transaction_id TEXT PRIMARY KEY,
            customer_id TEXT,
            timestamp TEXT,
            amount REAL,
            transaction_type TEXT,
            merchant TEXT,
            category TEXT,
            status TEXT
        )
    """)

def validate_csv_structure(csv_file: str, required_columns: list) -> bool:
    """
    Validate the structure of the CSV file.

    Args:
        csv_file: Path to the CSV file
        required_columns: List of required column names

    Returns:
        True if the CSV structure is valid, False otherwise
    """
    try:
        with open(csv_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            csv_columns = reader.fieldnames
            if not csv_columns:
                logger.error("CSV file is empty or has no headers.")
                return False
            missing_columns = [col for col in required_columns if col not in csv_columns]
            if missing_columns:
                logger.error(f"CSV file is missing required columns: {missing_columns}")
                return False
        return True
    except Exception as e:
        logger.error(f"Error validating CSV structure: {e}")
        return False

def ingest_data(csv_file: str, db_file: str) -> bool:
    """
    Ingest data from a CSV file into the bronze_transactions table.

    Args:
        csv_file: Path to the CSV file
        db_file: Path to the SQLite database file

    Returns:
        True if ingestion is successful, False otherwise
    """
    required_columns = [
        'transaction_id', 'customer_id', 'timestamp', 'amount',
        'transaction_type', 'merchant', 'category', 'status'
    ]

    if not validate_csv_structure(csv_file, required_columns):
        logger.error("CSV structure validation failed. Aborting ingestion.")
        return False

    try:
        # Ensure the directory for the database exists
        os.makedirs(os.path.dirname(db_file), exist_ok=True)

        # Connect to the SQLite database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Create table if it doesn't exist
        create_bronze_table(cursor)
        conn.commit()

        # Read CSV file and insert data into the table
        record_count = 0
        with open(csv_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    cursor.execute("""
                        INSERT INTO bronze_transactions 
                        (transaction_id, customer_id, timestamp, amount, transaction_type, merchant, category, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row['transaction_id'],
                        row['customer_id'],
                        row['timestamp'],
                        float(row['amount']),
                        row['transaction_type'],
                        row['merchant'].strip() if row['merchant'].strip() else None,
                        row['category'].strip() if row['category'].strip() else None,
                        row['status']
                     ))
                    record_count += 1
                except sqlite3.IntegrityError:
                    logger.warning(f"Record {row['transaction_id']} already exists. Skipping insertion.")
                except KeyError as e:
                    logger.error(f"Missing column in row: {row}. Error: {e}")
                except ValueError as e:
                    logger.error(f"Invalid data format in row: {row}. Error: {e}")

        # Commit changes and close the connection
        conn.commit()
        logger.info(f"Successfully ingested {record_count} records into bronze layer.")
        return True

    except Exception as e:
        logger.error(f"Error during data ingestion: {e}")
        return False

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    CSV_FILE = os.path.join(BASE_DIR, "data", "sample", "transactions.csv")
    DB_FILE = os.path.join(BASE_DIR, "data", "bronze_raw.db")  # Using data folder

    logger.info(f"Ingesting data from: {CSV_FILE}")
    logger.info(f"Saving database to: {DB_FILE}")

    if not os.path.exists(CSV_FILE):
        logger.error(f"CSV file not found: {CSV_FILE}")
    else:
        success = ingest_data(CSV_FILE, DB_FILE)
        if success:
            logger.info("Bronze layer ingestion completed successfully.")
        else:
            logger.error("Bronze layer ingestion failed.")
