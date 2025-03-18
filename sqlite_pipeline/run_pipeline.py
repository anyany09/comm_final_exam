import os
from bronze import ingest_data
from silver import transform_bronze_to_silver
from gold import aggregate_silver_to_gold
import logging
import pandas as pd  # Add pandas import if not already present
import sqlite3
import datetime  # added to generate timestamp
import shutil    # For copying .db files
import boto3     # For S3 operations
from dotenv import load_dotenv  # add this import
load_dotenv()  # load environment variables from .env file

# Read AWS credentials and region from environment variables
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

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

# S3 upload: Upload a local file to the specified bucket and key using the AWS credentials.
def upload_file_to_s3(local_file: str, bucket: str, s3_key: str) -> None:
    s3_client = boto3.client('s3',
                             aws_access_key_id=AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                             region_name=AWS_REGION)
    try:
        s3_client.upload_file(local_file, bucket, s3_key)
        logger.info(f"Uploaded {local_file} to s3://{bucket}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload {local_file} to s3://{bucket}/{s3_key}: {e}")

# S3 download: Download a file from the specified bucket and key to a local file using AWS credentials.
def download_file_from_s3(bucket: str, s3_key: str, local_file: str) -> None:
    s3_client = boto3.client('s3',
                             aws_access_key_id=AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                             region_name=AWS_REGION)  # fixed to use AWS_REGION variable
    try:
        s3_client.download_file(bucket, s3_key, local_file)
        logger.info(f"Downloaded s3://{bucket}/{s3_key} to {local_file}")
    except Exception as e:
        logger.error(f"Failed to download s3://{bucket}/{s3_key} to {local_file}: {e}")

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
    
    # Export outputs to data directory with timestamp (year, month, date, hour, minute, second)
    OUTPUT_DIR = os.path.join(BASE_DIR, "data")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # e.g., "2023-10-06_14-30-45"
    
    bronze_output = os.path.join(OUTPUT_DIR, f"{ts}_bronze_transactions.parquet")
    silver_output = os.path.join(OUTPUT_DIR, f"{ts}_silver_transactions.parquet")
    gold_output   = os.path.join(OUTPUT_DIR, f"{ts}_gold_daily_summary.parquet")
    
    export_table_to_parquet(BRONZE_DB, "bronze_transactions", bronze_output)
    export_table_to_parquet(SILVER_DB, "silver_transactions", silver_output)
    export_table_to_parquet(GOLD_DB, "gold_daily_summary", gold_output)
    
    logger.info("Exported Parquet files with timestamp in the data directory.")
    
    # Also copy .db files to the output directory with timestamp in filename.
    bronze_db_copy = os.path.join(OUTPUT_DIR, f"{ts}_bronze_raw.db")
    silver_db_copy = os.path.join(OUTPUT_DIR, f"{ts}_silver_raw.db")
    gold_db_copy   = os.path.join(OUTPUT_DIR, f"{ts}_gold_raw.db")
    
    shutil.copy2(BRONZE_DB, bronze_db_copy)
    shutil.copy2(SILVER_DB, silver_db_copy)
    shutil.copy2(GOLD_DB, gold_db_copy)
    
    logger.info("Exported .db files with timestamp in the data directory.")

    # S3 bucket names for each layer.
    bucket_bronze = "data-engineering-exam-bronze"
    bucket_silver = "data-engineering-exam-silver"
    bucket_gold   = "data-engineering-exam-gold"

    # Upload Parquet files to S3.
    upload_file_to_s3(bronze_output, bucket_bronze, os.path.basename(bronze_output))
    upload_file_to_s3(silver_output, bucket_silver, os.path.basename(silver_output))
    upload_file_to_s3(gold_output, bucket_gold, os.path.basename(gold_output))

    # NEW: Upload .db files to S3.
    upload_file_to_s3(bronze_db_copy, bucket_bronze, os.path.basename(bronze_db_copy))
    upload_file_to_s3(silver_db_copy, bucket_silver, os.path.basename(silver_db_copy))
    upload_file_to_s3(gold_db_copy, bucket_gold, os.path.basename(gold_db_copy))
    
    logger.info("Exported files (Parquet and .db) have been uploaded to their respective S3 buckets.")

    # Optionally, download example (uncomment below if you wish to test download):
    # download_file_from_s3(bucket_bronze, os.path.basename(bronze_output), os.path.join(OUTPUT_DIR, f"downloaded_{os.path.basename(bronze_output)}"))
    
    logger.info("Exported files have been uploaded to their respective S3 buckets.")
