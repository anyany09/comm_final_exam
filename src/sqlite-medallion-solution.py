#!/usr/bin/env python3
"""
SQLite3 Medallion Architecture Implementation

This module implements a complete ETL pipeline following the medallion architecture pattern,
with bronze (raw), silver (validated), and gold (aggregated) data layers.

The pipeline processes transaction data through these layers, applying appropriate
transformations at each stage and maintaining data lineage.
"""

import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime
import argparse
from typing import Dict, Optional, Tuple, List, Any
from utils.logger import setup_logger


# Set up logging
logger = setup_logger("SQLite_Pipeline", log_file="sqlite_pipeline.log")


# Define constants
DEFAULT_DB_PATH = "database/medallion.db"
DEFAULT_EXPORT_DIR = "data/s3_upload"

class MedallionPipeline:
    """Implements a medallion architecture ETL pipeline using SQLite3."""
    
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        """
        Initialize the pipeline with database path.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self._ensure_db_directory()
    
    def _ensure_db_directory(self) -> None:
        """Ensure the database directory exists."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def connect(self) -> sqlite3.Connection:
        """
        Create a connection to the SQLite database.
        
        Returns:
            SQLite connection object
        """
        return sqlite3.connect(self.db_path)
    
    def create_tables(self) -> bool:
        """
        Create bronze, silver, and gold tables in SQLite database.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Bronze layer - Raw data as ingested
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS bronze_transactions (
                transaction_id TEXT PRIMARY KEY,
                customer_id TEXT,
                timestamp TEXT,
                amount REAL,
                transaction_type TEXT,
                merchant TEXT,
                category TEXT,
                status TEXT,
                ingestion_timestamp TEXT,
                source_file TEXT
            )
            ''')
            
            # Silver layer - Cleaned and validated data
            cursor.execute('''
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
                processing_timestamp TEXT,
                bronze_ingestion_timestamp TEXT,
                validation_status TEXT
            )
            ''')
            
            # Gold layer - Aggregated business-ready data
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS gold_daily_summary (
                summary_date DATE,
                transaction_type TEXT,
                category TEXT,
                transaction_count INTEGER,
                total_amount REAL,
                avg_amount REAL,
                min_amount REAL,
                max_amount REAL,
                processing_timestamp TEXT,
                PRIMARY KEY (summary_date, transaction_type, category)
            )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Database tables created successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error creating database tables: {e}")
            return False
    
    def process_bronze_layer(self, file_path: str) -> bool:
        """
        Load raw data from PARQUET into bronze layer.
        
        Args:
            file_path: Path to the PARQUET file containing transaction data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.connect()
            
            # Read PARQUET file
            logger.info(f"Reading PARQUET file: {file_path}")
            df = pd.read_parquet(file_path)
            
            # Add ingestion metadata
            df['ingestion_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['source_file'] = os.path.basename(file_path)
            
            # Check for duplicates in transaction_id
            duplicate_count = df['transaction_id'].duplicated().sum()
            if duplicate_count > 0:
                logger.warning(f"Found {duplicate_count} duplicate transaction IDs in input file")
                df = df.drop_duplicates(subset=['transaction_id'], keep='first')
                logger.info(f"Removed duplicates, {len(df)} records remaining")
            
            # Check for existing records
            existing_ids = set()
            cursor = conn.cursor()
            cursor.execute("SELECT transaction_id FROM bronze_transactions")
            for row in cursor.fetchall():
                existing_ids.add(row[0])
            
            # Filter out existing records
            new_records = df[~df['transaction_id'].isin(existing_ids)]
            
            if len(new_records) == 0:
                logger.info("No new records to add to bronze layer")
                conn.close()
                return True
            
            # Insert data into bronze layer
            logger.info(f"Inserting {len(new_records)} new records into bronze layer")
            new_records.to_sql('bronze_transactions', conn, if_exists='append', index=False)
            
            conn.commit()
            conn.close()
            logger.info(f"Successfully loaded {len(new_records)} records into bronze layer")
            return True
        
        except Exception as e:
            logger.error(f"Error loading data into bronze layer: {e}")
            return False
    
    def process_silver_layer(self) -> bool:
        """
        Transform bronze data into silver layer.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.connect()
            
            # Get records from bronze that aren't in silver yet
            query = '''
            SELECT b.* 
            FROM bronze_transactions b
            LEFT JOIN silver_transactions s ON b.transaction_id = s.transaction_id
            WHERE s.transaction_id IS NULL
            '''
            
            df = pd.read_sql(query, conn)
            
            if len(df) == 0:
                logger.info("No new records to process for silver layer")
                conn.close()
                return True
            
            logger.info(f"Processing {len(df)} new records for silver layer")
            
            # Transform data
            silver_df = df.copy()
            
            # Validate and transform data
            validation_statuses = []
            for idx, row in silver_df.iterrows():
                validation_messages = []
                
                # Split timestamp into date and time
                try:
                    dt = pd.to_datetime(row['timestamp'])
                    silver_df.at[idx, 'transaction_date'] = dt.date()
                    silver_df.at[idx, 'transaction_time'] = dt.time()
                except Exception as e:
                    validation_messages.append(f"Invalid timestamp: {e}")
                
                # Validate amount for refunds
                if row['transaction_type'] == 'refund' and row['amount'] >= 0:
                    validation_messages.append("Refund with non-negative amount")
                    # Corrective action: make amount negative
                    silver_df.at[idx, 'amount'] = -abs(row['amount'])
                
                # Validate customer_id format
                if not str(row['customer_id']).startswith('CUST'):
                    validation_messages.append("Invalid customer ID format")
                
                # Status validation
                valid_statuses = ['completed', 'pending', 'failed', 'reversed']
                if row['status'].lower() not in valid_statuses:
                    validation_messages.append(f"Invalid status: {row['status']}")
                    # Default to 'pending' for invalid statuses
                    silver_df.at[idx, 'status'] = 'pending'
                
                # Set overall validation status
                if validation_messages:
                    validation_statuses.append("WARNING: " + "; ".join(validation_messages))
                else:
                    validation_statuses.append("VALID")
            
            silver_df['validation_status'] = validation_statuses
            
            # Add metadata
            silver_df['processing_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            silver_df['bronze_ingestion_timestamp'] = silver_df['ingestion_timestamp']
            
            # Drop bronze-specific columns
            silver_df = silver_df.drop(['timestamp', 'ingestion_timestamp', 'source_file'], axis=1)
            
            # Begin transaction for data integrity
            conn.execute("BEGIN TRANSACTION")
            
            # Insert into silver layer
            silver_df.to_sql('silver_transactions', conn, if_exists='append', index=False)
            
            # Commit transaction
            conn.commit()
            logger.info(f"Processed {len(silver_df)} records into silver layer")
            
            # Log validation statistics
            valid_count = sum(1 for status in validation_statuses if status == "VALID")
            warning_count = len(validation_statuses) - valid_count
            logger.info(f"Validation results: {valid_count} valid records, {warning_count} with warnings")
            
            conn.close()
            return True
        
        except Exception as e:
            logger.error(f"Error processing data for silver layer: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return False
    
    def process_gold_layer(self) -> bool:
        """
        Aggregate silver data into gold layer.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.connect()
            
            # Get the latest date in gold layer
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(summary_date) FROM gold_daily_summary")
            last_processed_date = cursor.fetchone()[0]
            
            # Build query to get data not yet aggregated
            if last_processed_date:
                query = f'''
                SELECT 
                    transaction_date,
                    transaction_type,
                    category,
                    COUNT(*) as transaction_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount
                FROM silver_transactions
                WHERE transaction_date > '{last_processed_date}'
                  AND validation_status = 'VALID'
                GROUP BY transaction_date, transaction_type, category
                '''
            else:
                query = '''
                SELECT 
                    transaction_date,
                    transaction_type,
                    category,
                    COUNT(*) as transaction_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount
                FROM silver_transactions
                WHERE validation_status = 'VALID'
                GROUP BY transaction_date, transaction_type, category
                '''
            
            df = pd.read_sql(query, conn)
            
            if len(df) == 0:
                logger.info("No new data to aggregate for gold layer")
                conn.close()
                return True
            
            # Add processing timestamp
            df['processing_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Rename columns to match gold table
            df = df.rename(columns={'transaction_date': 'summary_date'})
            
            # Handle empty categories
            df['category'] = df['category'].fillna('unknown')
            df.loc[df['category'] == '', 'category'] = 'unknown'
            
            # Begin transaction
            conn.execute("BEGIN TRANSACTION")
            
            # Insert into gold layer - handle conflicts by replacing
            for _, row in df.iterrows():
                cursor.execute('''
                INSERT OR REPLACE INTO gold_daily_summary (
                    summary_date, transaction_type, category, 
                    transaction_count, total_amount, avg_amount, 
                    min_amount, max_amount, processing_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['summary_date'], row['transaction_type'], row['category'],
                    row['transaction_count'], row['total_amount'], row['avg_amount'],
                    row['min_amount'], row['max_amount'], row['processing_timestamp']
                ))
            
            # Commit transaction
            conn.commit()
            logger.info(f"Aggregated {len(df)} records into gold layer")
            
            conn.close()
            return True
        
        except Exception as e:
            logger.error(f"Error aggregating data for gold layer: {e}")
            if 'conn' in locals() and conn:
                conn.rollback()
                conn.close()
            return False
    
    def export_data(self, layer: str, output_dir: str = DEFAULT_EXPORT_DIR) -> Optional[str]:
        """
        Export data from a specific layer to PARQUET for S3 upload.
        
        Args:
            layer: Layer to export ('bronze', 'silver', or 'gold')
            output_dir: Directory to save the exported file
            
        Returns:
            Path to the exported file if successful, None otherwise
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            conn = self.connect()
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            if layer == 'bronze':
                query = "SELECT * FROM bronze_transactions"
                output_file = f"{output_dir}/bronze_transactions_{timestamp}.PARQUET"
            elif layer == 'silver':
                query = "SELECT * FROM silver_transactions"
                output_file = f"{output_dir}/silver_transactions_{timestamp}.PARQUET"
            elif layer == 'gold':
                query = "SELECT * FROM gold_daily_summary"
                output_file = f"{output_dir}/gold_daily_summary_{timestamp}.PARQUET"
            else:
                logger.error(f"Invalid layer: {layer}")
                return None
            
            df = pd.read_sql(query, conn)
            df.to_parquet(output_file, index=False)
            
            conn.close()
            logger.info(f"Exported {len(df)} records from {layer} layer to {output_file}")
            return output_file
        
        except Exception as e:
            logger.error(f"Error exporting {layer} layer data: {e}")
            return None
    
    def get_layer_stats(self) -> Dict[str, int]:
        """
        Get record counts for each layer.
        
        Returns:
            Dictionary with record counts for each layer
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            stats = {}
            
            # Bronze layer count
            cursor.execute("SELECT COUNT(*) FROM bronze_transactions")
            stats['bronze_count'] = cursor.fetchone()[0]
            
            # Silver layer count
            cursor.execute("SELECT COUNT(*) FROM silver_transactions")
            stats['silver_count'] = cursor.fetchone()[0]
            
            # Gold layer count
            cursor.execute("SELECT COUNT(*) FROM gold_daily_summary")
            stats['gold_count'] = cursor.fetchone()[0]
            
            conn.close()
            return stats
        
        except Exception as e:
            logger.error(f"Error getting layer stats: {e}")
            return {'bronze_count': -1, 'silver_count': -1, 'gold_count': -1}
    
    def run_pipeline(self, input_file_path: Optional[str] = None) -> Dict[str, Optional[str]]:
        """
        Run the full ETL pipeline.
        
        Args:
            input_file_path: Path to input PARQUET file (optional)
            
        Returns:
            Dictionary with paths to exported files for each layer
        """
        try:
            # Create tables if they don't exist
            self.create_tables()
            
            # Process bronze layer if input_PARQUET is provided
            if input_file_path:
                bronze_success = self.process_bronze_layer(input_file_path)
                if not bronze_success:
                    raise Exception("Bronze layer processing failed")
            
            # Process silver layer
            silver_success = self.process_silver_layer()
            if not silver_success:
                raise Exception("Silver layer processing failed")
            
            # Process gold layer
            gold_success = self.process_gold_layer()
            if not gold_success:
                raise Exception("Gold layer processing failed")
            
            # Export data for S3 upload
            bronze_file = self.export_data('bronze')
            silver_file = self.export_data('silver')
            gold_file = self.export_data('gold')
            
            # Log statistics
            stats = self.get_layer_stats()
            logger.info(f"Pipeline completed successfully. Layer statistics: {stats}")
            
            return {
                'bronze_file': bronze_file,
                'silver_file': silver_file,
                'gold_file': gold_file
            }
        
        except Exception as e:
            logger.error(f"Error running pipeline: {e}")
            return {
                'bronze_file': None,
                'silver_file': None,
                'gold_file': None
            }


def main():
    """Command-line entry point for the pipeline."""
    parser = argparse.ArgumentParser(description='Run the medallion architecture ETL pipeline')
    # parser.add_argument('--PARQUET', type=str, help='Path to input PARQUET file')
    parser.add_argument('--parquet', type=str, help='Path to input PARQUET file')
    parser.add_argument('--db', type=str, default=DEFAULT_DB_PATH, help='Path to SQLite database')
    parser.add_argument('--export-dir', type=str, default=DEFAULT_EXPORT_DIR, help='Directory for exported files')
    parser.add_argument('--export-only', action='store_true', help='Only export data without processing')
    
    args = parser.parse_args()
    
    pipeline = MedallionPipeline(db_path=args.db)
    
    if args.export_only:
        bronze_file = pipeline.export_data('bronze', output_dir=args.export_dir)
        silver_file = pipeline.export_data('silver', output_dir=args.export_dir)
        gold_file = pipeline.export_data('gold', output_dir=args.export_dir)
        
        print("Export completed:")
        print(f"Bronze layer: {bronze_file}")
        print(f"Silver layer: {silver_file}")
        print(f"Gold layer: {gold_file}")
    else:
        # result = pipeline.run_pipeline(input_PARQUET=args.PARQUET)
        result = pipeline.run_pipeline(input_file_path=args.parquet)
        
        print("Pipeline execution completed:")
        print(f"Bronze layer export: {result['bronze_file']}")
        print(f"Silver layer export: {result['silver_file']}")
        print(f"Gold layer export: {result['gold_file']}")
    
    # Display layer statistics
    stats = pipeline.get_layer_stats()
    print("\nLayer statistics:")
    print(f"Bronze layer: {stats['bronze_count']} records")
    print(f"Silver layer: {stats['silver_count']} records")
    print(f"Gold layer: {stats['gold_count']} records")


if __name__ == "__main__":
    main()
