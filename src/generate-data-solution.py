#!/usr/bin/env python3
"""
Transaction Data Generator

This script generates synthetic financial transaction data for testing and development purposes.
It creates a dataset of realistic transaction records with various attributes, following patterns
typical of real-world financial data.

The script generates between 20,000-30,000 records by default and saves them to a CSV file.
"""

import pandas as pd
import csv
import random
import uuid
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from utils.logger import setup_logger

logger = setup_logger("Data_Generator", log_file="data_generator.log")

# Define constants
DEFAULT_OUTPUT_DIR = "../data/sample"
DEFAULT_NUM_RECORDS = 25000
DEFAULT_NUM_CUSTOMERS = 1000
DEFAULT_NUM_MERCHANTS = 200
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# Define transaction types and their probability weights
TRANSACTION_TYPES = {
    'purchase': 0.65,
    'refund': 0.05,
    'transfer': 0.15,
    'payment': 0.10,
    'withdrawal': 0.05
}

# Define spending categories with merchant associations
CATEGORIES = {
    'food': ['Restaurant', 'Grocery', 'Cafe', 'FastFood'],
    'entertainment': ['Cinema', 'Theater', 'StreamingService', 'GameStore'],
    'travel': ['Airline', 'Hotel', 'CarRental', 'TravelAgency'],
    'utilities': ['Electric', 'Water', 'Internet', 'Phone'],
    'retail': ['Clothing', 'Electronics', 'HomeGoods', 'OnlineRetail'],
    'healthcare': ['Pharmacy', 'Doctor', 'Hospital', 'Insurance']
}

# Define transaction statuses with probability weights
STATUSES = {
    'completed': 0.85,
    'pending': 0.10,
    'failed': 0.03,
    'reversed': 0.02
}

def generate_customer_ids(num_customers: int) -> List[str]:
    """Generate a list of unique customer IDs.
    
    Args:
        num_customers: Number of unique customers to generate
        
    Returns:
        List of customer ID strings
    """
    return [f"CUST{i:06d}" for i in range(1, num_customers + 1)]

def generate_merchants(num_merchants: int) -> Dict[str, Dict[str, Any]]:
    """Generate a dictionary of merchants with their categories.
    
    Args:
        num_merchants: Number of merchants to generate
        
    Returns:
        Dictionary mapping merchant IDs to their attributes
    """
    merchants = {}
    merchant_types = []
    
    # Flatten merchant types from categories
    for category, types in CATEGORIES.items():
        for type_name in types:
            merchant_types.append((category, type_name))
    
    # Create merchants
    for i in range(1, num_merchants + 1):
        merchant_id = f"MERCH{i:04d}"
        
        # Choose a merchant type and its associated category
        category, type_name = random.choice(merchant_types)
        
        # Create merchant name based on type with some uniqueness
        suffixes = ["Inc", "LLC", "Co", "Express", "Shop", "Mart", "Plus"]
        name = f"{type_name} {random.choice(suffixes)}"
        
        merchants[merchant_id] = {
            "name": name,
            "category": category
        }
    
    return merchants

def get_amount_for_transaction(transaction_type: str) -> float:
    """Generate a realistic amount based on transaction type.
    
    Args:
        transaction_type: Type of financial transaction
        
    Returns:
        Amount as a float, rounded to 2 decimal places
    """
    if transaction_type == 'purchase':
        # Purchases follow a right-skewed distribution - most are small, some are large
        amount = random.gammavariate(alpha=1.5, beta=20)
        return round(min(amount, 1000.0), 2)  # Cap at $1000
    
    elif transaction_type == 'refund':
        # Refunds are negative and typically smaller than purchases
        amount = random.gammavariate(alpha=1.2, beta=15)
        return round(-1 * min(amount, 500.0), 2)  # Cap at $500
    
    elif transaction_type == 'transfer':
        # Transfers tend to be larger and more varied
        if random.random() < 0.1:  # 10% chance of large transfer
            return round(random.uniform(1000.0, 10000.0), 2)
        else:
            return round(random.uniform(50.0, 1000.0), 2)
    
    elif transaction_type == 'payment':
        # Payments cluster around common bill amounts
        common_amounts = [9.99, 14.99, 29.99, 49.99, 99.99]
        if random.random() < 0.3:  # 30% chance of common payment amount
            return random.choice(common_amounts)
        else:
            return round(random.uniform(10.0, 500.0), 2)
    
    else:  # withdrawal
        # Withdrawals tend to be in rounded amounts
        amounts = [20.0, 40.0, 60.0, 80.0, 100.0, 200.0, 300.0, 500.0]
        return random.choice(amounts)

def generate_timestamp(start_date: datetime, end_date: datetime) -> str:
    """Generate a random timestamp between start and end dates.
    
    Args:
        start_date: The earliest possible timestamp
        end_date: The latest possible timestamp
        
    Returns:
        Formatted timestamp string
    """
    # Get random point between start and end dates
    time_between_dates = end_date - start_date
    random_seconds = random.randint(0, int(time_between_dates.total_seconds()))
    random_date = start_date + timedelta(seconds=random_seconds)
    
    # Format timestamp
    return random_date.strftime(TIMESTAMP_FORMAT)

def choose_weighted(options: Dict[str, float]) -> str:
    """Choose an option based on weighted probabilities.
    
    Args:
        options: Dictionary mapping options to their probability weights
        
    Returns:
        Selected option
    """
    choices, weights = zip(*options.items())
    return random.choices(choices, weights=weights, k=1)[0]

def create_transaction_record(
    customer_ids: List[str], 
    merchants: Dict[str, Dict[str, Any]], 
    start_date: datetime, 
    end_date: datetime
) -> Dict[str, Any]:
    """Create a single transaction record with realistic attributes.
    
    Args:
        customer_ids: List of available customer IDs
        merchants: Dictionary of available merchants
        start_date: Earliest possible transaction date
        end_date: Latest possible transaction date
        
    Returns:
        Dictionary containing transaction record attributes
    """
    # Select transaction type based on weighted probabilities
    transaction_type = choose_weighted(TRANSACTION_TYPES)
    
    # Generate base transaction
    transaction = {
        'transaction_id': str(uuid.uuid4()),
        'customer_id': random.choice(customer_ids),
        'timestamp': generate_timestamp(start_date, end_date),
        'amount': get_amount_for_transaction(transaction_type),
        'transaction_type': transaction_type,
        'status': choose_weighted(STATUSES)
    }
    
    # Add merchant and category for purchase and refund transactions
    if transaction_type in ['purchase', 'refund']:
        merchant_id = random.choice(list(merchants.keys()))
        merchant_data = merchants[merchant_id]
        
        transaction['merchant'] = f"{merchant_id}:{merchant_data['name']}"
        transaction['category'] = merchant_data['category']
    else:
        transaction['merchant'] = ''
        transaction['category'] = ''
    
    return transaction

def generate_transaction_data(
    num_records: int = DEFAULT_NUM_RECORDS,
    num_customers: int = DEFAULT_NUM_CUSTOMERS,
    num_merchants: int = DEFAULT_NUM_MERCHANTS,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    filename: str = "transactions.csv"
) -> Optional[str]:
    """Generate synthetic transaction data and save to CSV.
    
    Args:
        num_records: Number of transaction records to generate
        num_customers: Number of unique customers to include
        num_merchants: Number of unique merchants to include
        output_dir: Directory to save the output file
        filename: Name of the output CSV file
        
    Returns:
        Path to the generated file if successful, None otherwise
    """
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, filename)
        
        logger.info(f"Starting generation of {num_records} transaction records")
        
        # Generate customer IDs and merchants
        customer_ids = generate_customer_ids(num_customers)
        merchants = generate_merchants(num_merchants)
        
        # Define date range (last 30 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        # Define CSV headers
        headers = [
            'transaction_id', 
            'customer_id', 
            'timestamp', 
            'amount', 
            'transaction_type',
            'merchant',
            'category',
            'status'
        ]
        
        # Generate and write data
        # Generate and save data
        transactions = []
        for _ in range(num_records):
            transaction = create_transaction_record(
                customer_ids, merchants, start_date, end_date
            )
            transactions.append(transaction)

        # Import pandas for DataFrame creation and parquet saving


        # Convert to DataFrame and save as parquet
        df = pd.DataFrame(transactions)
        parquet_file = os.path.splitext(output_file)[0] + '.parquet'
        df.to_parquet(parquet_file, engine='pyarrow')

        # Also save as CSV for compatibility
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(transactions)
        
        logger.info(f"Successfully generated {num_records} records in {output_file}")
        return output_file
    
    except Exception as e:
        logger.error(f"Error generating transaction data: {e}")
        return None

def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate synthetic transaction data')
    parser.add_argument('--records', type=int, default=DEFAULT_NUM_RECORDS,
                        help=f'Number of records to generate (default: {DEFAULT_NUM_RECORDS})')
    parser.add_argument('--customers', type=int, default=DEFAULT_NUM_CUSTOMERS,
                        help=f'Number of unique customers (default: {DEFAULT_NUM_CUSTOMERS})')
    parser.add_argument('--merchants', type=int, default=DEFAULT_NUM_MERCHANTS,
                        help=f'Number of unique merchants (default: {DEFAULT_NUM_MERCHANTS})')
    parser.add_argument('--output-dir', type=str, default=DEFAULT_OUTPUT_DIR,
                        help=f'Output directory (default: {DEFAULT_OUTPUT_DIR})')
    parser.add_argument('--filename', type=str, default="transactions.csv",
                        help='Output filename (default: transactions.csv)')
    
    args = parser.parse_args()
    
    output_file = generate_transaction_data(
        num_records=args.records,
        num_customers=args.customers,
        num_merchants=args.merchants,
        output_dir=args.output_dir,
        filename=args.filename
    )
    
    if output_file:
        print(f"Data generation complete. File saved to: {output_file}")
    else:
        print("Data generation failed. Check logs for details.")

if __name__ == "__main__":
    main()
