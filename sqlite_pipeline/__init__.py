"""
SQLite Medallion ETL Package

Modules:
    bronze.py   - Ingests raw CSV data into the bronze layer.
    silver.py   - Transforms and validates data into the silver layer.
    gold.py     - Aggregates silver data into the gold layer.
    run_pipeline.py - Orchestrates the full ETL pipeline and exports outputs.

Version: 1.0.0
"""
