# utils/logger.py
import os
import logging
from datetime import datetime
from typing import Optional


def setup_logger(
    logger_name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    log_dir: str = "logs"
) -> logging.Logger:
    """
    Set up and return a logger with file and console handlers.

    Args:
        logger_name: Name of the logger
        log_file: Optional specific log filename (default: {logger_name}.log)
        level: Logging level (default: INFO)
        log_dir: Directory for log files (default: logs)

    Returns:
        Configured logger instance
    """
    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Set default log file name if not provided
    if log_file is None:
        log_file = f"{logger_name.lower().replace(' ', '_')}.log"

    log_path = os.path.join(log_dir, log_file)

    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Clear existing handlers to avoid duplicates if logger already exists
    if logger.handlers:
        logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Log the file location
    logger.info(f"Log file is being saved to: {os.path.abspath(log_path)}")

    return logger