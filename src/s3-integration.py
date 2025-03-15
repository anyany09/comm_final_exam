#!/usr/bin/env python3
"""
AWS S3 Integration Module

This module handles the integration between the local SQLite medallion architecture
and AWS S3 storage, providing functions to upload data from each layer to corresponding
S3 buckets with proper error handling, retries, and verification.
"""

import os
import logging
import hashlib
import time
import gzip
import json
from typing import Dict, List, Optional, Tuple, Union, BinaryIO
from pathlib import Path
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, EndpointConnectionError
import pandas as pd

# Import project-specific logger
from utils.logger import setup_logger

# Set up logger
logger = setup_logger('s3_integration')

# Default configuration
DEFAULT_BUCKET_PREFIX = "data-engineering-exam"
DEFAULT_REGION = "eu-central-1"
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 2  # seconds

class S3Integration:
    """Handles integration between local data and AWS S3 storage."""
    
    def __init__(
        self,
        bucket_prefix: str = DEFAULT_BUCKET_PREFIX,
        region: str = DEFAULT_REGION,
        retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
        retry_delay: int = DEFAULT_RETRY_DELAY,
        compress: bool = True,
        profile_name: Optional[str] = None
    ):
        """
        Initialize the S3 integration module.
        
        Args:
            bucket_prefix: Prefix for S3 bucket names
            region: AWS region to use
            retry_attempts: Number of retry attempts for S3 operations
            retry_delay: Delay between retry attempts in seconds
            compress: Whether to compress data before upload
            profile_name: AWS profile name to use for credentials
        """
        self.bucket_prefix = bucket_prefix
        self.region = region
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.compress = compress
        
        # Initialize boto3 session and clients
        self.session = boto3.Session(profile_name=profile_name, region_name=region)
        self.s3_client = self.session.client('s3')
        
        # Define bucket names
        self.bronze_bucket = f"{bucket_prefix}-bronze"
        self.silver_bucket = f"{bucket_prefix}-silver"
        self.gold_bucket = f"{bucket_prefix}-gold"
        
        # Configure transfer settings for multipart uploads
        self.transfer_config = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,  # 8MB
            max_concurrency=10,
            multipart_chunksize=8 * 1024 * 1024,  # 8MB
            use_threads=True
        )
        
        # Ensure buckets exist
        self._ensure_buckets_exist()
    
    def _ensure_buckets_exist(self) -> None:
        """Ensure all required S3 buckets exist, creating them if necessary."""
        buckets = [self.bronze_bucket, self.silver_bucket, self.gold_bucket]
        
        for bucket in buckets:
            try:
                self.s3_client.head_bucket(Bucket=bucket)
                logger.info(f"Bucket {bucket} already exists")
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code')
                
                if error_code == '404':
                    logger.info(f"Creating bucket {bucket} in region {self.region}")
                    try:
                        # Create bucket with appropriate configuration
                        if self.region == 'us-east-1':
                            self.s3_client.create_bucket(Bucket=bucket)
                        else:
                            location = {'LocationConstraint': self.region}
                            self.s3_client.create_bucket(
                                Bucket=bucket,
                                CreateBucketConfiguration=location
                            )
                        logger.info(f"Successfully created bucket {bucket}")
                    except ClientError as create_error:
                        logger.error(f"Failed to create bucket {bucket}: {create_error}")
                        raise
                else:
                    logger.error(f"Error checking bucket {bucket}: {e}")
                    raise
    
    def _calculate_md5(self, file_path: str) -> str:
        """
        Calculate MD5 hash of a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            MD5 hash as a hex string
        """
        md5_hash = hashlib.md5()
        
        with open(file_path, "rb") as f:
            # Read file in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
                
        return md5_hash.hexdigest()
    
    def _compress_file(self, file_path: str) -> str:
        """
        Compress a file using gzip.
        
        Args:
            file_path: Path to the file to compress
            
        Returns:
            Path to the compressed file
        """
        compressed_path = f"{file_path}.gz"
        
        with open(file_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                f_out.writelines(f_in)
        
        logger.info(f"Compressed {file_path} to {compressed_path}")
        return compressed_path

    def _upload_with_retry(
            self,
            file_path: str,
            bucket: str,
            object_key: str,
            metadata: Dict[str, str]
    ) -> bool:
        """
        Upload a file to S3 with retry logic.
        """
        attempt = 0
        md5_hash = self._calculate_md5(file_path)
        file_size = os.path.getsize(file_path)

        # Determine file type based on extension
        file_extension = Path(file_path).suffix.lower()
        if file_extension == '.parquet':
            content_type = 'application/vnd.apache-parquet'
        elif file_extension == '.csv':
            content_type = 'text/csv'
        else:
            # Default or determine by other extensions as needed
            content_type = 'application/octet-stream'

        # Add file information to metadata
        metadata.update({
            'md5_hash': md5_hash,
            'original_size': str(file_size),
            'content_type': content_type,
        })

        # Only compress non-parquet files (Parquet is already compressed)
        should_compress = self.compress and not file_path.endswith('.gz') and file_extension != '.parquet'

        if should_compress:
            file_path = self._compress_file(file_path)
            metadata['compression'] = 'gzip'

        while attempt < self.retry_attempts:
            try:
                logger.info(f"Uploading {file_path} to s3://{bucket}/{object_key} (Attempt {attempt + 1})")

                # Set appropriate content encoding
                content_encoding = None
                if should_compress:
                    content_encoding = 'gzip'

                extra_args = {
                    'Metadata': metadata,
                    'ContentType': content_type
                }

                if content_encoding:
                    extra_args['ContentEncoding'] = content_encoding

                # Use TransferManager for efficient uploads
                self.s3_client.upload_file(
                    Filename=file_path,
                    Bucket=bucket,
                    Key=object_key,
                    ExtraArgs=extra_args,
                    Config=self.transfer_config
                )

                # Verify upload
                if self._verify_upload(bucket, object_key, md5_hash, file_size):
                    logger.info(f"Successfully uploaded and verified file to s3://{bucket}/{object_key}")
                    return True
                else:
                    logger.warning(f"Upload verification failed for s3://{bucket}/{object_key}")
                    attempt += 1

            except (ClientError, EndpointConnectionError) as e:
                logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
                attempt += 1

                if attempt < self.retry_attempts:
                    sleep_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)

        logger.error(f"Failed to upload {file_path} after {self.retry_attempts} attempts")
        return False
    def _verify_upload(
        self,
        bucket: str,
        object_key: str,
        original_md5: str,
        original_size: int
    ) -> bool:
        """
        Verify that a file was correctly uploaded to S3.
        
        Args:
            bucket: S3 bucket name
            object_key: S3 object key
            original_md5: Original file MD5 hash
            original_size: Original file size
            
        Returns:
            True if verification passes, False otherwise
        """
        try:
            # Get object metadata
            response = self.s3_client.head_object(Bucket=bucket, Key=object_key)
            
            # Check if the file exists and has the expected metadata
            s3_metadata = response.get('Metadata', {})
            
            # If we're comparing MD5, check it matches
            if 'md5_hash' in s3_metadata:
                s3_md5 = s3_metadata['md5_hash']
                if s3_md5 != original_md5:
                    logger.warning(f"MD5 mismatch for s3://{bucket}/{object_key}: expected {original_md5}, got {s3_md5}")
                    return False
            
            # Additional checks could be performed here
            
            return True
        
        except ClientError as e:
            logger.error(f"Error verifying upload: {e}")
            return False
    
    def upload_layer_data(
        self,
        file_path: str,
        layer: str,
        partition_key: Optional[str] = None
    ) -> Optional[str]:
        """
        Upload data from a specific layer to the corresponding S3 bucket.
        
        Args:
            file_path: Path to the file to upload
            layer: Medallion layer ('bronze', 'silver', or 'gold')
            partition_key: Optional partition key (e.g., 'date=2023-01-01')
            
        Returns:
            S3 URI of the uploaded file if successful, None otherwise
        """
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
        
        # Determine appropriate bucket
        if layer.lower() == 'bronze':
            bucket = self.bronze_bucket
        elif layer.lower() == 'silver':
            bucket = self.silver_bucket
        elif layer.lower() == 'gold':
            bucket = self.gold_bucket
        else:
            logger.error(f"Invalid layer: {layer}")
            return None
        
        # Get the filename from the path
        filename = os.path.basename(file_path)
        
        # Construct S3 object key, including partition if provided
        if partition_key:
            object_key = f"{layer}/{partition_key}/{filename}"
        else:
            current_date = time.strftime("%Y-%m-%d")
            object_key = f"{layer}/date={current_date}/{filename}"
        
        # Prepare metadata
        metadata = {
            'source': 'sqlite_pipeline',
            'layer': layer,
            'upload_date': time.strftime("%Y-%m-%d %H:%M:%S"),
            'original_filename': filename
        }
        
        # Upload the file
        if self._upload_with_retry(file_path, bucket, object_key, metadata):
            s3_uri = f"s3://{bucket}/{object_key}"
            return s3_uri
        
        return None
    
    def upload_all_layers(
        self,
        bronze_file: Optional[str] = None,
        silver_file: Optional[str] = None,
        gold_file: Optional[str] = None
    ) -> Dict[str, Optional[str]]:
        """
        Upload data from all medallion layers to their respective S3 buckets.
        
        Args:
            bronze_file: Path to the bronze layer file
            silver_file: Path to the silver layer file
            gold_file: Path to the gold layer file
            
        Returns:
            Dictionary with S3 URIs for each uploaded file
        """
        result = {
            'bronze_uri': None,
            'silver_uri': None,
            'gold_uri': None
        }
        
        # Upload bronze layer if provided
        if bronze_file and os.path.exists(bronze_file):
            result['bronze_uri'] = self.upload_layer_data(bronze_file, 'bronze')
        
        # Upload silver layer if provided
        if silver_file and os.path.exists(silver_file):
            result['silver_uri'] = self.upload_layer_data(silver_file, 'silver')
        
        # Upload gold layer if provided
        if gold_file and os.path.exists(gold_file):
            result['gold_uri'] = self.upload_layer_data(gold_file, 'gold')
        
        return result
    
    def list_bucket_contents(self, layer: str) -> List[Dict[str, str]]:
        """
        List contents of a specific layer's bucket.
        
        Args:
            layer: Medallion layer ('bronze', 'silver', or 'gold')
            
        Returns:
            List of objects in the bucket with their metadata
        """
        if layer.lower() == 'bronze':
            bucket = self.bronze_bucket
        elif layer.lower() == 'silver':
            bucket = self.silver_bucket
        elif layer.lower() == 'gold':
            bucket = self.gold_bucket
        else:
            logger.error(f"Invalid layer: {layer}")
            return []
        
        try:
            result = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Get object metadata
                        metadata_response = self.s3_client.head_object(
                            Bucket=bucket,
                            Key=obj['Key']
                        )
                        
                        result.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].strftime("%Y-%m-%d %H:%M:%S"),
                            'metadata': metadata_response.get('Metadata', {})
                        })
            
            return result
        
        except ClientError as e:
            logger.error(f"Error listing bucket contents: {e}")
            return []
    
    def download_file(
        self,
        object_uri: str,
        download_path: str
    ) -> bool:
        """
        Download a file from S3.
        
        Args:
            object_uri: S3 URI (s3://bucket/key)
            download_path: Local path to download the file to
            
        Returns:
            True if download was successful, False otherwise
        """
        # Parse S3 URI
        if not object_uri.startswith('s3://'):
            logger.error(f"Invalid S3 URI: {object_uri}")
            return False
        
        bucket_key = object_uri[5:]  # Remove 's3://'
        parts = bucket_key.split('/', 1)
        
        if len(parts) != 2:
            logger.error(f"Invalid S3 URI format: {object_uri}")
            return False
        
        bucket, key = parts
        
        # Ensure download directory exists
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        
        try:
            # Download the file
            self.s3_client.download_file(bucket, key, download_path)
            logger.info(f"Successfully downloaded s3://{bucket}/{key} to {download_path}")
            
            # Check if the file is compressed
            metadata = self.s3_client.head_object(Bucket=bucket, Key=key).get('Metadata', {})
            
            # Decompress if needed
            if metadata.get('compression') == 'gzip' and not download_path.endswith('.gz'):
                decompressed_path = download_path.replace('.gz', '')
                
                with gzip.open(download_path, 'rb') as f_in:
                    with open(decompressed_path, 'wb') as f_out:
                        f_out.write(f_in.read())
                
                # Remove compressed file if decompression successful
                os.remove(download_path)
                logger.info(f"Decompressed {download_path} to {decompressed_path}")
                
                return True
            
            return True
        
        except ClientError as e:
            logger.error(f"Error downloading file: {e}")
            return False

    def delete_file(self, object_uri: str) -> bool:
        """
        Delete a file from S3.
        
        Args:
            object_uri: S3 URI (s3://bucket/key)
            
        Returns:
            True if deletion was successful, False otherwise
        """
        # Parse S3 URI
        if not object_uri.startswith('s3://'):
            logger.error(f"Invalid S3 URI: {object_uri}")
            return False
        
        bucket_key = object_uri[5:]  # Remove 's3://'
        parts = bucket_key.split('/', 1)
        
        if len(parts) != 2:
            logger.error(f"Invalid S3 URI format: {object_uri}")
            return False
        
        bucket, key = parts
        
        try:
            # Delete the file
            self.s3_client.delete_object(Bucket=bucket, Key=key)
            logger.info(f"Successfully deleted s3://{bucket}/{key}")
            return True
        
        except ClientError as e:
            logger.error(f"Error deleting file: {e}")
            return False

    def get_s3_stats(self) -> Dict[str, Dict[str, Union[int, float]]]:
        """
        Get statistics about S3 buckets.
        
        Returns:
            Dictionary with statistics for each layer
        """
        result = {}
        
        for layer in ['bronze', 'silver', 'gold']:
            if layer == 'bronze':
                bucket = self.bronze_bucket
            elif layer == 'silver':
                bucket = self.silver_bucket
            else:  # gold
                bucket = self.gold_bucket
            
            try:
                # Get bucket statistics
                total_size = 0
                object_count = 0
                
                paginator = self.s3_client.get_paginator('list_objects_v2')
                
                for page in paginator.paginate(Bucket=bucket):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            total_size += obj['Size']
                            object_count += 1
                
                # Calculate statistics
                result[layer] = {
                    'object_count': object_count,
                    'total_size_bytes': total_size,
                    'total_size_mb': round(total_size / (1024 * 1024), 2),
                    'average_size_kb': round(total_size / (object_count * 1024), 2) if object_count > 0 else 0
                }
            
            except ClientError as e:
                logger.error(f"Error getting stats for {bucket}: {e}")
                result[layer] = {
                    'object_count': -1,
                    'total_size_bytes': -1,
                    'total_size_mb': -1,
                    'average_size_kb': -1
                }
        
        return result


def main():
    """Main function to demonstrate the S3 integration."""
    import argparse
    
    parser = argparse.ArgumentParser(description='S3 Integration Tool')
    parser.add_argument('--bronze', type=str, help='Path to bronze layer file')
    parser.add_argument('--silver', type=str, help='Path to silver layer file')
    parser.add_argument('--gold', type=str, help='Path to gold layer file')
    parser.add_argument('--bucket-prefix', type=str, default=DEFAULT_BUCKET_PREFIX,
                        help='S3 bucket prefix')
    parser.add_argument('--region', type=str, default=DEFAULT_REGION,
                        help='AWS region')
    parser.add_argument('--profile', type=str, default='comm-de',
                        help='AWS profile name')
    parser.add_argument('--compress', action='store_true',
                        help='Compress files before upload')
    parser.add_argument('--list', type=str, choices=['bronze', 'silver', 'gold'],
                        help='List contents of a specific layer bucket')
    parser.add_argument('--stats', action='store_true',
                        help='Show statistics for all buckets')
    
    args = parser.parse_args()
    
    # Initialize S3 integration
    s3_integration = S3Integration(
        bucket_prefix=args.bucket_prefix,
        region=args.region,
        profile_name=args.profile,
        compress=args.compress
    )
    
    # List bucket contents if requested
    if args.list:
        contents = s3_integration.list_bucket_contents(args.list)
        print(f"\nContents of {args.list} bucket:")
        for item in contents:
            print(f"  {item['key']} ({item['size']} bytes, modified: {item['last_modified']})")
    
    # Show statistics if requested
    if args.stats:
        stats = s3_integration.get_s3_stats()
        print("\nBucket statistics:")
        for layer, layer_stats in stats.items():
            print(f"\n{layer.upper()} Layer:")
            print(f"  Objects: {layer_stats['object_count']}")
            print(f"  Total size: {layer_stats['total_size_mb']} MB")
            print(f"  Average object size: {layer_stats['average_size_kb']} KB")
    
    # Upload files if provided
    if args.bronze or args.silver or args.gold:
        result = s3_integration.upload_all_layers(
            bronze_file=args.bronze,
            silver_file=args.silver,
            gold_file=args.gold
        )
        
        print("\nUpload results:")
        for layer, uri in result.items():
            print(f"  {layer}: {uri if uri else 'Not uploaded'}")


if __name__ == "__main__":
    # data/s3_upload/bronze_transactions_20250311_053546.PARQUET
    # data/s3_upload/silver_transactions_20250311_053547.PARQUET
    # data/s3_upload/gold_transactions_20250311_053546.PARQUET
    main()
