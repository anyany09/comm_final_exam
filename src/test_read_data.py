import boto3

# Create an S3 client with correct credentials
s3_client = boto3.client(
    "s3",
    region_name="eu-central-1"
)

# List all S3 buckets
try:
    response = s3_client.list_buckets()
    for bucket in response["Buckets"]:
        print(f"Bucket Name: {bucket['Name']}")
except Exception as e:
    print(f"Error: {e}")