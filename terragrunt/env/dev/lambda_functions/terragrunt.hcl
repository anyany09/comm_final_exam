include {
  path = find_in_parent_folders()
}

terraform {
  source = "../../../terraform/modules/lambda"
}

inputs = {
  bronze_bucket_name = dependency.s3.outputs.bronze_bucket_name
  silver_bucket_name = dependency.s3.outputs.silver_bucket_name
  gold_bucket_name   = dependency.s3.outputs.gold_bucket_name

  bronze_to_silver_function_name = "bronze_to_silver_transformation_dev"
  silver_to_gold_function_name   = "silver_to_gold_transformation_dev"
}

# Define dependencies
dependency "s3" {
  config_path = "../s3_buckets"

  # Mock outputs for the initial plan/apply
  mock_outputs = {
    bronze_bucket_name = "mock-bronze-bucket"
    silver_bucket_name = "mock-silver-bucket"
    gold_bucket_name   = "mock-gold-bucket"
  }
}