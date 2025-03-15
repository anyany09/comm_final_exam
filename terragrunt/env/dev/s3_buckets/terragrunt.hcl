include {
  path = find_in_parent_folders()
}

terraform {
  source = "../../../terraform/modules/s3"
}

inputs = {
  bronze_bucket_name = "de-exam-bronze-dev"
  silver_bucket_name = "de-exam-silver-dev"
  gold_bucket_name   = "de-exam-gold-dev"

  # These will be populated after the Lambda functions are created
  bronze_to_silver_lambda_arn = dependency.lambda.outputs.bronze_to_silver_lambda_arn
  silver_to_gold_lambda_arn   = dependency.lambda.outputs.silver_to_gold_lambda_arn
}

# Define dependencies
dependency "lambda" {
  config_path = "../lambda_functions"

  # Mock outputs for the initial plan/apply
  mock_outputs = {
    bronze_to_silver_lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:mock-function"
    silver_to_gold_lambda_arn   = "arn:aws:lambda:us-east-1:123456789012:function:mock-function"
  }
}