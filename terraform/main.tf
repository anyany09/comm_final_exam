provider "aws" {
  region = var.aws_region
}

# S3 Buckets Module
module "s3_buckets" {
  source = "./modules/s3"

  bronze_bucket_name = var.bronze_bucket_name
  silver_bucket_name = var.silver_bucket_name
  gold_bucket_name   = var.gold_bucket_name

  tags = var.tags
}

# Lambda Functions Module
module "lambda_functions" {
  source = "./modules/lambda"

  bronze_to_silver_function_name = var.bronze_to_silver_function_name
  silver_to_gold_function_name   = var.silver_to_gold_function_name

  bronze_bucket_name = module.s3_buckets.bronze_bucket_name
  silver_bucket_name = module.s3_buckets.silver_bucket_name
  gold_bucket_name   = module.s3_buckets.gold_bucket_name

  tags = var.tags

  depends_on = [module.s3_buckets]
}