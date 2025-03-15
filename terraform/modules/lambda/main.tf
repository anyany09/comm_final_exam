# IAM role for Lambda functions
resource "aws_iam_role" "lambda_role" {
  name = "data_processing_lambda_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Policy for S3 access
resource "aws_iam_policy" "s3_access_policy" {
  name        = "lambda_s3_access_policy"
  description = "Policy for Lambda S3 access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Effect = "Allow"
        Resource = [
          "arn:aws:s3:::${var.bronze_bucket_name}",
          "arn:aws:s3:::${var.bronze_bucket_name}/*",
          "arn:aws:s3:::${var.silver_bucket_name}",
          "arn:aws:s3:::${var.silver_bucket_name}/*",
          "arn:aws:s3:::${var.gold_bucket_name}",
          "arn:aws:s3:::${var.gold_bucket_name}/*"
        ]
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

# Attach basic Lambda execution policy
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Bronze to Silver Lambda function
resource "aws_lambda_function" "bronze_to_silver" {
  function_name = var.bronze_to_silver_function_name
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"

  # Lambda deployment package
  filename      = "${path.module}/../../aws/lambda/bronze_to_silver/lambda_function.zip"
  source_code_hash = filebase64sha256("${path.module}/../../aws/lambda/bronze_to_silver/lambda_function.zip")

  role = aws_iam_role.lambda_role.arn

  timeout     = 60
  memory_size = 256

  environment {
    variables = {
      SOURCE_BUCKET      = var.bronze_bucket_name
      DESTINATION_BUCKET = var.silver_bucket_name
    }
  }

  tags = var.tags
}

# Silver to Gold Lambda function
resource "aws_lambda_function" "silver_to_gold" {
  function_name = var.silver_to_gold_function_name
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"

  # Lambda deployment package
  filename      = "${path.module}/../../aws/lambda/silver_to_gold/lambda_function.zip"
  source_code_hash = filebase64sha256("${path.module}/../../aws/lambda/silver_to_gold/lambda_function.zip")

  role = aws_iam_role.lambda_role.arn

  timeout     = 60
  memory_size = 256

  environment {
    variables = {
      SOURCE_BUCKET      = var.silver_bucket_name
      DESTINATION_BUCKET = var.gold_bucket_name
    }
  }

  tags = var.tags
}

# S3 trigger permissions for Bronze to Silver Lambda
resource "aws_lambda_permission" "bronze_bucket_permission" {
  statement_id  = "AllowExecutionFromS3Bronze"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.bronze_to_silver.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.bronze_bucket_name}"
}

# S3 trigger permissions for Silver to Gold Lambda
resource "aws_lambda_permission" "silver_bucket_permission" {
  statement_id  = "AllowExecutionFromS3Silver"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.silver_to_gold.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.silver_bucket_name}"
}