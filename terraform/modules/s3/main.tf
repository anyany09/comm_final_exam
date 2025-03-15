resource "aws_s3_bucket" "bronze_bucket" {
  bucket = var.bronze_bucket_name

  tags = merge(
    var.tags,
    {
      Name = "Bronze Data Layer"
    }
  )
}

resource "aws_s3_bucket" "silver_bucket" {
  bucket = var.silver_bucket_name

  tags = merge(
    var.tags,
    {
      Name = "Silver Data Layer"
    }
  )
}

resource "aws_s3_bucket" "gold_bucket" {
  bucket = var.gold_bucket_name

  tags = merge(
    var.tags,
    {
      Name = "Gold Data Layer"
    }
  )
}

# Configure bucket notifications for bronze bucket
resource "aws_s3_bucket_notification" "bronze_notification" {
  bucket = aws_s3_bucket.bronze_bucket.id

  lambda_function {
    lambda_function_arn = var.bronze_to_silver_lambda_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "bronze_"
    filter_suffix       = ".csv"
  }
}

# Configure bucket notifications for silver bucket
resource "aws_s3_bucket_notification" "silver_notification" {
  bucket = aws_s3_bucket.silver_bucket.id

  lambda_function {
    lambda_function_arn = var.silver_to_gold_lambda_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "silver_"
    filter_suffix       = ".csv"
  }
}