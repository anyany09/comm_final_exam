output "bronze_bucket_name" {
  description = "The name of the bronze data bucket"
  value       = aws_s3_bucket.bronze_bucket.bucket
}

output "silver_bucket_name" {
  description = "The name of the silver data bucket"
  value       = aws_s3_bucket.silver_bucket.bucket
}

output "gold_bucket_name" {
  description = "The name of the gold data bucket"
  value       = aws_s3_bucket.gold_bucket.bucket
}