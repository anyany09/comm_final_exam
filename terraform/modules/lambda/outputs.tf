output "bronze_to_silver_lambda_arn" {
  description = "ARN of the bronze to silver Lambda function"
  value       = aws_lambda_function.bronze_to_silver.arn
}

output "silver_to_gold_lambda_arn" {
  description = "ARN of the silver to gold Lambda function"
  value       = aws_lambda_function.silver_to_gold.arn
}