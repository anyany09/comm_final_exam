variable "bronze_bucket_name" {
  description = "Name for the bronze data bucket"
  type        = string
}

variable "silver_bucket_name" {
  description = "Name for the silver data bucket"
  type        = string
}

variable "gold_bucket_name" {
  description = "Name for the gold data bucket"
  type        = string
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "bronze_to_silver_lambda_arn" {
  description = "ARN of the Lambda function to trigger on bronze bucket object creation"
  type        = string
}

variable "silver_to_gold_lambda_arn" {
  description = "ARN of the Lambda function to trigger on silver bucket object creation"
  type        = string
}