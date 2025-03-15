variable "bronze_bucket_name" {
  description = "Name of the bronze S3 bucket"
  type        = string
}

variable "silver_bucket_name" {
  description = "Name of the silver S3 bucket"
  type        = string
}

variable "gold_bucket_name" {
  description = "Name of the gold S3 bucket"
  type        = string
}

variable "bronze_to_silver_function_name" {
  description = "Name of the bronze to silver Lambda function"
  type        = string
  default     = "bronze_to_silver_transformation"
}

variable "silver_to_gold_function_name" {
  description = "Name of the silver to gold Lambda function"
  type        = string
  default     = "silver_to_gold_transformation"
}

variable "tags" {
  description = "Tags for resources"
  type        = map(string)
  default     = {
    Project = "DataEngineeringExam"
  }
}