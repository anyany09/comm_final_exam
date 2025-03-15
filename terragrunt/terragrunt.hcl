# Set global terragrunt settings
remote_state {
  backend = "s3"

  generate = {
    path      = "backend.tf"
    if_exists = "overwrite"
  }

  config = {
    bucket         = "de-exam-terraform-state-${get_aws_account_id()}"
    key            = "${path_relative_to_include()}/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "de-exam-terraform-locks"
  }
}

# Generate provider.tf file
generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite"
  contents  = <<EOF
provider "aws" {
  region = "${local.aws_region}"
}
EOF
}

# Define inputs that are common across all environments
inputs = {
  tags = {
    Project     = "DataEngineeringExam"
    ManagedBy   = "Terragrunt"
    Environment = local.env
  }
}

# Extract environment-specific variables
locals {
  # Load variables from the environment-specific vars file
  env_vars = read_terragrunt_config(find_in_parent_folders("env.hcl"))

  env        = local.env_vars.locals.environment
  aws_region = local.env_vars.locals.aws_region
}
