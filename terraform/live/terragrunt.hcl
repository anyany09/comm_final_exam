# Root level configuration
remote_state {
  backend = "local"  # We'll start with local backend for simplicity
  config = {
    path = "${path_relative_to_include()}/terraform.tfstate"
  }
}

# Global variables that will be available to all child configurations
inputs = {
  project = "final-exam-prep"  # Adjust this to your project name
  common_tags = {
    ManagedBy = "Terragrunt"
    Project   = "final-exam-prep"
  }
}

# AWS Provider configuration
generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<EOF
provider "aws" {
  region = "eu-west-1"  # Adjust this to your preferred AWS region
}
EOF
}
