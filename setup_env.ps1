# Get the current directory as project root
$PROJECT_ROOT = $PSScriptRoot

# Set PYTHONPATH environment variable for this session
$env:PYTHONPATH = "$PROJECT_ROOT;$env:PYTHONPATH"

Write-Host "Python path has been set to: $env:PYTHONPATH"
Write-Host "You can now run your Python scripts in this PowerShell window."
Write-Host "Example: python src/s3-integration.py --profile=comm-de"