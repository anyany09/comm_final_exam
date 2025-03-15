@echo off
REM set_pythonpath.bat - Sets up the Python path for the project

REM Get the current directory as project root
set PROJECT_ROOT=%~dp0
set PROJECT_ROOT=%PROJECT_ROOT:~0,-1%

REM Set PYTHONPATH environment variable
set PYTHONPATH=%PROJECT_ROOT%;%PYTHONPATH%

echo Python path has been set to: %PYTHONPATH%
echo You can now run your Python scripts manually.
echo Example: python src/s3-integration.py --profile=comm-de
echo.
echo NOTE: This path setting will only remain active in this command prompt window.
echo.
cmd /k