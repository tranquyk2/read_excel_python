@echo off
echo Starting Excel to Database Sync Tool...
echo.

REM Kiểm tra Python có cài đặt không
python --version >nul 2>&1
if errorlevel 1 (
    echo Python is not installed or not in PATH
    echo Please install Python 3.8 or higher
    pause
    exit /b 1
)

REM Kiểm tra các thư viện cần thiết
echo Checking required libraries...
python -c "import pandas, openpyxl, requests, watchdog, configparser" >nul 2>&1
if errorlevel 1 (
    echo Installing required libraries...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo Failed to install required libraries
        pause
        exit /b 1
    )
)

echo Starting application...
python main.py

if errorlevel 1 (
    echo Application encountered an error
    pause
)