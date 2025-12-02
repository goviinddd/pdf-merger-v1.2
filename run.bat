@echo off
TITLE Automated Invoice Merger Bot
color 0A

:: --- CONFIGURATION ---
:: Check for Python
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python is not installed!
    echo Please install Python 3.10+ from python.org and check "Add to PATH".
    pause
    exit /b
)

:: --- SETUP VIRTUAL ENV ---
if not exist "venv" (
    echo [INFO] First run detected. Setting up virtual environment...
    python -m venv venv
    call venv\Scripts\activate
    
    echo [INFO] Installing AI dependencies...
    pip install -r requirements.txt
    
    echo [INFO] Downloading YOLO Model...
    :: Note: Usually you include the .pt file in the zip, but this ensures pip is fresh
) else (
    call venv\Scripts\activate
)

:: --- RUN THE BOT ---
cls
echo ========================================================
echo    PDF MERGER BOT IS RUNNING (Do not close)
echo ========================================================
echo  [+] Watching: Purchase_order, Delivery_note, Sales_invoice
echo  [+] Saving to: Merged_PDFs
echo  [+] Mode: Auto-Pilot (Scanning every 10 seconds)
echo ========================================================
echo.

:: Run in loop mode, checking every 10 seconds
python cli.py --loop --interval 10

pause