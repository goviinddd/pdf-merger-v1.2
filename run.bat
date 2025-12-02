@echo off
TITLE Automated Invoice Merger Bot
color 0A

:: Ensure we are running in the script's directory
cd /d "%~dp0"

:: --- CHECK FOR PYTHON ---
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python is not installed or not in PATH!
    echo Please install Python 3.10+ and check "Add to PATH" during installation.
    pause
    exit /b
)

:: --- SETUP VIRTUAL ENV ---
if not exist "venv" (
    echo [INFO] First run detected. Creating virtual environment...
    python -m venv venv
    
    echo [INFO] Activating environment...
    call venv\Scripts\activate
    
    echo [INFO] Installing dependencies...
    pip install -r requirements.txt
    
    echo [INFO] Setup complete.
) else (
    echo [INFO] Virtual environment found. Activating...
    call venv\Scripts\activate
)

:: --- VALIDATION ---
if not exist "po_detector.pt" (
    echo [WARNING] po_detector.pt is missing!
    echo Please ensure the YOLO model file is in this folder.
    pause
)

:: --- RUN THE BOT ---
cls
echo ========================================================
echo    PDF MERGER BOT IS RUNNING
echo ========================================================
echo  [+] Watching: Purchase_order, Delivery_note, Sales_invoice
echo  [+] AI Model: YOLO + Gemini Flash
echo  [+] Status: Active (Looping every 10s)
echo ========================================================
echo.

:: Run python from the venv explicitly to be safe
venv\Scripts\python.exe cli.py --loop --interval 10

:: If it crashes, keep window open so user can see error
echo.
echo [CRITICAL] The bot has stopped. See error above.
pause
