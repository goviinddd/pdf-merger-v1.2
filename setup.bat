@echo off
setlocal
echo 🪟 --- WINDOWS SETUP (Full Auto) ---

:: 1. Create venv
if not exist "venv" (
    echo 📦 Creating virtual environment...
    python -m venv venv
)

:: 2. Activate & Install Libs
call venv\Scripts\activate
echo ⬇️  Installing Python requirements...
pip install --upgrade pip
pip install -r requirements.txt
:: Fix for 'magic' on Windows
pip install python-magic-bin

:: 3. AUTO POPPLER INSTALL
if not exist "bin\poppler" (
    echo 🔧 Downloading Poppler (PDF Tool)...
    if not exist "bin" mkdir bin
    powershell -Command "Invoke-WebRequest -Uri 'https://github.com/oschwartz10612/poppler-windows/releases/download/v24.02.0-0/Release-24.02.0-0.zip' -OutFile 'bin\poppler.zip'"
    powershell -Command "Expand-Archive -Path 'bin\poppler.zip' -DestinationPath 'bin\poppler_temp'"
    move "bin\poppler_temp\poppler-24.02.0\Library" "bin\poppler"
    rmdir /s /q "bin\poppler_temp"
    del "bin\poppler.zip"
)

:: 4. Create Folders
if not exist "reports" mkdir reports
if not exist "gemini_cache" mkdir gemini_cache
if not exist "quarantine" mkdir quarantine
if not exist "Purchase_order" mkdir Purchase_order

echo ✅ SETUP COMPLETE!
pause