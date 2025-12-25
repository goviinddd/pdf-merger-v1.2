@echo off
title Receipt Bot GUI
setlocal

:: 1. Activate
call venv\Scripts\activate

:: 2. LINK POPPLER (Critical Step)
set "PATH=%CD%\bin\poppler\bin;%PATH%"

:: 3. Launch GUI
echo 🚀 Launching Receipt Bot GUI...
echo (Keep this window open for logs)
echo --------------------------------
python cli.py --gui

pause