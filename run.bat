@echo off
title AUTOMATION SYSTEM GUI
setlocal

:: 1. Activate
call venv\Scripts\activate

:: 2. Launch GUI
echo 🚀 AUTOMATION SYSTEM...
echo (Press Ctrl+C to close)
echo --------------------------------
python cli.py --gui

pause