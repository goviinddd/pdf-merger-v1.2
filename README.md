Automated PDF Merger System (V1.3 Hybrid)

A smart, hybrid pipeline that automatically scans, extracts, reconciles, and merges related supply chain documents. It combines local OCR speed with Google Gemini's AI reasoning to handle complex receipts.
⚡ What's New in V1.3

    🖥️ Dashboard GUI: Monitor the pipeline in real-time with a dark-mode graphical interface (--gui).

    🧠 Smart Caching: Implements disk-based caching (gemini_cache/). If a file has been analyzed once, the system remembers the result instantly—saving API costs and time on re-runs.

    🛡️ Strict Reconciliation: Now performs fuzzy text matching on descriptions. Even if Line Numbers match, it will Quarantine the bundle if the item descriptions (e.g., "Gloves" vs "Drill") don't match.

    📝 External Prompts: All AI instructions are now stored in prompts.yaml. You can tweak the AI's behavior without touching the Python code.

    🔎 Multi-Page Scanning: No longer limited to Page 1. The system scans the entire PDF to find tables that spill over to subsequent pages.

🛠️ Key Features

    Hybrid Extraction Engine:

        Fast Path: Uses Regex/Text extraction for digital PDFs (Instant, Free).

        Smart Path: Falls back to Google Gemini 2.5 & YOLOv8 for scanned images or complex layouts.

    Bundle Quarantine: If a PO has 3 items but the Delivery Note only has 1 (Partial Delivery), or if data conflicts, the system moves all related files to a specific quarantine/MISMATCH_... folder for manual review.

    Safety Net: Prevents "Ghost Matches." The system refuses to merge unless it positively identifies all items.

    Rate Limiting: Automatically throttles API requests to prevent "429 Too Many Requests" errors.

    Excel Reporting: Generates a detailed Reconciliation_Report.xlsx summarizing every successful merge and every failure reason.

🚀 Installation
1. Prerequisites

    Python 3.10+

    Poppler (Required for PDF-to-Image conversion):

        Windows: Download binary, extract, and add bin/ to System PATH.

        Mac: brew install poppler

        Linux: sudo apt-get install poppler-utils

2. Setup

Open your terminal in the project folder:
Bash

# 1. Create virtual environment
python -m venv venv

# 2. Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 3. Install Python Dependencies
pip install -r requirements.txt

3. Configuration (config.txt)

Create a file named config.txt in the root directory. Paste your Google Gemini API Key inside:
Ini, TOML

GEMINI_API_KEY=AIzaSyYourKeyHere...
DB_PATH=merger_state.db

    Tip: You can edit prompts.yaml to change how the AI reads tables without changing code.

🎮 How to Run
Option 1: The GUI Dashboard (Recommended)

Visualizes pending files, successful merges, and quarantines.
Bash

python cli.py --gui

Option 2: Command Line (Headless)

Good for automated servers or cron jobs.
Bash

# Single Pass
python cli.py

# Continuous Watchdog Mode (Runs every 60 seconds)
python cli.py --loop --interval 60

🔍 Troubleshooting Logic

Why was my file Quarantined? Check the DISCREPANCY_REPORT.txt inside the specific quarantine folder. Common reasons:

    Partial Delivery: The PO ordered 5 items, but the Delivery Note only lists 3. The system waits for the full order.

    Content Mismatch: The Line Numbers match (e.g., "1"), but the Description is totally different (e.g., PO: "Apples" vs DO: "Oranges").

    Extraction Failure: The file was too blurry or the table format was unrecognizable.

How to reset the memory? If you want to re-process files from scratch:

    Delete merger_state.db.

    (Optional) Delete the gemini_cache/ folder to force fresh API calls.
