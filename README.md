Automated PDF Merger V1.1

A smart, AI-powered pipeline that automatically scans, extracts, reconciles, and merges related supply chain documents.

# Key Features

YOLOv8: Visually detects and crops tables and PO numbers from PDFs with high precision.

Gemini AI: Uses Google's Gemini 2.5 Flash to intelligently read complex, nested line items from tables.

3-Way Matching: Automatically reconciles Purchase Orders against Delivery Notes and Invoices.

Safety Net: Prevents merging if items are missing, incomplete, or if "Ghost Matches" occur.

# 🛠️ 1. Installation

Install Python 3.10+

Ensure Python is installed and added to your system PATH.

Set up the Environment

Open your terminal in the project folder and run:

# Create virtual environment
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate


# Install Dependencies

pip install -r requirements.txt


# ⚙️ 2. Configuration

Create a .env file in the root directory.

Add your API Key:

GEMINI_API_KEY=your_key_starts_with_AIza...


# 🔑 How to Get a Free Gemini API Key

Go to Google AI Studio.

Click Create API key.

Select a project (or create new) and copy the key string.

Tip: Enable billing on Google Cloud to access the "Paid Tier" (Pay-As-You-Go). It removes data training usage (privacy) and rate limits for pennies.

# 🚀 3. How to Run

Start the Bot

python cli.py


On the first run, the system will automatically create the necessary input folders.

#  Feed the Bot

Drop your PDF files into these folders:

Purchase_order/

Delivery_note/

Sales_invoice/

# Watch the Magic

The system will:

Scan files.

Extract data using YOLO + AI.

Validate the line items.

Merge successful bundles into Merged_PDFs/.
