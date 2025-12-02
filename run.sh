#!/bin/bash

# Define colors
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}>>> Automated PDF Merger Bot <<<${NC}"

# Check for venv
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    echo "Installing dependencies..."
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# Run
echo "Starting Pipeline in Daemon Mode..."
python3 cli.py --loop --interval 10