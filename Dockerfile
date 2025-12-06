# Use a lightweight Python base image
FROM python:3.10-slim

# 1. Install System Dependencies
# - poppler-utils: For PDF to Image conversion
# - libmagic1: For the Security Byte check
# - libgl1 & libglib2.0-0: Required by OpenCV
RUN apt-get update && apt-get install -y \
    poppler-utils \
    libmagic1 \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# 2. Set Working Directory
WORKDIR /app

# 3. Install Python Dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy Application Code
COPY src/ ./src/
COPY cli.py .
COPY prompts.yaml .

# 5. Create Data Directories
RUN mkdir -p Purchase_order Delivery_note Sales_invoice \
    Merged_PDFs quarantine gemini_cache reports

# 6. Set Environment Variables
ENV PYTHONUNBUFFERED=1

# 7. Default Command
CMD ["python", "cli.py", "--loop", "--interval", "60"]