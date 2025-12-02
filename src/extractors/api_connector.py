import os
import logging
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from PIL import Image
from dotenv import load_dotenv
import time
import json

# Configure Logging
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY:
    logger.warning("⚠️ GEMINI_API_KEY not found in .env file. API features will fail.")

# Configure Gemini
genai.configure(api_key=API_KEY)

def debug_print_models():
    """Helper to list all models available to your specific API Key."""
    try:
        print("\n--- AVAILABLE MODELS FOR YOUR KEY ---")
        for m in genai.list_models():
            if 'generateContent' in m.supported_generation_methods:
                print(f" - {m.name}")
        print("-------------------------------------\n")
    except Exception as e:
        logger.error(f"Could not list models: {e}")

def extract_line_items_from_crop(image: Image.Image, retry_count=0):
    """
    Sends a TABLE CROP image to Gemini Flash to extract line items.
    Used in the 'Crop & Link' strategy.
    """
    # Priority list
    candidate_models = [
        'gemini-2.5-flash',      
        'gemini-2.0-flash',      
        'gemini-flash-latest',
        'gemini-1.5-flash-latest'
    ]

    # --- THE CROP-SPECIFIC PROMPT ---
    prompt = """
    You are an expert data extraction agent. 
    You are looking at a cropped image of a table from an invoice.
    
    Extract the data row by row.
    
    Fields to extract:
    1. "line_ref": The line number (e.g., "1", "10", "SL 1"). If missing, try to infer from order.
    2. "description": The full description text.
    3. "part_no": Any part number, SKU, or Material No found in the row.
    4. "quantity": The numeric quantity.
    
    Output format: A pure JSON list of objects.
    Example: 
    [
      {"line_ref": "1", "description": "Hammer", "part_no": "H-123", "quantity": "5"},
      {"line_ref": "2", "description": "Nails", "part_no": "N-99", "quantity": "100"}
    ]
    
    If the image contains NO legible table data, return []
    """
    
    # Disable safety filters (Invoices contain addresses/names)
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }

    for model_name in candidate_models:
        try:
            logger.info(f"🤖 Sending table crop to model: {model_name}")
            model = genai.GenerativeModel(model_name)
            
            response = model.generate_content(
                [prompt, image],
                safety_settings=safety_settings
            )
            
            raw_text = response.text
            
            # Sanitize the output
            clean_json = raw_text.replace("```json", "").replace("```", "").strip()
            
            if clean_json:
                if "[" in clean_json and "]" in clean_json:
                    return clean_json
                else:
                    logger.warning(f"Model {model_name} returned invalid JSON: {clean_json[:50]}...")
            else:
                logger.warning(f"Model {model_name} returned empty text.")
            
        except Exception as e:
            if "429" in str(e): # Rate Limit
                if retry_count < 3:
                    wait_time = (2 ** retry_count) + 1
                    logger.warning(f"Rate limit (429). Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    return extract_line_items_from_crop(image, retry_count + 1)
            
            logger.error(f"Error with {model_name}: {e}")
            continue

    logger.error("❌ All Gemini models failed to read the table crop.")
    return "[]"