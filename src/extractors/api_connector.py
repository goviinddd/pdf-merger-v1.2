import json
import logging
import time
import random
import re
import os
import google.generativeai as genai
import pypdfium2 as pdfium 
from pypdf import PdfReader
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from PIL import Image
from src.core.pattern_loader import pattern_config  
from src.core.config_loader import settings

logger = logging.getLogger(__name__)
# --- LOAD SETTINGS ---
try:
    API_KEY = settings.get_api_key()
    LLM_CONFIG = settings.get_llm_settings()
    
    # Configure Gemini immediately
    genai.configure(api_key=API_KEY)
    
    ACTIVE_MODELS = [LLM_CONFIG['model_name']]
    
except Exception as e:
    logger.error(f"CRITICAL: Failed to load config settings: {e}")
    API_KEY = None
    ACTIVE_MODELS = []

# --- HELPER: ROBUST RETRY LOGIC ---
def _generate_with_retry(model, content, config=None, safety_settings=None, max_retries=3):
    """
    Tries to generate content, handling Rate Limits (429) automatically.
    """
    for attempt in range(max_retries):
        try:
            return model.generate_content(
                content,
                generation_config=config,
                safety_settings=safety_settings
            )
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "Resource exhausted" in error_str:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Rate limit hit (429). Retrying in {wait_time:.2f}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"Gemini API Error: {e}")
                return None # Stop retrying on non-transient errors
    logger.error("Max retries exceeded for Gemini API.")
    return None

def extract_po_number(file_path: str):
    """
    Fallback: Sends the document to Gemini to find the PO Number.
    """
    if not API_KEY: return None

    prompt = """
    Extract the Purchase Order (PO) Number.
    Look for labels like "PO Number", "Order #", "P.O.", "Procurement Ref".
    Return ONLY JSON: {"po_number": "value"}
    If NOT found, return {"po_number": null}
    """
    
    # Use config settings for generation
    gen_config = {
        "response_mime_type": "application/json",
        "temperature": 0.0 # Force precision for extraction
    }

    def _query(image, model_name):
        try:
            model = genai.GenerativeModel(model_name)
            response = _generate_with_retry(
                model, 
                [prompt, image],
                config=gen_config
            )
            if response and response.text:
                data = json.loads(response.text)
                return data.get("po_number")
        except Exception:
            pass
        return None

    try:
        pdf = pdfium.PdfDocument(file_path)
        if len(pdf) == 0: return None

        for model_name in ACTIVE_MODELS:
            # Check Page 1
            page_1_img = pdf[0].render(scale=3).to_pil().convert("RGB")
            po = _query(page_1_img, model_name)
            if po: return po
            
            # Check Page 2 if needed
            if len(pdf) > 1:
                page_2_img = pdf[1].render(scale=3).to_pil().convert("RGB")
                po = _query(page_2_img, model_name)
                if po: return po
            
    except Exception as e:
        logger.error(f"Gemini Fallback Error: {e}")

    return None

def extract_line_items_from_crop(image: Image.Image):
    """
    Sends a TABLE CROP to Gemini.
    """
    prompt = """
    Extract table rows.
    
    CRITICAL RULE FOR 'line_ref':
    1. Look inside the 'Description' or text columns for phrases like "Line Item - X", "PO Line X", or "Item No: X".
    2. If found, use THAT number 'X' as the "line_ref".
    3. ONLY use the 'SL', 'No', or Row Number column if NO specific PO Line reference exists in the text.
    
    Output JSON list: [{"line_ref": "2", "description": "Item Name", "part_no": "123", "quantity": "4"}]
    Return [] if unreadable.
    """
    
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }

    # Pull user config
    gen_config = {
        "response_mime_type": "application/json",
        "temperature": LLM_CONFIG['temperature'],
        "max_output_tokens": LLM_CONFIG['max_tokens']
    }

    for model_name in ACTIVE_MODELS:
        try:
            model = genai.GenerativeModel(model_name)
            response = _generate_with_retry(
                model,
                [prompt, image],
                safety_settings=safety_settings,
                config=gen_config
            )
            if response and response.text:
                return response.text
        except Exception as e:
            logger.error(f"Error with {model_name}: {e}")
            continue

    return "[]"

def extract_line_items_full_page(file_path: str):
    """
    Fallback: Full page scan.
    """
    if not API_KEY: return "[]"

    prompt = """
    Analyze this document. Identify the main table.
    
    CRITICAL RULE FOR 'line_ref':
    1. Look inside the 'Description' or text columns for phrases like "Line Item - X", "PO Line X", or "Item No: X".
    2. If found, use THAT number 'X' as the "line_ref".
    3. ONLY use the 'SL', 'No', or Row Number column if NO specific PO Line reference exists in the text.
    
    Output JSON list: [{"line_ref": "2", "description": "Item Name", "part_no": "123", "quantity": "4"}]
    Return [] if no table data is found.
    """
    
    gen_config = {
        "response_mime_type": "application/json",
        "temperature": LLM_CONFIG['temperature'],
        "max_output_tokens": LLM_CONFIG['max_tokens']
    }

    def _query(image, model_name):
        try:
            model = genai.GenerativeModel(model_name)
            response = _generate_with_retry(
                model,
                [prompt, image],
                config=gen_config
            )
            if response and response.text:
                # Cleanup markdown just in case model ignores mime_type
                return response.text.replace("```json", "").replace("```", "").strip()
        except Exception as e:
            logger.warning(f"Full-page scan with {model_name} failed: {e}")
            return None

    try:
        pdf = pdfium.PdfDocument(file_path)
        if len(pdf) == 0: return "[]"
        
        image = pdf[0].render(scale=3).to_pil().convert("RGB")
        
        for model_name in ACTIVE_MODELS:
            result = _query(image, model_name)
            if result and result != "[]":
                return result
                
    except Exception as e:
        logger.error(f"Gemini Full-Page Fallback Error: {e}")

    return "[]"

def classify_document_type(file_path: str) -> str:
    """
    Hybrid Classifier:
    1. Tries to read text keywords (Free/Fast).
    2. If that fails (scanned PDF), asks Gemini (Cost/Smart).
    """
    
    #  REGEX 
    try:
        reader = PdfReader(file_path)
        if len(reader.pages) > 0:
            text = reader.pages[0].extract_text().lower()
            preview = text[:100].replace('\n', ' ') # Show first 100 chars flat
            logger.info(f"🔎 TEXT CHECK for {os.path.basename(file_path)}: '{preview}'")
            if not text.strip():
                logger.warning("   (Text is empty! File is likely a scanned image.)")
            type_patterns = pattern_config.get_type_patterns()
            
            # Loop through the YAML structure
            for doc_type, regex_list in type_patterns.items():
                for pattern in regex_list:
                    if re.search(pattern, text):
                        return doc_type
    except Exception as e:
        logger.warning(f"Regex classification failed (file might be image-only): {e}")

    # GEMINI VISION 
    if not API_KEY: return "unknown"

    logger.info(f"Regex failed. Asking Gemini to classify {os.path.basename(file_path)}...")

    prompt = """
    Classify this document image into one of these exact categories:
    1. "purchase_order"
    2. "delivery_note"
    3. "sales_invoice"
    
    Return ONLY the category string. If unsure, return "unknown".
    """
    
    try:
        pdf = pdfium.PdfDocument(file_path)
        if len(pdf) == 0: return "unknown"
        
        image = pdf[0].render(scale=1).to_pil().convert("RGB") 
        
        for model_name in ACTIVE_MODELS:
            try:
                model = genai.GenerativeModel(model_name)
                response = _generate_with_retry(model, [prompt, image])
                if response and response.text:
                    result = response.text.strip().lower().replace('"', '').replace("'", "")
                    for valid in ["purchase_order", "delivery_note", "sales_invoice"]:
                        if valid in result:
                            return valid
            except:
                continue
                
    except Exception as e:
        logger.error(f"Gemini Classification Error: {e}")

    return "unknown"