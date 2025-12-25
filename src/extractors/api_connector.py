import json
import logging
import time
import base64
import re
import os
import hashlib
from io import BytesIO
from groq import Groq, InternalServerError, RateLimitError
import pypdfium2 as pdfium 
from pypdf import PdfReader
from PIL import Image
from src.core.pattern_loader import pattern_config  
from src.core.config_loader import settings
from src.core.prompt_loader import PromptLoader 

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
CACHE_DIR = "groq_cache"
os.makedirs(CACHE_DIR, exist_ok=True)
MIN_REQUEST_INTERVAL = 2.0 
LAST_CALL_TIME = 0

# --- ROBUST KEY LOADER ---
def get_groq_key_safe():
    key = os.getenv("GROQ_API_KEY")
    if key: return key.strip()
    possible_files = ["config.txt", ".env", "settings.ini"]
    for fname in possible_files:
        if os.path.exists(fname):
            try:
                with open(fname, "r", encoding="utf-8") as f:
                    for line in f:
                        if "GROQ_API_KEY" in line and "=" in line:
                            parts = line.split("=", 1)
                            clean_key = parts[1].strip().strip('"').strip("'")
                            if clean_key.startswith("gsk_"): return clean_key
            except: pass
    try: return settings.get_api_key()
    except: return None

try:
    API_KEY = get_groq_key_safe()
    client = Groq(api_key=API_KEY) if API_KEY else None
    MODEL_NAME = "meta-llama/llama-4-scout-17b-16e-instruct"
except Exception as e:
    logger.error(f"CRITICAL: Failed to init Groq client: {e}")
    client = None

# --- HELPERS ---
def encode_image(image: Image.Image):
    buffered = BytesIO()
    image.save(buffered, format="JPEG", quality=85) 
    return base64.b64encode(buffered.getvalue()).decode('utf-8')

def _get_file_hash(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""): hasher.update(chunk)
    return hasher.hexdigest()

def _get_cache_path(file_hash, operation_tag, page_index=None):
    suffix = f"_p{page_index}" if page_index is not None else ""
    return os.path.join(CACHE_DIR, f"{file_hash}_{operation_tag}{suffix}.json")

def _load_from_cache(cache_path):
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f: return json.load(f)
        except: pass
    return None

def _save_to_cache(cache_path, data):
    try:
        with open(cache_path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=2)
    except: pass

def _enforce_rate_limit():
    global LAST_CALL_TIME
    elapsed = time.time() - LAST_CALL_TIME
    if elapsed < MIN_REQUEST_INTERVAL: time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    LAST_CALL_TIME = time.time()

def _call_groq_vision(prompt_text, image_obj, max_retries=3):
    if not client: return None
    _enforce_rate_limit()
    base64_image = encode_image(image_obj)
    messages = [{"role": "user", "content": [{"type": "text", "text": prompt_text}, {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}]}]
    for attempt in range(max_retries):
        try:
            chat_completion = client.chat.completions.create(
                messages=messages, model=MODEL_NAME, temperature=0.0, max_tokens=2048, response_format={"type": "json_object"} 
            )
            return chat_completion.choices[0].message.content
        except RateLimitError:
            time.sleep((attempt + 1) * 5)
        except Exception as e:
            logger.error(f"[API ERROR] {e}")
            return None
    return None

def clean_json_response(raw_text):
    if not raw_text: return None
    try:
        text = re.sub(r"```json\s*", "", raw_text, flags=re.IGNORECASE)
        text = re.sub(r"```", "", text)
        return text.strip()
    except: return raw_text

# =========================================================
#                   EXTRACTORS (WITH FILE CLOSING)
# =========================================================

def extract_po_number(file_path: str):
    file_hash = _get_file_hash(file_path)
    cache_path = _get_cache_path(file_hash, "po_num")
    cached = _load_from_cache(cache_path)
    if cached: return cached

    prompt = PromptLoader.get("extract_po_number") + "\nIMPORTANT: Return ONLY the JSON object."
    result_val = None
    
    # [FIX] Explicitly close PDF
    pdf = None
    try:
        pdf = pdfium.PdfDocument(file_path)
        if len(pdf) > 0:
            img = pdf[0].render(scale=2).to_pil().convert("RGB")
            raw_json = _call_groq_vision(prompt, img)
            if raw_json:
                data = json.loads(clean_json_response(raw_json))
                result_val = data.get("po_number")
    except Exception as e: logger.warning(f"PO Extract failed: {e}")
    finally:
        if pdf: pdf.close()  # <--- RELEASES FILE LOCK

    _save_to_cache(cache_path, result_val)
    return result_val

def extract_line_items_from_crop(image: Image.Image):
    prompt = PromptLoader.get("extract_line_items_crop") + "\nReturn ONLY valid JSON."
    logger.info(f"[CROP] Sending Table to Groq...")
    
    raw_resp = _call_groq_vision(prompt, image)
    if raw_resp:
        clean_text = clean_json_response(raw_resp)
        try:
            data = json.loads(clean_text)
            # Handle dictionary wrapper (fixes NULL columns)
            if isinstance(data, dict):
                for k in ["items", "rows", "table_rows", "data"]:
                    if k in data: return data[k]
                # If wrapped in unknown key, try finding the list
                for v in data.values():
                    if isinstance(v, list): return v
                return [data] # fallback
            return data
        except: return []
    return []

def extract_line_items_full_page(file_path: str, page_index=0):
    file_hash = _get_file_hash(file_path)
    cache_path = _get_cache_path(file_hash, "full_table", page_index)
    cached = _load_from_cache(cache_path)
    if cached: return cached

    prompt = PromptLoader.get("extract_line_items_full_page")
    result_list = []
    
    # [FIX] Explicitly close PDF
    pdf = None
    try:
        pdf = pdfium.PdfDocument(file_path)
        if page_index < len(pdf):
            img = pdf[page_index].render(scale=2).to_pil().convert("RGB")
            logger.info(f"[PAGE] Sending Page {page_index+1} to Groq...")
            raw_resp = _call_groq_vision(prompt, img)
            if raw_resp:
                clean_text = clean_json_response(raw_resp)
                try:
                    data = json.loads(clean_text)
                    if isinstance(data, list): result_list = data
                    elif isinstance(data, dict):
                        for k in ["items", "rows", "table_rows"]:
                            if k in data: 
                                result_list = data[k]
                                break
                except: pass
    except Exception: pass
    finally:
        if pdf: pdf.close() # <--- RELEASES FILE LOCK

    _save_to_cache(cache_path, result_list)
    return result_list

def classify_document_type(file_path: str) -> str:
    # 1. Regex (Safe open)
    try:
        with open(file_path, 'rb') as f:
            reader = PdfReader(f)
            if len(reader.pages) > 0:
                text = reader.pages[0].extract_text().lower()
                if text.strip():
                    for doc_type, regex_list in pattern_config.get_type_patterns().items():
                        for pattern in regex_list:
                            if re.search(pattern, text): return doc_type
    except: pass

    # 2. Vision
    file_hash = _get_file_hash(file_path)
    cache_path = _get_cache_path(file_hash, "classification")
    cached = _load_from_cache(cache_path)
    if cached: return cached

    prompt = "Classify: 'purchase_order', 'delivery_note', 'sales_invoice', or 'unknown'. JSON: {\"type\": \"...\"}"
    result_type = "unknown"
    
    # [FIX] Explicitly close PDF
    pdf = None
    try:
        pdf = pdfium.PdfDocument(file_path)
        img = pdf[0].render(scale=1).to_pil().convert("RGB")
        raw_resp = _call_groq_vision(prompt, img)
        if raw_resp:
            data = json.loads(clean_json_response(raw_resp))
            result_type = data.get("type", "unknown")
    except: pass
    finally:
        if pdf: pdf.close() # <--- RELEASES FILE LOCK

    _save_to_cache(cache_path, result_type)
    return result_type