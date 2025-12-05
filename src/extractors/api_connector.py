import json
import logging
import time
import random
import re
import os
import hashlib
import google.generativeai as genai
import pypdfium2 as pdfium 
from pypdf import PdfReader
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from PIL import Image
from src.core.pattern_loader import pattern_config  
from src.core.config_loader import settings
from src.core.prompt_loader import PromptLoader 

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
CACHE_DIR = "gemini_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

LAST_CALL_TIME = 0
MIN_REQUEST_INTERVAL = 4.0 

# --- LOAD SETTINGS ---
try:
    API_KEY = settings.get_api_key()
    LLM_CONFIG = settings.get_llm_settings()
    genai.configure(api_key=API_KEY)
    ACTIVE_MODELS = [LLM_CONFIG['model_name']]
except Exception as e:
    logger.error(f"CRITICAL: Failed to load config settings: {e}")
    API_KEY = None
    ACTIVE_MODELS = []

# --- CACHING & HELPERS ---
def _get_file_hash(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def _get_cache_path(file_hash, operation_tag, page_index=None):
    suffix = f"_p{page_index}" if page_index is not None else ""
    return os.path.join(CACHE_DIR, f"{file_hash}_{operation_tag}{suffix}.json")

def _load_from_cache(cache_path):
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"   ⚡ Cache Hit: {os.path.basename(cache_path)}")
                return data
        except: pass
    return None

def _save_to_cache(cache_path, data):
    try:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    except: pass

def _enforce_rate_limit():
    global LAST_CALL_TIME
    elapsed = time.time() - LAST_CALL_TIME
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    LAST_CALL_TIME = time.time()

def _generate_with_retry(model, content, config=None, safety_settings=None, max_retries=3):
    _enforce_rate_limit()
    for attempt in range(max_retries):
        try:
            return model.generate_content(content, generation_config=config, safety_settings=safety_settings)
        except Exception as e:
            if "429" in str(e) or "Resource exhausted" in str(e):
                time.sleep((2 ** attempt) + random.uniform(0, 1))
            else:
                return None
    return None

# =========================================================
#                   EXTRACTORS
# =========================================================

def extract_po_number(file_path: str):
    if not API_KEY: return None
    file_hash = _get_file_hash(file_path)
    cache_path = _get_cache_path(file_hash, "po_num")
    
    cached = _load_from_cache(cache_path)
    if cached: return cached

    prompt = PromptLoader.get("extract_po_number")
    gen_config = {"response_mime_type": "application/json", "temperature": 0.0}

    result_val = None
    try:
        pdf = pdfium.PdfDocument(file_path)
        if len(pdf) > 0:
            for i in range(min(2, len(pdf))):
                img = pdf[i].render(scale=3).to_pil().convert("RGB")
                for model_name in ACTIVE_MODELS:
                    model = genai.GenerativeModel(model_name)
                    resp = _generate_with_retry(model, [prompt, img], config=gen_config)
                    if resp and resp.text:
                        data = json.loads(resp.text)
                        if data.get("po_number"):
                            result_val = data.get("po_number")
                            break
                if result_val: break
    except: pass

    _save_to_cache(cache_path, result_val)
    return result_val

def extract_line_items_from_crop(image: Image.Image):
    # UPDATED PROMPT: Text Priority
    prompt = PromptLoader.get("extract_line_items_crop")
    
    safety_settings = {HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE}
    gen_config = {"response_mime_type": "application/json", "temperature": LLM_CONFIG['temperature']}

    for model_name in ACTIVE_MODELS:
        try:
            model = genai.GenerativeModel(model_name)
            resp = _generate_with_retry(model, [prompt, image], safety_settings=safety_settings, config=gen_config)
            if resp and resp.text: return resp.text
        except: continue
    return "[]"

def extract_line_items_full_page(file_path: str, page_index=0):
    if not API_KEY: return "[]"
    
    file_hash = _get_file_hash(file_path)
    cache_path = _get_cache_path(file_hash, "full_table", page_index)
    cached = _load_from_cache(cache_path)
    if cached: return cached

    prompt = PromptLoader.get("extract_line_items_full_page")
    
    gen_config = {"response_mime_type": "application/json", "temperature": LLM_CONFIG['temperature']}
    
    result_json = "[]"
    try:
        pdf = pdfium.PdfDocument(file_path)
        if page_index < len(pdf):
            image = pdf[page_index].render(scale=3).to_pil().convert("RGB")
            for model_name in ACTIVE_MODELS:
                model = genai.GenerativeModel(model_name)
                resp = _generate_with_retry(model, [prompt, image], config=gen_config)
                if resp and resp.text:
                    result_json = resp.text.replace("```json", "").replace("```", "").strip()
                    break
    except: pass

    _save_to_cache(cache_path, result_json)
    return result_json

def classify_document_type(file_path: str) -> str:
    try:
        reader = PdfReader(file_path)
        if len(reader.pages) > 0:
            text = reader.pages[0].extract_text().lower()
            if text.strip():
                type_patterns = pattern_config.get_type_patterns()
                for doc_type, regex_list in type_patterns.items():
                    for pattern in regex_list:
                        if re.search(pattern, text): return doc_type
    except: pass

    if not API_KEY: return "unknown"
    
    file_hash = _get_file_hash(file_path)
    cache_path = _get_cache_path(file_hash, "classification")
    cached = _load_from_cache(cache_path)
    if cached: return cached

    prompt = PromptLoader.get("classify_document")
    
    result_type = "unknown"
    try:
        pdf = pdfium.PdfDocument(file_path)
        image = pdf[0].render(scale=1).to_pil().convert("RGB")
        for model_name in ACTIVE_MODELS:
            model = genai.GenerativeModel(model_name)
            resp = _generate_with_retry(model, [prompt, image])
            if resp and resp.text:
                clean = resp.text.lower().strip().replace('"','').replace("'", "")
                if "purchase" in clean: result_type = "purchase_order"
                elif "delivery" in clean: result_type = "delivery_note"
                elif "invoice" in clean: result_type = "sales_invoice"
                break
    except: pass

    _save_to_cache(cache_path, result_type)
    return result_type