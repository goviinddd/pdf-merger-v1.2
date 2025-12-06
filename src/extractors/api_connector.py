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

# Settings
CACHE_TTL_DAYS = 7        # Delete files older than 7 days
MIN_REQUEST_INTERVAL = 4.0 # Rate limit buffer

# Rate Limiting Globals
LAST_CALL_TIME = 0

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

# =========================================================
#                   CACHE MECHANICS
# =========================================================

def _prune_cache():
    """
    Housekeeping: Deletes cache files older than CACHE_TTL_DAYS.
    Runs silently on module import.
    """
    try:
        now = time.time()
        cutoff = now - (CACHE_TTL_DAYS * 86400)
        deleted = 0
        
        for f in os.listdir(CACHE_DIR):
            fpath = os.path.join(CACHE_DIR, f)
            if os.path.isfile(fpath) and f.endswith(".json"):
                if os.path.getmtime(fpath) < cutoff:
                    os.remove(fpath)
                    deleted += 1
        
        if deleted > 0:
            logger.info(f"Cache Maintenance: Removed {deleted} stale files (> {CACHE_TTL_DAYS} days old).")
    except Exception as e:
        logger.warning(f"Cache prune warning: {e}")

# Run cleanup immediately when module loads
_prune_cache()

def _get_file_hash(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def _get_prompt_hash(prompt_text):
    """Generates a short hash of the prompt instructions."""
    return hashlib.md5(prompt_text.encode('utf-8')).hexdigest()[:8]

def _get_cache_path(file_hash, prompt_text, operation_tag, page_index=None):
    """
    Constructs a cache filename including the PROMPT HASH.
    This ensures that if you edit prompts.yaml, the cache invalidates automatically.
    """
    prompt_sig = _get_prompt_hash(prompt_text)
    suffix = f"_p{page_index}" if page_index is not None else ""
    # Format: [FileHash]_[PromptHash]_[Tag].json
    filename = f"{file_hash}_{prompt_sig}_{operation_tag}{suffix}.json"
    return os.path.join(CACHE_DIR, filename)

            
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

# --- RATE LIMITER ---
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
    
    # Load Prompt
    prompt = PromptLoader.get("extract_po_number")
    
    # Check Cache (Now includes prompt hash)
    file_hash = _get_file_hash(file_path)
    cache_path = _get_cache_path(file_hash, prompt, "po_num")
    
    cached = _load_from_cache(cache_path)
    if cached: return cached

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
    # NOTE: We cannot easily hash an in-memory image for caching without saving it first.
    # For crop extraction, we skip disk caching to maintain speed, relying on Rate Limiting.
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
    
    prompt = PromptLoader.get("extract_line_items_full_page")
    
    file_hash = _get_file_hash(file_path)
    cache_path = _get_cache_path(file_hash, prompt, "full_table", page_index)
    
    cached = _load_from_cache(cache_path)
    if cached: return cached

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
    # Regex Phase (Fast)
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
    
    prompt = PromptLoader.get("classify_document")
    
    file_hash = _get_file_hash(file_path)
    cache_path = _get_cache_path(file_hash, prompt, "classification")
    
    cached = _load_from_cache(cache_path)
    if cached: return cached

    result_type = "unknown"
    try:
        pdf = pdfium.PdfDocument(file_path)
        if len(pdf) > 0:
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