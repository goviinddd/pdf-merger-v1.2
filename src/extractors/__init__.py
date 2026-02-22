import logging
import os
import re
from .models import DocumentInfo

# --- EXTRACTORS ---
from .text_extractors.digital import FastDigitalExtractor
from .text_extractors.ocr import RapidOCRExtractor
from .text_extractors.yolo_extractor import YoloExtractor
from .po_finder import heuristics

# --- IMPORT THE AI "BRAIN" ---
from .api_connector import extract_po_number as ai_extract_po

logger = logging.getLogger(__name__)

# --- INITIALIZATION ---
_fast_extractor = FastDigitalExtractor()
_ocr_extractor = RapidOCRExtractor()

YOLO_MODEL_PATH = "po_detector.pt"
if os.path.exists(YOLO_MODEL_PATH):
    try:
        _yolo_extractor = YoloExtractor(model_path=YOLO_MODEL_PATH, target_class_id=1)
        logger.info(f" YOLOv8 loaded from {YOLO_MODEL_PATH}")
    except Exception as e:
        logger.warning(f" YOLO crashed on load: {e}")
        _yolo_extractor = None
else:
    _yolo_extractor = None
    logger.warning(f" YOLO model not found at {os.path.abspath(YOLO_MODEL_PATH)}")


def _is_valid_po(candidate: str) -> bool:
    """ 🛡️ THE BOUNCER (V3): Smarter Date & Junk Rejection. """
    if not candidate: return False
    
    val = str(candidate).strip().upper()
    
    # 1. Ban List (Headers/Junk)
    BANNED = [
        "DESCRIPTION", "CODE", "ITEM", "QTY", "TOTAL", "DATE", 
        "PO NUMBER", "INVOICE", "BILL TO", "SHIP TO", "TERMS",
        "PAYMENT", "SUB TOTAL", "PAGE", "OF", "VAT", "TRN"
    ]
    
    if val in BANNED:
        return False
        
    # 2. Minimum Standards
    if len(val) < 3: return False
    
    # 3. Must contain at least one digit
    if not any(char.isdigit() for char in val):
        return False

    date_patterns = [
        r'\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b',  # e.g., 12/31/2023 or 1-2-24
        r'\b\d{4}[./-]\d{1,2}[./-]\d{1,2}\b',    # e.g., 2023-12-31
    ]
    
    for pattern in date_patterns:
        if re.search(pattern, val):
            return False

    return True


def get_document_info(file_path: str, doc_type: str) -> DocumentInfo:
    """
    The Main Public Facade (V1.7 - Digital First Priority).
    Strategy: Digital Regex (100% Acc) -> YOLO (Scans) -> AI (Complex)
    """
    po_number = None

    # --- STRATEGY 1: The Fast Track (Digital Regex) ---
    # MOVED TO TOP: Trust the PDF text data first. It prevents YOLO "typos".
    if doc_type != 'do':
        try:
            extracted_text = _fast_extractor.extract(file_path)
            candidate = heuristics.find_po_number_in_text(extracted_text)
            
            if _is_valid_po(candidate):
                logger.info(f" Digital Fast Track Hit: {candidate}")
                return DocumentInfo(file_path, doc_type, candidate)
        except Exception as e:
            logger.warning(f"Fast Track Failed: {e}")

    # --- STRATEGY 2: The Specialist (YOLO) ---
    # Use this for SCANNED docs where Digital Fast Track failed.
    # Added "and doc_type != 'do'" to fix the Delivery Note "Order #" issue.
    if _yolo_extractor and doc_type != 'do':
        try:
            yolo_text = _yolo_extractor.extract(file_path)
            candidate = heuristics.rescue_yolo_hit(yolo_text)
            
            if _is_valid_po(candidate):
                logger.info(f" YOLO Hit: {candidate}")
                return DocumentInfo(file_path, doc_type, candidate)
        except Exception as e:
            logger.warning(f"YOLO Failed: {e}")

    # --- STRATEGY 3: The Brain (AI / LLM) ---
    logger.info(" Fast methods failed. Calling AI (Gemini/Groq)...")
    try:
        candidate = ai_extract_po(file_path)
        
        if _is_valid_po(candidate):
            logger.info(f" AI Solved it: {candidate}")
            return DocumentInfo(file_path, doc_type, candidate)
    except Exception as e:
        logger.error(f"AI Extraction Failed: {e}")

    # --- STRATEGY 4: Final Brute Force (RapidOCR) ---
    if not po_number:
        logger.warning(f" Sniper & AI failed. Attempting full-page RapidOCR...")
        try:
            extracted_text = _ocr_extractor.extract(file_path)
            candidate = heuristics.find_po_number_in_text(extracted_text)
            if _is_valid_po(candidate):
                po_number = candidate
        except: pass

    return DocumentInfo(file_path, doc_type, po_number)