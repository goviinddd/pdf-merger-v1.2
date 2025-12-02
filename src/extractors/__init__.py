import logging
import os
from typing import Optional

from .models import DocumentInfo

from .text_extractors.digital import FastDigitalExtractor
from .text_extractors.ocr import RapidOCRExtractor
from .text_extractors.yolo_extractor import YoloExtractor 

from .po_finder import heuristics

logger = logging.getLogger(__name__)

# --- INITIALIZATION ---
_fast_extractor = FastDigitalExtractor()
_ocr_extractor = RapidOCRExtractor()

YOLO_MODEL_PATH = "po_detector.pt"
if os.path.exists(YOLO_MODEL_PATH):
    _yolo_extractor = YoloExtractor(model_path=YOLO_MODEL_PATH, target_class_id=1) 
    logger.info(f"YOLOv8 loaded from {YOLO_MODEL_PATH}")
else:
    _yolo_extractor = None
    logger.warning(f"YOLO model not found at {os.path.abspath(YOLO_MODEL_PATH)}.")

def get_document_info(file_path: str, doc_type: str) -> DocumentInfo:
    """
    The Main Public Facade (V1.5 - Optimized Sniper).
    """
    po_number = None
    
    # --- STRATEGY 1: The Specialist (YOLO) ---
    if _yolo_extractor:
        yolo_text = _yolo_extractor.extract(file_path)
        po_number = heuristics.rescue_yolo_hit(yolo_text)
        
        if po_number:
             logger.info(f"YOLO Hit: {po_number}")
             return DocumentInfo(file_path, doc_type, po_number)

    # --- STRATEGY 2: The Fast Track (Digital) ---
    # Good for digital PDFs if YOLO somehow misses
    if doc_type != 'do':
        extracted_text = _fast_extractor.extract(file_path)
        po_number = heuristics.find_po_number_in_text(extracted_text)
        if po_number:
            logger.info(f"Digital Fast Track Hit: {po_number}")
            return DocumentInfo(file_path, doc_type, po_number)

    # --- STRATEGY 3: Final Brute Force (RapidOCR) ---
    # If all else fails
    if not po_number:
         logger.warning(f"Sniper & Digital failed. Attempting full-page RapidOCR...")
         extracted_text = _ocr_extractor.extract(file_path)
         po_number = heuristics.find_po_number_in_text(extracted_text)
    
    return DocumentInfo(file_path, doc_type, po_number)