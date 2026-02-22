import logging
import numpy as np
from PIL import Image
import pypdfium2 as pdfium 
from ..base import BaseTextExtractor
import os
from src.core.config_loader import settings 

# Configure logging
logger = logging.getLogger(__name__)

# --- CONSTANTS ---
CONFIDENCE_THRESHOLD = 0.25 

class YoloExtractor(BaseTextExtractor):
    def __init__(self, model_path="po_detector.pt", target_class_id=1):
        self.model_path = model_path
        self.target_class_id = target_class_id # PO Number Class
        self.table_class_id = None # Cached dynamically
        self.yolo_model = None
        self.ocr_engine = None
        self._loaded = False
        
        # Load Hardware Settings once
        self.hw_settings = settings.get_hardware_settings()
        self.device = 'cpu' if self.hw_settings['force_cpu'] else None 

    def _load_models(self):
        """
        Lazy loader for YOLO and RapidOCR.
        """
        if self._loaded: return
        try:
            from ultralytics import YOLO
            from rapidocr_onnxruntime import RapidOCR
            
            logger.info(f"Loading YOLO model from {self.model_path} (Device: {self.device or 'Auto'})...")
            self.yolo_model = YOLO(self.model_path)
            
            # Apply Device Settings
            if self.device:
                self.yolo_model.to(self.device)
            
            # Cache the Table Zone Class ID immediately
            if self.yolo_model.names:
                for id, name in self.yolo_model.names.items():
                    if name == 'Table Zone':
                        self.table_class_id = id
                        break
            
            # Initialize OCR
            self.ocr_engine = RapidOCR(det_use_cuda=False, cls_use_cuda=False, rec_use_cuda=False)
            
            self._loaded = True
            logger.info(" YOLO + RapidOCR loaded successfully.")
        except ImportError:
            logger.error(" Missing dependencies (ultralytics or rapidocr_onnxruntime).")
        except Exception as e:
            logger.error(f" Failed to load YOLO model: {e}")

    def extract(self, file_path: str) -> str:
        """
        Extracts PO Number (Sniper Strategy).
        Scans only the first page by default for PO numbers.
        """
        self._load_models()
        if not self.yolo_model: return ""

        extracted_candidates = []
        try:
            with pdfium.PdfDocument(file_path) as pdf:
                # Scan first page only for PO Number
                # min(1, len(pdf)) is just checking if pdf has pages
                if len(pdf) > 0:
                    page = pdf[0]
                    pil_image = page.render(scale=3).to_pil().convert("RGB")
                    
                    # Run YOLO
                    results = self.yolo_model(pil_image, verbose=False, conf=CONFIDENCE_THRESHOLD)
                    
                    for result in results:
                        for box in result.boxes:
                            if int(box.cls[0]) == self.target_class_id:
                                # Found PO Box
                                x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
                                
                                # Add slight padding for OCR
                                crop = pil_image.crop((x1-5, y1-5, x2+5, y2+5))
                                
                                # OCR
                                crop_np = np.array(crop)
                                ocr_result, _ = self.ocr_engine(crop_np)
                                
                                if ocr_result:
                                    for line in ocr_result:
                                        text = line[1].strip()
                                        # Basic filter: must contain at least one digit
                                        if any(char.isdigit() for char in text):
                                            extracted_candidates.append(text)

            if extracted_candidates:
                return "\n".join(extracted_candidates)

            return "" 

        except Exception as e:
            logger.error(f"Sniper extraction failed: {e}")
            return ""

    def extract_table_crop(self, file_path: str) -> Image.Image:
        """
        LEGACY: Returns the first table found.
        """
        crops = self.extract_all_table_crops(file_path)
        return crops[0] if crops else None

    def extract_all_table_crops(self, file_path: str, page_index=None) -> list[Image.Image]:
        """
        Scans for tables and returns a list of crop images.
        """
        self._load_models()
        if not self.yolo_model: return []
        
        # Fast fail if we never found the class ID during load
        if self.table_class_id is None:
            return []

        found_crops = []

        try:
            with pdfium.PdfDocument(file_path) as pdf:
                total_pages = len(pdf)

                # Determine which pages to scan
                if page_index is not None:
                    if 0 <= page_index < total_pages:
                        pages_to_scan = [page_index]
                    else:
                        logger.warning(f"Requested page_index {page_index} is out of bounds for {file_path}")
                        return []
                else:
                    # Default behavior: Scan first 5 pages
                    pages_to_scan = range(min(5, total_pages))

                for i in pages_to_scan:
                    page = pdf[i]
                    # High scale for better small-table detection
                    pil_image = page.render(scale=3).to_pil().convert("RGB")
                    
                    results = self.yolo_model(pil_image, verbose=False, conf=CONFIDENCE_THRESHOLD)
                    
                    for result in results:
                        for box in result.boxes:
                            if int(box.cls[0]) == self.table_class_id:
                                # Found Table!
                                x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
                                width, height = pil_image.size
                                
                                # Add padding to crop (helps Gemini context)
                                crop = pil_image.crop((
                                    max(0, x1 - 15), 
                                    max(0, y1 - 15), 
                                    min(width, x2 + 15), 
                                    min(height, y2 + 15)
                                ))
                                found_crops.append(crop)
            
            return found_crops 
            
        except Exception as e:
            logger.error(f"Table crop failed on {os.path.basename(file_path)}: {e}")
            return []