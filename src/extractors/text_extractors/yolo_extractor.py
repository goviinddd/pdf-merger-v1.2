import logging
import numpy as np
from PIL import Image
import pypdfium2 as pdfium 
from ..base import BaseTextExtractor
import os
import cv2

# Configure logging
logger = logging.getLogger(__name__)

# --- CONSTANTS ---
# Lower threshold slightly to catch faint tables
CONFIDENCE_THRESHOLD = 0.25 
DEBUG_OUTPUT_DIR = "debug_yolo_crops"

class YoloExtractor(BaseTextExtractor):
    def __init__(self, model_path="po_detector.pt", target_class_id=1):
        self.model_path = model_path
        self.target_class_id = target_class_id # PO Number Class
        self.yolo_model = None
        self.ocr_engine = None
        self._loaded = False
        
        if not os.path.exists(DEBUG_OUTPUT_DIR):
            os.makedirs(DEBUG_OUTPUT_DIR)

    def _load_models(self):
        if self._loaded: return
        try:
            from ultralytics import YOLO
            from rapidocr_onnxruntime import RapidOCR
            self.yolo_model = YOLO(self.model_path)
            self.ocr_engine = RapidOCR(det_use_cuda=False, cls_use_cuda=False, rec_use_cuda=False)
            logger.info("✅ YOLO + RapidOCR loaded successfully.")
            self._loaded = True
        except ImportError:
            logger.error("Missing dependencies.")

    def extract(self, file_path: str) -> str:
        """
        Extracts PO Number.
        """
        self._load_models()
        if not self.yolo_model: return ""

        extracted_candidates = []
        try:
            pdf = pdfium.PdfDocument(file_path)
            # Scan first page only for PO Number
            for i in range(min(1, len(pdf))):
                page = pdf[i]
                pil_image = page.render(scale=3).to_pil().convert("RGB")
                
                # Run YOLO
                results = self.yolo_model(pil_image, verbose=False, conf=CONFIDENCE_THRESHOLD)
                
                for result in results:
                    for box in result.boxes:
                        if int(box.cls[0]) == self.target_class_id:
                            # Found PO Box
                            x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
                            crop = pil_image.crop((x1-5, y1-5, x2+5, y2+5))
                            
                            # OCR
                            crop_np = np.array(crop)
                            ocr_result, _ = self.ocr_engine(crop_np)
                            
                            if ocr_result:
                                for line in ocr_result:
                                    text = line[1].strip()
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
        LEGACY: Single crop method (kept for compatibility if needed).
        """
        crops = self.extract_all_table_crops(file_path)
        return crops[0] if crops else None

    def extract_all_table_crops(self, file_path: str) -> list[Image.Image]:
        """
        Scans ALL pages for tables and returns a list of crop images.
        """
        self._load_models()
        if not self.yolo_model: return []

        TABLE_CLASS_ID = None
        for id, name in self.yolo_model.names.items():
            if name == 'Table Zone':
                TABLE_CLASS_ID = id
                break
        
        if TABLE_CLASS_ID is None: return []

        found_crops = []

        try:
            pdf = pdfium.PdfDocument(file_path)
            # Scan up to 5 pages
            for i in range(min(5, len(pdf))):
                page = pdf[i]
                pil_image = page.render(scale=3).to_pil().convert("RGB")
                
                results = self.yolo_model(pil_image, verbose=False, conf=CONFIDENCE_THRESHOLD)
                
                for result in results:
                    # Save debug image
                    debug_saved = False
                    
                    for box in result.boxes:
                        if int(box.cls[0]) == TABLE_CLASS_ID:
                            # Found Table!
                            x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
                            width, height = pil_image.size
                            
                            crop = pil_image.crop((
                                max(0, x1 - 10), 
                                max(0, y1 - 10), 
                                min(width, x2 + 10), 
                                min(height, y2 + 10)
                            ))
                            found_crops.append(crop)
                            
                            if not debug_saved:
                                debug_img_array = result.plot()
                                debug_filename = f"{os.path.basename(file_path)}_p{i}_debug.jpg"
                                debug_path = os.path.join(DEBUG_OUTPUT_DIR, debug_filename)
                                try:
                                    cv2.imwrite(debug_path, debug_img_array)
                                    logger.info(f"Saved YOLO debug image to {debug_path}")
                                    debug_saved = True
                                except Exception:
                                    pass
            
            return found_crops 
            
        except Exception as e:
            logger.error(f"Table crop failed: {e}")
            return []