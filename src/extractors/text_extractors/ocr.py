import logging
import numpy as np
from pdf2image import convert_from_path
from rapidocr_onnxruntime import RapidOCR
from ..base import BaseTextExtractor

logger = logging.getLogger(__name__)

class RapidOCRExtractor(BaseTextExtractor):
    """
    Strategy C: The 'Eagle Eye' Approach (RapidOCR).
    
    Replaces Tesseract with RapidOCR (ONNX version of PaddleOCR).
    - Speed: ~1-2s per page on CPU.
    - Accuracy: Far superior to Tesseract for noisy/scanned docs.
    - Hardware: optimized for standard CPUs (no GPU needed).
    """
    
    def __init__(self):
        # Initialize the model once to save overhead
        # det_use_cuda=False ensures it runs on CPU without crashing
        try:
            self.engine = RapidOCR(det_use_cuda=False, cls_use_cuda=False, rec_use_cuda=False)
            self._model_loaded = True
        except Exception as e:
            logger.error(f"Failed to load RapidOCR: {e}")
            self._model_loaded = False

    def extract(self, file_path: str) -> str:
        if not self._model_loaded:
            return ""

        text_content = []
        try:
            # 1. Convert PDF pages to images (Memory efficient: 200 DPI is enough for RapidOCR)
            images = convert_from_path(file_path, dpi=200)
            
            for i, image in enumerate(images):
                # Convert PIL image to numpy array (RGB)
                img_array = np.array(image)
                
                # 2. Run OCR
                # result structure: [[[[x1,y1],...], "text", confidence], ...]
                result, _ = self.engine(img_array)
                
                if result:
                    # Extract just the text parts and join them
                    page_text = "\n".join([line[1] for line in result])
                    text_content.append(page_text)
            
            return "\n".join(text_content)
            
        except Exception as e:
            logger.error(f"RapidOCR extraction failed for {file_path}: {e}")
            return ""