import logging
import numpy as np
import pypdfium2 as pdfium
from ..base import BaseTextExtractor

logger = logging.getLogger(__name__)

class RapidOCRExtractor(BaseTextExtractor):
    """
    Strategy C: The 'Eagle Eye' Approach (RapidOCR).
    
    OPTIMIZED:
    - Uses pypdfium2 (No Poppler dependency).
    - Lazy loads model.
    - Streams pages for low memory usage.
    """
    
    def __init__(self):
        self.engine = None
        self._model_loaded = False

    def _load_model(self):
        if self._model_loaded: return
        try:
            from rapidocr_onnxruntime import RapidOCR
            # det_use_cuda=False ensures it runs on CPU without crashing
            self.engine = RapidOCR(det_use_cuda=False, cls_use_cuda=False, rec_use_cuda=False)
            self._model_loaded = True
        except ImportError:
            logger.error("❌ rapidocr_onnxruntime not installed.")
        except Exception as e:
            logger.error(f"Failed to load RapidOCR: {e}")
            self._model_loaded = False

    def extract(self, file_path: str) -> str:
        self._load_model()
        if not self._model_loaded:
            return ""

        text_content = []
        try:
            with pdfium.PdfDocument(file_path) as pdf:
                for page in pdf:
                    # scale=3 is roughly 216 DPI (72 * 3), perfect for OCR
                    pil_image = page.render(scale=3).to_pil().convert("RGB")
                    img_array = np.array(pil_image)
                    
                    # Run OCR
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