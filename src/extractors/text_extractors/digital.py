import pdfplumber
import logging
from ..base import BaseTextExtractor

# Configure logging
logger = logging.getLogger(__name__)

class FastDigitalExtractor(BaseTextExtractor):
    def extract(self, file_path: str) -> str:
        text_content = []
        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    # Extract text and filter out generic noise if needed
                    page_text = page.extract_text() or ""
                    text_content.append(page_text)
            return "\n".join(text_content)
        except Exception as e:
            logger.error(f"Fast extraction failed for {file_path}: {e}")
            return ""
