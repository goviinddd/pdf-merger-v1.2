from dataclasses import dataclass
from typing import Optional

@dataclass
class DocumentInfo:
    """
    A unified data contract representing the state of a single document
    within the V1 pipeline.

    As defined in the blueprint[cite: 60], this holds:
    1. The file location
    2. The type of document (PO, DO, SI)
    3. The extracted PO Number (the 'Universe' ID)
    4. A hash for duplicate detection
    """
    file_path: str
    doc_type: str  # Expected values: 'po', 'do', 'si'
    po_number: Optional[str] = None
    content_hash: Optional[str] = None

    def is_valid(self) -> bool:
        """Helper to check if the critical extraction succeeded."""
        return self.po_number is not None