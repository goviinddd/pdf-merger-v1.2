import os
import shutil
import logging
import re
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger(__name__)

class FileSystemManager:
    """
    The 'Actuator' of the system.
    Handles all physical interactions with the hard drive.
    """
    
    def __init__(self, root_dir: str = "."):
        # If running from cli.py, root might need to be adjusted depending on where you run it.
        # using "." assumes you run python cli.py from the project root.
        self.root = Path(root_dir)
        self.dirs = {
            'po': self.root / "Purchase_order",
            'do': self.root / "Delivery_note",
            'si': self.root / "Sales_invoice",
            'output': self.root / "Merged_PDFs",
            'archive': self.root / "Archive", 
            'quarantine': self.root / "Quarantine"
        }
        self._ensure_directories()

    def _ensure_directories(self):
        """Creates necessary folders if they don't exist."""
        for path in self.dirs.values():
            path.mkdir(parents=True, exist_ok=True)

    def scan_and_rename(self) -> List[Tuple[str, str, str]]:
        """
        Phase 1: Standardization.
        Scans input folders, renames files to a standard format (TYPE_Filename),
        and returns a list of (new_full_path, filename, type) for the DB.
        """
        found_files = []
        
        for doc_type, folder in self.dirs.items():
            if doc_type in ['output', 'archive', 'quarantine']:
                continue
                
            if not folder.exists():
                logger.warning(f"Input folder missing: {folder}")
                continue

            # We use glob('*') to catch everything, but filter for PDFs
            for file_path in folder.glob("*"):
                if file_path.suffix.lower() != '.pdf':
                    continue

                # Safety check: Skip files that are already renamed (start with prefix)
                prefix = doc_type.upper() + "_"
                if file_path.name.startswith(prefix):
                    found_files.append((str(file_path), file_path.name, doc_type))
                    continue

                # Create new standardized name: PO_OriginalNameCleaned.pdf
                clean_name = re.sub(r'[^a-zA-Z0-9]', '_', file_path.stem)
                new_filename = f"{prefix}{clean_name}{file_path.suffix}"
                new_path = folder / new_filename
                
                try:
                    file_path.rename(new_path)
                    logger.info(f"Renamed: {file_path.name} -> {new_filename}")
                    found_files.append((str(new_path), new_filename, doc_type))
                except OSError as e:
                    logger.error(f"Failed to rename {file_path}: {e}")
        
        return found_files

    def move_to_quarantine(self, file_path: str):
        """Moves a failed file out of the processing queue."""
        self._move_file(file_path, self.dirs['quarantine'])

    def move_to_archive(self, file_path: str):
        """Moves a successfully processed file."""
        self._move_file(file_path, self.dirs['archive'])

    def _move_file(self, src_path: str, dest_folder: Path):
        try:
            shutil.move(src_path, dest_folder / Path(src_path).name)
        except Exception as e:
            logger.error(f"Error moving file {src_path}: {e}")

    def save_merged_pdf(self, pdf_writer, po_number: str) -> str:
        """
        Saves the final merged document to the Output folder.
        Format: Combined_PO_[Number].pdf
        """
        filename = f"Combined_PO_{po_number}.pdf"
        output_path = self.dirs['output'] / filename
        
        try:
            with open(output_path, "wb") as f:
                pdf_writer.write(f)
            return str(output_path)
        except Exception as e:
            logger.error(f"Failed to save merged PDF {filename}: {e}")
            raise e