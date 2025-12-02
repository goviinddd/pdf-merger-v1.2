import time
import logging
import os
from dotenv import load_dotenv 
from typing import List, Dict
from pypdf import PdfWriter, PdfReader
import json

# Import our modules
from .database import DatabaseManager
from .file_utils import FileSystemManager
from ..extractors import get_document_info, _yolo_extractor 
from src.extractors.api_connector import extract_line_items_from_crop
from src.logic.linker import link_extracted_data
from src.logic.reconciler import Reconciler

# Setup Logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class PipelineOrchestrator:
    def __init__(self):
        self.fs = FileSystemManager()
        db_path = os.getenv("DB_PATH", "merger_state.db")
        self.db = DatabaseManager(db_path) 
        self.type_priority = {'po': 1, 'do': 2, 'si': 3}

    def run(self):
        logger.info(">>> Starting Pipeline Pass")
        self._step_scan_inputs()
        self._step_process_files()
        self._step_merge_documents()
        logger.info(">>> Pipeline Pass Completed")

    def _step_scan_inputs(self):
        logger.info("Scanning input directories...")
        found_files = self.fs.scan_and_rename()
        new_count = 0
        for file_path, filename, doc_type in found_files:
            if self.db.register_file(file_path, filename, doc_type):
                new_count += 1
        if new_count > 0:
            logger.info(f"Registered {new_count} new files.")

    def _step_process_files(self):
        pending_files = self.db.get_pending_files()
        if not pending_files: return

        logger.info(f"Processing {len(pending_files)} pending files...")

        for file_path, doc_type, current_status in pending_files:
            if not os.path.exists(file_path):
                logger.warning(f"👻 File vanished: {file_path}. Marking as FAILED.")
                self.db.update_status(file_path, 'FAILED', error="File Not Found on Disk")
                continue

            try:
                self.db.update_status(file_path, 'PROCESSING')
                
                # 1. Extract PO Number
                doc_info = get_document_info(file_path, doc_type)

                if doc_info.po_number:
                    self.db.update_status(file_path, 'SUCCESS', po_number=doc_info.po_number)
                    logger.info(f"✓ Solved: {doc_type.upper()} -> PO: {doc_info.po_number}")
                    
                    # 2. Extract Line Items (YOLO Only)
                    if _yolo_extractor:
                        # Scan for tables (This now uses the updated 5-page scan)
                        table_crops = _yolo_extractor.extract_all_table_crops(file_path)
                        
                        all_extracted_items = []
                        if table_crops:
                            for i, crop in enumerate(table_crops):
                                # Send to Cloud API
                                json_str = extract_line_items_from_crop(crop)
                                try:
                                    raw_data = json.loads(json_str)
                                    if raw_data:
                                        all_extracted_items.extend(raw_data)
                                except Exception as e:
                                    logger.error(f"   Failed to parse API JSON: {e}")
                            
                            if all_extracted_items:
                                linked_data = link_extracted_data(doc_info.po_number, all_extracted_items)
                                for item in linked_data:
                                    item['doc_type'] = doc_type
                                
                                self.db.save_line_items(linked_data)
                                logger.info(f"   + Extracted {len(linked_data)} items from {len(table_crops)} pages.")
                            else:
                                logger.warning(f"   YOLO found tables, but Gemini extracted 0 items.")
                        else:
                            logger.warning(f"   No table found by YOLO for {file_path}. Skipping line items.")

                else:
                    self.db.update_status(file_path, 'MANUAL_REVIEW', error="No PO Number found")
                    logger.warning(f"⚠ Failed: Could not identify PO for {file_path}")

            except Exception as e:
                logger.error(f"CRITICAL ERROR processing {file_path}: {e}")
                self.db.update_status(file_path, 'FAILED', error=str(e))

    def _step_merge_documents(self):
        bundles = self.db.get_mergeable_bundles()
        reconciler = Reconciler(self.db)
        
        for po_number, files in bundles.items():
            sorted_files = sorted(
                files, 
                key=lambda x: self.type_priority.get(x['type'], 99)
            )

            # --- RECONCILIATION CHECK ---
            recon_report = reconciler.reconcile_po(po_number)
            status = recon_report.get('overall_status', 'UNKNOWN')
            line_items = recon_report.get('line_items', [])
            
            # 1. Skip if incomplete
            if status == "INCOMPLETE":
                logger.warning(f"🛑 Skipping merge for {po_number}: Partial delivery.")
                continue
            
            # 2. Skip if PO Extraction Failed
            if status == "PO_DATA_MISSING":
                logger.warning(f"🛑 Skipping merge for {po_number}: PO line items were not extracted.")
                continue

            # 3. CRITICAL FIX: Skip "Ghost Matches" (Empty vs Empty)
            if status == "MATCH" and not line_items:
                logger.warning(f"🛑 Skipping merge for {po_number}: No items extracted from ANY document (Ghost Match).")
                continue

            # 4. Warn but allow merge if it's just weird extra items
            if status == "ATTENTION":
                logger.warning(f"⚠️ Merging {po_number} with warnings (Unsolicited items).")

            try:
                merger = PdfWriter()
                file_paths_used = []

                for file_data in sorted_files:
                    path = file_data['path']
                    if not os.path.exists(path): continue
                    merger.append(path)
                    file_paths_used.append(path)

                if not file_paths_used: continue

                output_path = self.fs.save_merged_pdf(merger, po_number)
                logger.info(f"★ MERGED: {po_number} ({len(sorted_files)} docs) -> {output_path}")

                for path in file_paths_used:
                    self.db.update_status(path, 'MERGED')
                    self.fs.move_to_archive(path) 

            except Exception as e:
                logger.error(f"Failed to merge bundle for PO {po_number}: {e}")