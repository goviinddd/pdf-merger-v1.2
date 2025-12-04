import time
import logging
import os
from typing import List, Dict
from pypdf import PdfWriter, PdfReader
import json

# Import our modules
from .database import DatabaseManager
from .file_utils import FileSystemManager
from ..extractors import get_document_info, _yolo_extractor 
from src.extractors.api_connector import extract_line_items_from_crop, extract_po_number
from src.logic.linker import link_extracted_data
from src.logic.reconciler import Reconciler

from src.extractors.api_connector import (
    extract_line_items_from_crop, 
    extract_po_number, 
    extract_line_items_full_page,
    classify_document_type  
)

# Setup Logging
logger = logging.getLogger(__name__)

# Load environment variables

TYPE_ALIASES = {
    "po": "purchase_order",
    "do": "delivery_note",
    "si": "sales_invoice"
}

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
                logger.warning(f"File vanished: {file_path}. Marking as FAILED.")
                self.db.update_status(file_path, 'FAILED', error="File Not Found on Disk")
                continue

            # --- NEW: AUTO-CORRECTION LOGIC ---
            # We check the file type before processing to catch "Traps"
            detected_type = classify_document_type(file_path)
            
            # Note: Your DB stores types as 'po', 'do', 'si' or full names?
            # The classifier returns: 'purchase_order', 'delivery_note', 'sales_invoice'
            # The DB/Folder usually gives: 'Purchase_order', 'Delivery_note', etc.
            # We normalize to lowercase for comparison.
            
            folder_type_normalized = doc_type.lower().replace(" ", "_")
            if folder_type_normalized.lower() in TYPE_ALIASES:

                folder_type_normalized = TYPE_ALIASES[folder_type_normalized.lower()]

            if detected_type != "unknown" and detected_type != folder_type_normalized:
                logger.warning(f" MISMATCH: {os.path.basename(file_path)}")
                logger.warning(f"   Folder says: {doc_type}")
                logger.warning(f"   System says: {detected_type}")
                
                
                # Trust the System -> Update the variable for this run
                # (We map back to the format your DB/Extractors expect)
                if detected_type == "purchase_order": doc_type = "Purchase_order"
                elif detected_type == "delivery_note": doc_type = "Delivery_note"
                elif detected_type == "sales_invoice": doc_type = "Sales_invoice"
                
                logger.info(f"   >>> Auto-Correcting type to: {doc_type}")

            try:
                self.db.update_status(file_path, 'PROCESSING')
                
                # 1. Extract PO Number (Now using the potentially corrected doc_type)
                doc_info = get_document_info(file_path, doc_type)

                # --- FALLBACK LOGIC ---
                if not doc_info.po_number:
                    logger.info(f"YOLO failed to find PO for {os.path.basename(file_path)}. Attempting Gemini Fallback...")
                    fallback_po = extract_po_number(file_path)
                    if fallback_po:
                        doc_info.po_number = fallback_po
                        logger.info(f"Gemini Fallback Success: PO -> {fallback_po}")

                if doc_info.po_number:
                    # Update DB with success
                    self.db.update_status(file_path, 'SUCCESS', po_number=doc_info.po_number)
                    logger.info(f"Solved: {doc_type.upper()} -> PO: {doc_info.po_number}")
                    
                    # 2. Extract Line Items
                    if _yolo_extractor:
                        table_crops = _yolo_extractor.extract_all_table_crops(file_path)
                        all_extracted_items = []
                        
                        if table_crops:
                            logger.info(f"   YOLO found {len(table_crops)} tables. Using Crop Strategy.")
                            for crop in table_crops:
                                json_str = extract_line_items_from_crop(crop)
                                try:
                                    raw_data = json.loads(json_str)
                                    if raw_data: all_extracted_items.extend(raw_data)
                                except Exception as e:
                                    logger.error(f"   Crop JSON parse failed: {e}")
                        else:
                            logger.warning(f"   No table found by YOLO. Switching to Full-Page Gemini Scan...")
                            json_str = extract_line_items_full_page(file_path)
                            try:
                                raw_data = json.loads(json_str)
                                if raw_data: 
                                    all_extracted_items.extend(raw_data)
                                    logger.info(f"   Fallback Success: Extracted {len(raw_data)} items from full page.")
                            except Exception as e:
                                logger.error(f"   Fallback JSON parse failed: {e}")

                        # 3. Save Results
                        if all_extracted_items:
                            linked_data = link_extracted_data(doc_info.po_number, all_extracted_items)
                            for item in linked_data:
                                item['doc_type'] = doc_type # Using corrected type
                            
                            self.db.save_line_items(linked_data)
                            logger.info(f"   + Database updated with {len(linked_data)} items.")
                        else:
                            logger.warning(f"   Failed to extract items via both YOLO and Fallback.")
                else:
                    self.db.update_status(file_path, 'MANUAL_REVIEW', error="No PO Number found")
                    logger.warning(f"Failed: Could not identify PO for {file_path}")

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
            
            # 1. Skip if documents are missing (The 3-Way Rule)
            if status == "WAITING_FOR_DOCS":
                logger.info(f"Skipping {po_number}: {recon_report.get('details')}")
                continue

            # 2. Skip if incomplete
            if status == "INCOMPLETE":
                logger.warning(f"Skipping merge for {po_number}: Partial delivery.")
                continue
            
            # 3. Skip if PO Extraction Failed
            if status == "PO_DATA_MISSING":
                logger.warning(f"Skipping merge for {po_number}: PO line items were not extracted.")
                continue

            # 4. CRITICAL FIX: Skip "Ghost Matches" (Empty vs Empty)
            if status == "MATCH" and not line_items:
                logger.warning(f"Skipping merge for {po_number}: No items extracted from ANY document (Ghost Match).")
                continue

            # 5. Warn but allow merge if it's just weird extra items
            if status == "ATTENTION":
                logger.warning(f"Merging {po_number} with warnings (Unsolicited items).")

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
                logger.info(f"MERGED: {po_number} ({len(sorted_files)} docs) -> {output_path}")

                for path in file_paths_used:
                    self.db.update_status(path, 'MERGED')
                    self.fs.move_to_archive(path) 

            except Exception as e:
                logger.error(f"Failed to merge bundle for PO {po_number}: {e}")
    