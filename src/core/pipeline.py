import time
import logging
import os
import shutil
import datetime
import json
import magic
from typing import List, Dict
from pypdf import PdfWriter, PdfReader

# Import our modules
from .database import DatabaseManager
from .file_utils import FileSystemManager
from ..extractors import get_document_info, _yolo_extractor 
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

    # --- QUARANTINE SINGLE FILE (Corruption/No PO) ---
    def _quarantine_file(self, file_path, error_msg):
        """Moves a single problematic file to quarantine."""
        quarantine_base = "quarantine"
        os.makedirs(quarantine_base, exist_ok=True)

        filename = os.path.basename(file_path)
        destination_pdf = os.path.join(quarantine_base, filename)

        if os.path.exists(destination_pdf):
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            name, ext = os.path.splitext(filename)
            destination_pdf = os.path.join(quarantine_base, f"{name}_{timestamp}{ext}")

        try:
            shutil.move(file_path, destination_pdf)
            logger.warning(f"MOVED TO QUARANTINE: {filename}")
            
            log_path = os.path.splitext(destination_pdf)[0] + ".txt"
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"File: {filename}\nError: {error_msg}\n")
            return True
        except Exception as e:
            logger.error(f"Failed to quarantine {filename}: {e}")
            return False

    # --- NEW: QUARANTINE BUNDLE (Logic Mismatch) ---
    def _quarantine_bundle(self, po_number, files, reason):
        """
        Moves ALL files associated with a PO (PO, DO, Invoice) to a dedicated folder.
        Used when documents exist but content contradicts (e.g. 3-1 vs Line Item 3).
        """
        # Create a folder specifically for this mismatch
        folder_name = f"MISMATCH_{po_number}_{datetime.datetime.now().strftime('%H%M%S')}"
        quarantine_path = os.path.join("quarantine", folder_name)
        os.makedirs(quarantine_path, exist_ok=True)
        
        logger.warning(f"Quarantining Bundle {po_number} -> {folder_name}")

        # 1. Create Explaination File
        with open(os.path.join(quarantine_path, "DISCREPANCY_REPORT.txt"), "w") as f:
            f.write(f"PO Number: {po_number}\n")
            f.write(f"Reason: {reason}\n")
            f.write("-" * 30 + "\n")
            f.write("Files moved:\n")
            for file_data in files:
                f.write(f"- {os.path.basename(file_data['path'])}\n")

        # 2. Move all files in the bundle
        for file_data in files:
            src = file_data['path']
            if os.path.exists(src):
                dst = os.path.join(quarantine_path, os.path.basename(src))
                try:
                    shutil.move(src, dst)
                    # Update DB to stop processing this file
                    self.db.update_status(src, 'QUARANTINED', error=f"Bundle Mismatch: {reason}")
                except Exception as e:
                    logger.error(f"Failed to move {src} to bundle quarantine: {e}")

    def _is_safe_pdf(self, file_path):
        """
        Validates file integrity using Magic Bytes (Hex Signature).
        Prevents processing of renamed .exe files or other malicious files.
        """
        filename = os.path.basename(file_path)
        
        try:
            # 1. Size Check (DoS Protection)
            # Limit to 50MB (Adjust if you deal with massive blueprints)
            max_size_mb = 50
            file_size = os.path.getsize(file_path)
            
            if file_size > max_size_mb * 1024 * 1024:
                logger.error(f"⛔ Security: {filename} is too large ({file_size/1024/1024:.2f} MB). Skipping.")
                return False
            # 2. Magic Byte Check (Anti-Spoofing)
            # Reads the actual file header bits, ignoring the extension
            detected_type = magic.from_file(file_path, mime=True)
            
            if detected_type != 'application/pdf':
                logger.critical(f"Security: SPOOF DETECTED! {filename} is actually '{detected_type}', not a PDF.")
                
                self._quarantine_file(file_path, f"Security Risk: Fake PDF. Actual type: {detected_type}")
                return False

            return True

        except Exception as e:
            logger.error(f"❌ Security check failed for {filename}: {e}")
            return False
        
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

            if not self._is_safe_pdf(file_path):
                self.db.update_status(file_path, 'FAILED', error="Security Validation Failed")
                continue

            # --- AUTO-CORRECTION LOGIC ---
            detected_type = classify_document_type(file_path)
            folder_type_normalized = doc_type.lower().replace(" ", "_")
            if folder_type_normalized in TYPE_ALIASES:
                folder_type_normalized = TYPE_ALIASES[folder_type_normalized]

            if detected_type != "unknown" and detected_type != folder_type_normalized:
                if detected_type == "purchase_order": doc_type = "Purchase_order"
                elif detected_type == "delivery_note": doc_type = "Delivery_note"
                elif detected_type == "sales_invoice": doc_type = "Sales_invoice"
                logger.info(f"   >>> Auto-Correcting type to: {doc_type}")

            # --- START SAFE PROCESSING ---
            try:
                self.db.update_status(file_path, 'PROCESSING')
                
                # 1. Extract Header Info (PO Number) - Usually fine on Page 1
                doc_info = get_document_info(file_path, doc_type)

                # Fallback for PO Number
                if not doc_info.po_number:
                    fallback_po = extract_po_number(file_path)
                    if fallback_po:
                        doc_info.po_number = fallback_po

                if doc_info.po_number:
                    self.db.update_status(file_path, 'SUCCESS', po_number=doc_info.po_number)
                    logger.info(f"Solved: {doc_type.upper()} -> PO: {doc_info.po_number}")
                    
                    # 2. MULTI-PAGE LINE ITEM EXTRACTION
                    # We need to scan ALL pages, not just the first one.
                    
                    all_extracted_items = []
                    
                    try:
                        reader = PdfReader(file_path)
                        total_pages = len(reader.pages)
                        logger.info(f"   Scanning {total_pages} page(s) for line items...")
                    except:
                        total_pages = 1 # Fallback if pypdf fails to read
                    
                    # Loop through every page
                    for page_idx in range(total_pages):
                        page_num = page_idx + 1
                        logger.debug(f"     -> Processing Page {page_num}/{total_pages}...")
                        
                        items_on_this_page = []

                        # A. Try YOLO Table Extraction for this page
                        if _yolo_extractor:
                            # Note: Ensure your extractor accepts 'page_index' or 'page_number'
                            # If not, you might need to update the extractor to use pdf2image with first_page=page_num, last_page=page_num
                            table_crops = _yolo_extractor.extract_all_table_crops(file_path, page_index=page_idx)
                            
                            if table_crops:
                                logger.info(f"     [Page {page_num}] YOLO found {len(table_crops)} tables.")
                                for crop in table_crops:
                                    json_str = extract_line_items_from_crop(crop)
                                    try:
                                        raw_data = json.loads(json_str)
                                        if raw_data: items_on_this_page.extend(raw_data)
                                    except: pass
                            else:
                                # B. Fallback: Full Page Scan for this page
                                # Only run fallback if YOLO found nothing on this page
                                logger.debug(f"     [Page {page_num}] No tables via YOLO. Trying Gemini Full-Scan...")
                                json_str = extract_line_items_full_page(file_path, page_index=page_idx)
                                try:
                                    raw_data = json.loads(json_str)
                                    if raw_data: 
                                        items_on_this_page.extend(raw_data)
                                        logger.info(f"     [Page {page_num}] Gemini found {len(raw_data)} items.")
                                except: pass
                        
                        # Add items found on this page to the master list
                        if items_on_this_page:
                            all_extracted_items.extend(items_on_this_page)

                    # 3. Save Consolidated Results
                    if all_extracted_items:
                        logger.info(f"   Total Items Extracted (All Pages): {len(all_extracted_items)}")
                        linked_data = link_extracted_data(doc_info.po_number, all_extracted_items)
                        for item in linked_data:
                            item['doc_type'] = doc_type 
                        
                        self.db.save_line_items(linked_data)
                        logger.info(f"   + Database updated.")
                    else:
                        logger.warning(f"   Failed to extract items from any page.")
                else:
                    error_msg = "No PO Number found after all fallback attempts."
                    self._quarantine_file(file_path, error_msg)
                    self.db.update_status(file_path, 'QUARANTINED', error=error_msg)

            except Exception as e:
                logger.error(f"CRITICAL ERROR processing {file_path}: {e}")
                self._quarantine_file(file_path, str(e))
                self.db.update_status(file_path, 'QUARANTINED', error=str(e))

    def _step_merge_documents(self):
        bundles = self.db.get_mergeable_bundles()
        reconciler = Reconciler(self.db)
        
        for po_number, files in bundles.items():
            sorted_files = sorted(files, key=lambda x: self.type_priority.get(x['type'], 99))

            # --- RECONCILIATION ---
            recon_report = reconciler.reconcile_po(po_number)
            status = recon_report.get('overall_status', 'UNKNOWN')
            
            # 1. Skip if waiting for docs
            if status == "WAITING_FOR_DOCS":
                continue

            # 2. Logic Mismatch Check (e.g. 3-1 vs Line Item 3)
            if status == "MISMATCH" or status == "DATA_DISCREPANCY":
                reason = recon_report.get('details', 'Line Item Mismatch detected')
                self._quarantine_bundle(po_number, sorted_files, reason)
                continue

            if status == "INCOMPLETE":
                logger.warning(f"⛔ Skipping merge for {po_number}: Partial delivery ({recon_report.get('details')}).")
                continue 
            
            if status == "MATCH" and not recon_report.get('line_items'):
                logger.warning(f"Skipping {po_number}: Ghost Match (No items extracted).")
                continue

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
                # Optional: Quarantine on merge crash?
                # self._quarantine_bundle(po_number, sorted_files, f"Merge Crash: {e}")