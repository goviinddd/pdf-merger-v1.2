import time
import logging
import os
import shutil
import datetime
import json
import magic
import gc  # Added for garbage collection
import re  # Added for quantity cleaning
from typing import List, Dict
from pypdf import PdfWriter, PdfReader

# Import our modules
from .database import DatabaseManager
from .file_utils import FileSystemManager
from ..extractors import get_document_info 
# Try importing YOLO, handle if missing
try:
    from src.extractors.vision_extractors.yolo_extractor import YOLOExtractor
    _yolo_extractor = YOLOExtractor()
except:
    _yolo_extractor = None

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

# --- HELPER: SAFE FILE MOVE (Fixes WinError 32) ---
def safe_move_file(src, dst, max_retries=5):
    """
    Moves a file with retry logic to handle Windows file locks.
    Forces garbage collection to release dangling handles.
    """
    if not os.path.exists(src): return False
    
    # Force Garbage Collection to close dangling file handles
    gc.collect()
    
    # Ensure target directory exists
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    
    for attempt in range(max_retries):
        try:
            shutil.move(src, dst)
            return True
        except PermissionError:
            # File is locked. Wait and retry.
            time.sleep(1.0)
        except Exception as e:
            logger.error(f"Move failed (Attempt {attempt+1}): {e}")
            time.sleep(1.0)
            
    logger.error(f"❌ Failed to move {src} -> {dst} (File Locked)")
    return False

class PipelineOrchestrator:
    def __init__(self):
        self.fs = FileSystemManager()
        db_path = os.getenv("DB_PATH", "merger_state.db")
        self.db = DatabaseManager(db_path) 
        self.type_priority = {'po': 1, 'do': 2, 'si': 3}
        self.quarantine_folder = "quarantine"
        self.archive_folder = "archive"
        os.makedirs(self.quarantine_folder, exist_ok=True)
        os.makedirs(self.archive_folder, exist_ok=True)

    def run(self):
        logger.info(">>> Starting Pipeline Pass")
        self._step_scan_inputs()
        self._step_process_files()
        self._step_merge_documents()
        logger.info(">>> Pipeline Pass Completed")

    # --- QUARANTINE SINGLE FILE ---
    def _quarantine_file(self, file_path, reason):
        filename = os.path.basename(file_path)
        target = os.path.join(self.quarantine_folder, filename)
        
        logger.warning(f"Quarantining {filename} -> {reason}")
        
        if not safe_move_file(file_path, target):
            logger.error(f"Could not move {filename} (System Lock)")

    # --- QUARANTINE BUNDLE ---
    def _quarantine_bundle(self, po_number, files, reason):
        folder_name = f"MISMATCH_{po_number}_{datetime.datetime.now().strftime('%H%M%S')}"
        quarantine_path = os.path.join(self.quarantine_folder, folder_name)
        os.makedirs(quarantine_path, exist_ok=True)
        
        logger.warning(f"Quarantining Bundle {po_number} -> {folder_name}")

        # 1. Create Explanation File
        with open(os.path.join(quarantine_path, "DISCREPANCY_REPORT.txt"), "w") as f:
            f.write(f"PO Number: {po_number}\n")
            f.write(f"Reason: {reason}\n")
            f.write("-" * 30 + "\n")
            f.write("Files moved:\n")
            for file_data in files:
                f.write(f"- {os.path.basename(file_data['path'])}\n")

        # 2. Move all files using Safe Move
        for file_data in files:
            src = file_data['path']
            if os.path.exists(src):
                dst = os.path.join(quarantine_path, os.path.basename(src))
                if safe_move_file(src, dst):
                    self.db.update_status(src, 'QUARANTINED', error=f"Bundle Mismatch: {reason}")

    def _is_safe_pdf(self, file_path):
        filename = os.path.basename(file_path)
        try:
            # 1. Size Check (50MB Limit)
            max_size_mb = 50
            file_size = os.path.getsize(file_path)
            if file_size > max_size_mb * 1024 * 1024:
                logger.error(f"⛔ Security: {filename} too large. Skipping.")
                return False
                
            # 2. Magic Byte Check
            detected_type = magic.from_file(file_path, mime=True)
            if detected_type != 'application/pdf':
                logger.critical(f"Security: SPOOF DETECTED! {filename} is actually '{detected_type}'")
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

    def _sanitize_extractor_output(self, raw_output):
        """
        Smart Helper: Turns whatever Groq sends back (str, dict, list) 
        into a clean List of Dictionaries. Handles 'rows', 'data', 'items' keys.
        """
        # 1. Decode JSON string if needed
        if isinstance(raw_output, str):
            try: raw_output = json.loads(raw_output)
            except: return []

        # 2. Unwrap Dictionaries (Groq wrap fix)
        if isinstance(raw_output, dict):
            # Groq often wraps the list in random keys
            for key in ["items", "rows", "table_rows", "data", "result"]:
                if key in raw_output and isinstance(raw_output[key], list):
                    raw_output = raw_output[key]
                    break
            
            # If we still have a dict, maybe it's a single item wrapped?
            if isinstance(raw_output, dict):
                # Last ditch: check values for a list
                found = False
                for v in raw_output.values():
                    if isinstance(v, list):
                        raw_output = v
                        found = True
                        break
                if not found:
                    raw_output = [raw_output] # Treat dict as single item

        # 3. Final Type Check
        if not isinstance(raw_output, list):
            return []

        # 4. Filter only valid dictionaries
        valid_items = [x for x in raw_output if isinstance(x, dict)]
        return valid_items

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
                
                # 1. Extract Header Info (PO Number)
                doc_info = get_document_info(file_path, doc_type)

                if not doc_info.po_number:
                    fallback_po = extract_po_number(file_path)
                    if fallback_po:
                        doc_info.po_number = fallback_po

                if doc_info.po_number:
                    self.db.update_status(file_path, 'SUCCESS', po_number=doc_info.po_number)
                    logger.info(f"Solved: {doc_type.upper()} -> PO: {doc_info.po_number}")
                    
                    # 2. MULTI-PAGE LINE ITEM EXTRACTION
                    all_extracted_items = []
                    
                    try:
                        reader = PdfReader(file_path)
                        total_pages = len(reader.pages)
                        logger.info(f"   Scanning {total_pages} page(s) for line items...")
                    except:
                        total_pages = 1 
                    
                    # Loop through every page
                    for page_idx in range(total_pages):
                        page_num = page_idx + 1
                        items_on_this_page = []

                        # A. Try YOLO Table Extraction
                        yolo_success = False
                        if _yolo_extractor:
                            table_crops = _yolo_extractor.extract_all_table_crops(file_path, page_index=page_idx)
                            
                            if table_crops:
                                logger.info(f"     [Page {page_num}] YOLO found {len(table_crops)} tables.")
                                for crop in table_crops:
                                    raw_output = extract_line_items_from_crop(crop)
                                    
                                    # --- SANITIZATION BLOCK ---
                                    clean_list = self._sanitize_extractor_output(raw_output)
                                    
                                    if clean_list:
                                        items_on_this_page.extend(clean_list)
                                        yolo_success = True

                        # B. Fallback: Full Page Scan
                        if not yolo_success:
                            # logger.debug(f"     [Page {page_num}] No tables via YOLO. Trying Full-Scan...")
                            raw_output = extract_line_items_full_page(file_path, page_index=page_idx)
                            
                            # --- SANITIZATION BLOCK ---
                            clean_list = self._sanitize_extractor_output(raw_output)

                            if clean_list: 
                                items_on_this_page.extend(clean_list)
                                logger.info(f"     [Page {page_num}] Vision found {len(clean_list)} items.")
                        
                        # Add valid items to master list
                        if items_on_this_page:
                            all_extracted_items.extend(items_on_this_page)

                    # 3. Save Consolidated Results
                    if all_extracted_items:
                        logger.info(f"   Total Items Extracted (All Pages): {len(all_extracted_items)}")
                        
                        linked_data = link_extracted_data(doc_info.po_number, all_extracted_items)
                        
                        # --- SAFE ASSIGNMENT (Prevents 'str' crash) ---
                        final_safe_list = []
                        for item in linked_data:
                            if isinstance(item, dict):
                                item['doc_type'] = doc_type 
                                item['source_file'] = os.path.basename(file_path)
                                final_safe_list.append(item)
                        
                        if final_safe_list:
                            # --- 🧹 DATA CLEANING: Fix Quantities (ADDED) ---
                            for item in final_safe_list:
                                raw_qty = str(item.get('quantity', '0'))
                                # Remove everything that isn't a digit or a dot (e.g. "6.00 box" -> "6.00")
                                clean_qty = re.sub(r"[^\d.]", "", raw_qty)
                                try:
                                    # Convert to float
                                    val = float(clean_qty)
                                    item['quantity'] = val
                                except:
                                    item['quantity'] = 0.0
                            # ------------------------------------------------

                            self.db.save_line_items(final_safe_list)
                            logger.info(f"   + Database updated.")
                    else:
                        logger.warning(f"   Failed to extract items from any page.")
                else:
                    error_msg = "No PO Number found after all fallback attempts."
                    self._quarantine_file(file_path, error_msg)
                    self.db.update_status(file_path, 'QUARANTINED', error=error_msg)

            except Exception as e:
                logger.error(f"CRITICAL ERROR processing {file_path}: {e}", exc_info=True)
                self._quarantine_file(file_path, str(e))
                self.db.update_status(file_path, 'QUARANTINED', error=str(e))

    def _step_merge_documents(self):
        bundles = self.db.get_mergeable_bundles()
        reconciler = Reconciler(self.db)
        
        for po_number, files in bundles.items():
            
            # We strictly require: 1 PO + 1 DO + 1 Invoice to proceed.
            
            # 1. Gather all document types present in this bundle
            present_types = {f['type'].lower() for f in files}
            
            # 2. Check for missing critical documents
            # Note: We normalize keys to match 'po', 'do', 'si'
            has_po = 'po' in present_types or 'purchase_order' in present_types
            has_do = 'do' in present_types or 'delivery_note' in present_types or 'delivery' in present_types
            has_si = 'si' in present_types or 'sales_invoice' in present_types or 'invoice' in present_types

            if not (has_po and has_do and has_si):
                # Calculate what is missing for the log
                missing = []
                if not has_po: missing.append("PO")
                if not has_do: missing.append("DO")
                if not has_si: missing.append("Invoice")
                
                logger.info(f"⏳ Waiting for docs {po_number}: Missing {', '.join(missing)}")
                continue  
            # --------------------------------

            sorted_files = sorted(files, key=lambda x: self.type_priority.get(x['type'], 99))

            # ... rest of the code (reconciler, merge logic) ...

            # --- RECONCILIATION ---
            recon_report = reconciler.reconcile_po(po_number)
            status = recon_report.get('overall_status', 'UNKNOWN')
            
            if status == "WAITING_FOR_DOCS": continue

            # Logic Mismatch Check
            if status == "MISMATCH" or status == "DATA_DISCREPANCY":
                reason = recon_report.get('details', 'Line Item Mismatch detected')
                self._quarantine_bundle(po_number, sorted_files, reason)
                continue

            if status == "INCOMPLETE":
                logger.warning(f"⛔ Skipping merge for {po_number}: Partial delivery.")
                continue 
            
            if status == "MATCH" and not recon_report.get('line_items'):
                logger.warning(f"Skipping {po_number}: Ghost Match (No items extracted).")
                continue

            try:
                merger = PdfWriter()
                file_paths_used = []

                for file_data in sorted_files:
                    path = file_data['path']
                    filename = os.path.basename(path)
                    
                    if not os.path.exists(path): continue

                    try:
                        # Use the new method we just added
                        item_count = self.db.get_line_item_count(filename)
                        
                        # If the DB check failed (-1), we skip the filter to be safe
                        if item_count == -1:
                            logger.warning(f"⚠️ Could not verify items for {filename}, skipping filter.")
                            item_count = 1 # Force keep

                        if item_count == 0:
                            # [Rest of the logic stays the same...]
                            # ✅ EXCEPTION: Is this a Delivery Note?
                            file_type = file_data.get('type', '').lower()
                            is_delivery_doc = file_type in ['do', 'delivery_note', 'delivery']
                            
                            if is_delivery_doc:
                                logger.info(f"📎 Keeping '{filename}' (Bill of Lading / Supporting Doc)")
                            else:
                                logger.warning(f"📉 Skipping '{filename}' (File contains 0 line items)")
                                target_archive = os.path.join(self.archive_folder, filename)
                                safe_move_file(path, target_archive)
                                self.db.update_status(path, 'ARCHIVED', error="Skipped: No Items")
                                continue

                    except Exception as e:
                        logger.warning(f"⚠️ verification failed for {filename}, merging anyway: {e}")

                        if item_count == 0:
                            # ✅ EXCEPTION: Is this a Delivery Note?
                            # Courier Receipts (Bill of Lading) often have 0 items but are vital proof.
                            file_type = file_data.get('type', '').lower()
                            is_delivery_doc = file_type in ['do', 'delivery_note', 'delivery']
                            
                            if is_delivery_doc:
                                logger.info(f"📎 Keeping '{filename}' (Bill of Lading / Supporting Doc)")
                                # Fall through to merge
                            else:
                                # It's 0 items and NOT a Delivery Note. Likely a T&C page or Junk.
                                logger.warning(f"📉 Skipping '{filename}' (File contains 0 line items)")
                                target_archive = os.path.join(self.archive_folder, filename)
                                safe_move_file(path, target_archive)
                                self.db.update_status(path, 'ARCHIVED', error="Skipped: No Items")
                                continue  # <--- Skip this file
                    except Exception as e:
                        logger.warning(f"⚠️ verification failed for {filename}, merging anyway: {e}")
                    # -------------------------------------------

                    merger.append(path)
                    file_paths_used.append(path)

                if not file_paths_used: continue

                output_path = self.fs.save_merged_pdf(merger, po_number)
                logger.info(f"MERGED: {po_number} ({len(sorted_files)} docs) -> {output_path}")

                for path in file_paths_used:
                    self.db.update_status(path, 'MERGED')
                    # Use Safe Move for archiving
                    filename = os.path.basename(path)
                    target = os.path.join(self.archive_folder, filename)
                    safe_move_file(path, target)

            except Exception as e:
                logger.error(f"Failed to merge bundle for PO {po_number}: {e}")