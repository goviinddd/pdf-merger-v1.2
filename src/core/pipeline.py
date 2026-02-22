import time
import logging
import os
import shutil
import datetime
import json
import magic
import gc
import re
import hashlib
from typing import List, Dict
from pypdf import PdfWriter, PdfReader

# Import our modules
from .database import DatabaseManager
from .file_utils import FileSystemManager
from ..extractors import get_document_info 

# Try importing YOLO, handle if missing
try:
    from src.extractors.text_extractors.yolo_extractor import YOLOExtractor
    _yolo_extractor = YOLOExtractor()
except ImportError:
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

# Pre-compile regex for performance
QTY_CLEANER = re.compile(r"[^\d.]")

# --- HELPER: SAFE FILE MOVE (Fixes WinError 32) ---
def safe_move_file(src, dst, max_retries=5):
    """
    Moves a file with retry logic to handle Windows file locks.
    Only forces garbage collection if a lock is detected.
    """
    if not os.path.exists(src): return False
    
    # Ensure target directory exists
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    
    for attempt in range(max_retries):
        try:
            shutil.move(src, dst)
            return True
        except PermissionError:
            # File is locked. Force GC now (lazy loading) and wait.
            gc.collect()
            time.sleep(1.0)
        except Exception as e:
            logger.error(f"Move failed (Attempt {attempt+1}): {e}")
            time.sleep(1.0)
            
    logger.error(f"Failed to move {src} -> {dst} (File Locked)")
    return False

def calculate_file_hash(file_path):
    """Generates MD5 hash for duplicate detection."""
    try:
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            # Read in chunks to handle large files
            for chunk in iter(lambda: f.read(65536), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception:
        return None

class PipelineOrchestrator:
    def __init__(self):
        self.fs = FileSystemManager()
        db_path = os.getenv("DB_PATH", "merger_state.db")
        self.db = DatabaseManager(db_path) 
        
        # --- ORDERING LOGIC (SI -> DO -> PO) ---
        # 1 = Top of PDF, 3 = Bottom of PDF
        # --- ORDERING LOGIC ---
        # 1 = Top of PDF, 99 = Bottom of PDF
        self.type_priority = {
            # 1. Sales Invoice (Top)
            'si': 1, 'sales_invoice': 1, 'invoice': 1,
            
            # 2. Delivery Note (Middle)
            'do': 2, 'delivery_note': 2, 'delivery': 2,
            
            # 3. Purchase Order (Bottom)
            'po': 3, 'purchase_order': 3,
            
            # 4. Customs / Export / Supporting Docs (ABSOLUTE LAST)
            'customs': 99, 'export': 99, 'other': 99, 'unknown': 99
        }

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
                logger.error(f"Security: {filename} too large. Skipping.")
                return False
                
            # 2. Magic Byte Check
            detected_type = magic.from_file(file_path, mime=True)
            if detected_type != 'application/pdf':
                logger.critical(f"Security: SPOOF DETECTED! {filename} is actually '{detected_type}'")
                self._quarantine_file(file_path, f"Security Risk: Fake PDF. Actual type: {detected_type}")
                return False

            return True
        except Exception as e:
            logger.error(f"Security check failed for {filename}: {e}")
            return False
        
    def _step_scan_inputs(self):
        logger.info("Scanning input directories...")
        found_files = self.fs.scan_and_rename()
        new_count = 0
        skipped_dupes = 0
        
        for file_path, filename, doc_type in found_files:
            # --- HASH CHECK ---
            file_hash = calculate_file_hash(file_path)
            if not file_hash:
                logger.warning(f"Could not hash {filename}, skipping.")
                continue

            success, existing_name = self.db.register_file(file_path, filename, doc_type, file_hash)
            
            if success:
                new_count += 1
            elif existing_name:
                # --- 🛡️ THE RESTART FIX: Check if it's the EXACT same file ---
                is_same_file = False
                try:
                    with self.db.connect() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT file_path FROM files WHERE content_hash = ?", (file_hash,))
                        db_result = cursor.fetchone()
                        # If the path on the hard drive perfectly matches the path in the DB, 
                        # it is NOT a duplicate. We just restarted the app.
                        if db_result and db_result[0] == file_path:
                            is_same_file = True
                except:
                    pass

                if is_same_file:
                    # Do absolutely nothing. Leave the file alone so it can be processed.
                    continue
                
                # --- TRUE DUPLICATE DELETION ---
                # If we get here, it means the hashes match but the file paths are DIFFERENT.
                # This is a real duplicate copy.
                logger.warning(f"🗑️ DELETING DUPLICATE: {filename} (Identical to {existing_name})")
                skipped_dupes += 1
                try:
                    os.remove(file_path) 
                except Exception as e:
                    logger.error(f"Failed to delete duplicate {filename}: {e}")
                # --------------------------------------------------------

        if new_count > 0:
            logger.info(f"Registered {new_count} new unique files.")
        if skipped_dupes > 0:
            logger.info(f"Deleted {skipped_dupes} duplicate files.")

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
                for v in raw_output.values():
                    if isinstance(v, list):
                        raw_output = v
                        break
                # If still dict, treat as single item if not empty
                if isinstance(raw_output, dict):
                    raw_output = [raw_output]

        # 3. Final Type Check
        if not isinstance(raw_output, list):
            return []

        # 4. Filter only valid dictionaries
        return [x for x in raw_output if isinstance(x, dict)]

    def _step_process_files(self):
        pending_files = self.db.get_pending_files()
        if not pending_files: return

        # --- MEMORY FIX: PROCESS ALL, BUT CLEAN RAM ---
        files_to_process = pending_files 
        
        logger.info(f"Processing {len(files_to_process)} pending files...")

        for file_path, doc_type, current_status in files_to_process:
            if not os.path.exists(file_path):
                logger.warning(f"File vanished: {file_path}. Marking as FAILED.")
                self.db.update_status(file_path, 'FAILED', error="File Not Found on Disk")
                continue

            if not self._is_safe_pdf(file_path):
                self.db.update_status(file_path, 'FAILED', error="Security Validation Failed")
                continue

            # --- START SAFE PROCESSING ---
            reader = None
            table_crops = None
            all_extracted_items = []
            
            try:
                self.db.update_status(file_path, 'PROCESSING')
                
                # 1. Extract Header Info (PO Number)
                doc_info = get_document_info(file_path, doc_type)

                # --- 🛡️ FIXED: PO SANITY CHECK ---
                if doc_info.po_number:
                    po_check = str(doc_info.po_number).upper().strip().replace("-", "_")
                    
                    # ONLY reject if it specifically STARTS WITH the known invoice codes
                    if po_check.startswith("SIV_RHO") or po_check.startswith("SIV_RAK"):
                        logger.warning(f"Rejected fake PO guess (Invoice code detected): {doc_info.po_number}")
                        doc_info.po_number = None # Kill it so Groq takes over

                # Use Groq AI if the first attempt was missing or rejected
                if not doc_info.po_number:
                    fallback_po = extract_po_number(file_path)
                    if fallback_po:
                        doc_info.po_number = fallback_po
                # -------------------------------

                # 👇 THE MISSING LOGIC GATE: Only proceed if we actually have a PO!
                if doc_info.po_number:
                    self.db.update_status(file_path, 'SUCCESS', po_number=doc_info.po_number)
                    logger.info(f"Solved: {doc_type.upper()} -> PO: {doc_info.po_number}")
                    
                    # 2. MULTI-PAGE LINE ITEM EXTRACTION
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
                                    clean_list = self._sanitize_extractor_output(raw_output)
                                    if clean_list:
                                        items_on_this_page.extend(clean_list)
                                        yolo_success = True

                        # B. Fallback: Full Page Scan
                        if not yolo_success:
                            raw_output = extract_line_items_full_page(file_path, page_index=page_idx)
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
                        
                        final_safe_list = []
                        basename = os.path.basename(file_path)
                        for item in linked_data:
                            if isinstance(item, dict):
                                item['doc_type'] = doc_type 
                                item['source_file'] = basename
                                
                                # --- 🧹 DATA CLEANING: Fix Quantities (OPTIMIZED) ---
                                raw_qty = str(item.get('quantity', '0'))
                                clean_qty = QTY_CLEANER.sub("", raw_qty) # Use pre-compiled regex
                                try:
                                    item['quantity'] = float(clean_qty)
                                except:
                                    item['quantity'] = 0.0
                                # ------------------------------------------------
                                final_safe_list.append(item)
                        
                        if final_safe_list:
                            self.db.save_line_items(final_safe_list)
                            logger.info(f"   + Database updated.")
                    else:
                        logger.warning(f"   Failed to extract items from any page.")
                
                else:
                    # 👇 This else block correctly fires ONLY if YOLO and Groq both failed.
                    error_msg = "No PO Number found after all fallback attempts."
                    self._quarantine_file(file_path, error_msg)
                    self.db.update_status(file_path, 'QUARANTINED', error=error_msg)

            except Exception as e:
                logger.error(f"CRITICAL ERROR processing {file_path}: {e}", exc_info=True)
                self._quarantine_file(file_path, str(e))
                self.db.update_status(file_path, 'QUARANTINED', error=str(e))
            
            finally:
                # --- MEMORY FIX: AGGRESSIVE GARBAGE COLLECTION ---
                if 'reader' in locals(): del reader
                if 'table_crops' in locals(): del table_crops
                if 'all_extracted_items' in locals(): del all_extracted_items
                if 'doc_info' in locals(): del doc_info
                gc.collect()
                
    def _step_merge_documents(self):
        bundles = self.db.get_mergeable_bundles()
        reconciler = Reconciler(self.db)
        
        for po_number, files in bundles.items():
            
            # We strictly require: 1 PO + 1 DO + 1 Invoice to proceed.
            present_types = {f['type'].lower() for f in files}
            
            # Normalization helper
            normalized_types = set()
            for t in present_types:
                if t in ['po', 'purchase_order']: normalized_types.add('po')
                elif t in ['do', 'delivery_note', 'delivery']: normalized_types.add('do')
                elif t in ['si', 'sales_invoice', 'invoice']: normalized_types.add('si')
            
            if not ({'po', 'do', 'si'} <= normalized_types):
                # Calculate missing for log
                missing = []
                if 'po' not in normalized_types: missing.append("PO")
                if 'do' not in normalized_types: missing.append("DO")
                if 'si' not in normalized_types: missing.append("Invoice")
                
                logger.info(f"Waiting for docs {po_number}: Missing {', '.join(missing)}")
                continue  

            # --- SORTING USING NEW PRIORITIES ---
            sorted_files = sorted(files, key=lambda x: self.type_priority.get(x['type'].lower(), 99))

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
                logger.warning(f"Skipping merge for {po_number}: Partial delivery.")
                continue 
            
            if status == "MATCH" and not recon_report.get('line_items'):
                logger.warning(f"Skipping {po_number}: Ghost Match (No items extracted).")
                continue

            # --- NEW 3-WAY MATCH CHECK ---
            if status == "INVOICE_PENDING":
                logger.warning(f"Skipping merge for {po_number}: Invoiced quantity < Received quantity.")
                continue


            try:
                merger = PdfWriter()
                file_paths_used = []

                for file_data in sorted_files:
                    path = file_data['path']
                    filename = os.path.basename(path)
                    
                    if not os.path.exists(path): continue

                    # --- CLEANED UP ITEM VERIFICATION LOGIC ---
                    item_count = 0
                    try:
                        item_count = self.db.get_line_item_count(filename)
                        if item_count == -1:
                            logger.warning(f"Could not verify items for {filename}, skipping filter (safe mode).")
                            item_count = 1 # Force keep
                    except Exception as e:
                         logger.warning(f"verification check failed for {filename}, assuming valid: {e}")
                         item_count = 1

                    if item_count == 0:
                        file_type = file_data.get('type', '').lower()
                        is_delivery_doc = file_type in ['do', 'delivery_note', 'delivery']
                        
                        if is_delivery_doc:
                            is_valid_support_doc = False
                            try:
                                reader = PdfReader(path)
                                if len(reader.pages) > 0:
                                    # Fast text extraction on page 1
                                    text = reader.pages[0].extract_text().lower()
                                    support_keywords = [
                                        'received', 'sign', 'stamp', 'weight', 
                                        'courier', 'awb', 'proof of delivery', 
                                        'bol', 'bill of lading', 'driver'
                                    ]
                                    if any(kw in text for kw in support_keywords):
                                        is_valid_support_doc = True
                            except Exception as e:
                                logger.warning(f"Could not read {filename} for ghost check: {e}")
                                
                            if is_valid_support_doc:
                                logger.info(f"Keeping '{filename}' (Valid Supporting Doc / Receipt)")
                            else:
                                logger.warning(f"Skipping '{filename}' (0 items & no supporting keywords. Likely Blank/T&C)")
                                self.db.update_status(path, 'QUARANTINED', error="Skipped: Blank or T&C page")
                                continue # <--- Block the ghost merge
                            # ------------------------------------------------
                        else:
                            # It's 0 items and NOT a Delivery Note. Likely a T&C page or Junk.
                            logger.warning(f"Skipping '{filename}' (File contains 0 line items)")
                            self.db.update_status(path, 'ARCHIVED', error="Skipped: No Items")
                            continue  # <--- Skip this file


                    merger.append(path)
                    file_paths_used.append(path)

                if not file_paths_used: continue

                output_path = self.fs.save_merged_pdf(merger, po_number)
                logger.info(f"MERGED: {po_number} ({len(sorted_files)} docs) -> {output_path}")

                for path in file_paths_used:
                    self.db.update_status(path, 'MERGED')
                    
                    # --- CHANGE 2: DISABLE ARCHIVING (FOR DEBUG) ---
                    # filename = os.path.basename(path)
                    # target = os.path.join(self.archive_folder, filename)
                    # safe_move_file(path, target)
                    logger.info(f"   [DEBUG] Kept source file: {os.path.basename(path)}")
                    # -----------------------------------------------

            except Exception as e:
                logger.error(f"Failed to merge bundle for PO {po_number}: {e}")
                # Add this method at the end of the PipelineOrchestrator class
    def generate_reconciliation_report(self):
        """Generates a full 3-Way Match Excel Report for all active POs."""
        try:
            import pandas as pd
        except ImportError:
            logger.error("Pandas is missing. Run: pip install pandas openpyxl")
            return None

        os.makedirs("reports", exist_ok=True)
        reconciler = Reconciler(self.db)
        
        # Fetch all unique POs from the system
        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT po_number FROM files WHERE po_number IS NOT NULL AND po_number != ''")
            all_pos = [row[0] for row in cursor.fetchall()]

        if not all_pos:
            logger.warning("No POs found in database. Nothing to report.")
            return None

        report_data = []
        for po in all_pos:
            recon = reconciler.reconcile_po(po)
            overall_status = recon.get('overall_status', 'UNKNOWN')
            lines = recon.get('line_items', [])
            
            if not lines:
                report_data.append({
                    "PO Number": po,
                    "Overall Status": overall_status,
                    "Line Ref": "N/A",
                    "Description": recon.get('details', 'No items extracted'),
                    "Ordered Qty": 0,
                    "Received Qty": 0,
                    "Invoiced Qty": 0,
                    "Line Status": "N/A"
                })
            else:
                for line in lines:
                    report_data.append({
                        "PO Number": po,
                        "Overall Status": overall_status,
                        "Line Ref": line.get("Line", ""),
                        "Description": line.get("Desc", ""),
                        "Ordered Qty": line.get("Ordered", 0),
                        "Received Qty": line.get("Received", 0),
                        "Invoiced Qty": line.get("Invoiced", 0),
                        "Line Status": line.get("Status", "")
                    })
                    
        df = pd.DataFrame(report_data)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.abspath(os.path.join("reports", f"Reconciliation_Report_{timestamp}.xlsx"))
        
        df.to_excel(filepath, index=False)
        logger.info(f"📊 Report successfully generated: {filepath}")
        return filepath