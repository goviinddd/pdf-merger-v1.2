import os
import sqlite3
import hashlib
import logging
from collections import defaultdict

# Setup clean logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("AUDIT")

DB_PATH = "merger_state.db"
INPUT_FOLDERS = ["Purchase_order", "Delivery_note", "Sales_invoice"]

def get_file_hash(file_path):
    """Calculates MD5 hash of a file to find exact duplicates."""
    try:
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            buf = f.read(65536)
            while len(buf) > 0:
                hasher.update(buf)
                buf = f.read(65536)
        return hasher.hexdigest()
    except Exception:
        return None

def check_physical_duplicates():
    logger.info("\n🔍 --- STEP 1: PHYSICAL DUPLICATE CHECK ---")
    hashes = defaultdict(list)
    
    total_files = 0
    for folder in INPUT_FOLDERS:
        if not os.path.exists(folder): continue
        for root, _, files in os.walk(folder):
            for file in files:
                if not file.lower().endswith('.pdf'): continue
                path = os.path.join(root, file)
                file_hash = get_file_hash(path)
                if file_hash:
                    hashes[file_hash].append(path)
                    total_files += 1

    duplicates = {k: v for k, v in hashes.items() if len(v) > 1}
    
    if not duplicates:
        logger.info("✅ No physical duplicates found.")
    else:
        logger.warning(f"❌ FOUND {len(duplicates)} SETS OF DUPLICATES (Causes Over-Shipping):")
        for h, paths in duplicates.items():
            logger.info(f"   Hash {h[:8]} appears {len(paths)} times:")
            for p in paths:
                logger.info(f"      - {p}")
    
    return total_files

def check_database_integrity():
    logger.info("\n🧠 --- STEP 2: DATABASE & LOGIC AUDIT ---")
    
    if not os.path.exists(DB_PATH):
        logger.error("❌ Database not found! Run the pipeline first.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Check Misclassification (Identity Crisis)
    logger.info("... Checking for Classification Mismatches")
    cursor.execute("SELECT file_path, doc_type FROM files")
    mismatches = []
    for row in cursor.fetchall():
        path = row['file_path']
        db_type = row['doc_type'].lower()
        
        # Infer type from folder name
        expected = "unknown"
        if "Purchase_order" in path: expected = "purchase_order"
        elif "Delivery_note" in path: expected = "delivery_note"
        elif "Sales_invoice" in path: expected = "sales_invoice"
        
        # Normalize DB type for comparison
        normalized_db = db_type
        if db_type == "po": normalized_db = "purchase_order"
        if db_type == "do": normalized_db = "delivery_note"
        if db_type == "si": normalized_db = "sales_invoice"

        if expected != "unknown" and normalized_db != expected:
            mismatches.append(f"{path} (Folder: {expected} != DB: {db_type})")

    if mismatches:
        logger.warning(f"❌ Found {len(mismatches)} Misclassified Files (Auto-Correct gone wrong):")
        for m in mismatches[:10]: logger.info(f"   - {m}")
        if len(mismatches) > 10: logger.info(f"   ... and {len(mismatches)-10} more.")
    else:
        logger.info("✅ No classification mismatches found.")

    # 2. Check Ghost Files (Success but 0 items)
    logger.info("\n... Checking for Ghost Extractions (Empty POs)")
    cursor.execute("""
        SELECT f.filename, f.status, COUNT(l.id) as item_count 
        FROM files f 
        LEFT JOIN line_items l ON f.filename = l.source_file 
        WHERE f.status = 'SUCCESS' 
        GROUP BY f.filename
    """)
    
    ghosts = []
    for row in cursor.fetchall():
        if row['item_count'] == 0:
            ghosts.append(row['filename'])
            
    if ghosts:
        logger.warning(f"❌ Found {len(ghosts)} Ghost Files (Marked SUCCESS but 0 items extracted):")
        for g in ghosts: logger.info(f"   - {g}")
    else:
        logger.info("✅ No Ghost files found.")

    conn.close()

if __name__ == "__main__":
    print("=========================================")
    print("   NEXUS SYSTEM DIAGNOSTIC TOOL v1.0   ")
    print("=========================================")
    
    check_physical_duplicates()
    check_database_integrity()
    
    print("\n=========================================")
    print("           AUDIT COMPLETE              ")
    print("=========================================")