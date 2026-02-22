import sqlite3
import os
import json
from datetime import datetime
import logging

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path="merger_state.db"):
        self.db_path = db_path
        self._init_db()

    def connect(self):
        """Returns a raw connection."""
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        """Creates tables if they don't exist."""
        with self.connect() as conn:
            cursor = conn.cursor()
            
            # 1. Files Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT UNIQUE,
                    filename TEXT,
                    content_hash TEXT UNIQUE,
                    doc_type TEXT,
                    po_number TEXT,
                    status TEXT DEFAULT 'PENDING',
                    error_message TEXT,
                    last_updated TIMESTAMP
                )
            ''')

            # 2. Line Items Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS line_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    po_number TEXT,
                    doc_type TEXT,
                    source_file TEXT, 
                    line_ref TEXT,
                    description TEXT,
                    part_no TEXT,
                    quantity REAL,
                    raw_json TEXT
                )
            ''')

            # Migration check for hash column
            cursor.execute("PRAGMA table_info(files)")
            columns = [info[1] for info in cursor.fetchall()]
            if 'content_hash' not in columns:
                logger.info("🔧 Migrating DB: Adding 'content_hash' column...")
                try:
                    cursor.execute("ALTER TABLE files ADD COLUMN content_hash TEXT")
                    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_hash ON files(content_hash)")
                except Exception as e:
                    logger.warning(f"Migration warning: {e}")

    def register_file(self, file_path, filename, doc_type, content_hash):
        """
        Adds a new file to the DB if it doesn't exist.
        Blocks duplicates based on content_hash.
        Returns: (Success: bool, Existing_Filename: str|None)
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            
            # Check if hash already exists
            cursor.execute("SELECT filename FROM files WHERE content_hash = ?", (content_hash,))
            existing = cursor.fetchone()
            if existing:
                return False, existing[0] 

            # Check if path already exists
            cursor.execute("SELECT id FROM files WHERE file_path = ?", (file_path,))
            if cursor.fetchone() is None:
                cursor.execute('''
                    INSERT INTO files (file_path, filename, doc_type, content_hash, status, last_updated)
                    VALUES (?, ?, ?, ?, 'PENDING', ?)
                ''', (file_path, filename, doc_type, content_hash, datetime.now()))
                return True, None
            return False, None

    def get_pending_files(self):
        """Returns files that need processing."""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_path, doc_type, status FROM files WHERE status IN ('PENDING', 'FAILED')")
            return cursor.fetchall()

    def update_status(self, file_path, status, po_number=None, error=None, new_type=None):
        updates = ["status = ?", "last_updated = ?"]
        params = [status, datetime.now()]

        if po_number:
            updates.append("po_number = ?")
            params.append(po_number)
        
        if error:
            updates.append("error_message = ?")
            params.append(error)

        if new_type:
            updates.append("doc_type = ?")
            params.append(new_type)
            
        params.append(file_path)
        sql = f"UPDATE files SET {', '.join(updates)} WHERE file_path = ?"

        with self.connect() as conn:
            conn.execute(sql, params)

    def save_line_items(self, items):
        if not items: return
        
        data_to_insert = []
        for item in items:
            data_to_insert.append((
                item.get('po_number'),
                item.get('doc_type'),
                item.get('source_file', ''),
                item.get('line_ref'),
                item.get('description'),
                item.get('part_no'),
                item.get('quantity', 0.0),
                json.dumps(item)
            ))

        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT INTO line_items (po_number, doc_type, source_file, line_ref, description, part_no, quantity, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)

    def fetch_line_items(self, po_number):
        with self.connect() as conn:
            conn.row_factory = sqlite3.Row 
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM line_items WHERE po_number = ?", (po_number,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_line_item_count(self, filename):
        try:
            with self.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM line_items WHERE source_file=?", (filename,))
                result = cursor.fetchone()
                return result[0] if result else 0
        except Exception:
            return -1 

    # --- COUNTER METHODS FOR GUI ---

    def get_pending_count(self):
        """Counts files waiting to be processed."""
        try:
            with self.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM files WHERE status IN ('PENDING', 'FAILED')")
                result = cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f"DB Error (Pending Count): {e}")
            return 0

    def get_merged_count(self):
        """Counts how many files have been successfully merged."""
        try:
            with self.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM files WHERE status='MERGED'")
                result = cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f"DB Error (Merged Count): {e}")
            return 0

    def get_quarantined_count(self):
        """Counts how many files are in quarantine."""
        try:
            with self.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM files WHERE status='QUARANTINED'")
                result = cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f"DB Error (Quarantined Count): {e}")
            return 0
        
    def get_mergeable_bundles(self):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT po_number, file_path, doc_type 
                FROM files 
                WHERE po_number IS NOT NULL AND status = 'SUCCESS'
            """)
            rows = cursor.fetchall()

        bundles = {}
        for po, path, dtype in rows:
            if po not in bundles: bundles[po] = []
            bundles[po].append({"path": path, "type": dtype})
            
        return bundles