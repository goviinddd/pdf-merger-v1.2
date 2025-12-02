# The Database Layer
import sqlite3
import logging
from datetime import datetime
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        """
        Initializes the tables if they don't exist.
        """
        # 1. Main Files Table
        query_files = """
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT UNIQUE NOT NULL,
            filename TEXT NOT NULL,
            doc_type TEXT NOT NULL,
            status TEXT DEFAULT 'PENDING',
            po_number TEXT,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # 2. Line Items Table
        query_items = """
        CREATE TABLE IF NOT EXISTS line_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number TEXT,
            doc_type TEXT,
            line_ref TEXT,
            description TEXT,
            part_no TEXT,
            quantity REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        with self._get_connection() as conn:
            conn.execute(query_files)
            conn.execute(query_items)
            # Create indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_po_number ON files(po_number);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_po ON line_items(po_number);")

    def register_file(self, file_path: str, filename: str, doc_type: str) -> bool:
        """Adds a new file to the queue. Returns False if it already exists."""
        try:
            with self._get_connection() as conn:
                conn.execute(
                    "INSERT INTO files (file_path, filename, doc_type) VALUES (?, ?, ?)",
                    (file_path, filename, doc_type)
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def update_status(self, file_path: str, status: str, po_number: Optional[str] = None, error: Optional[str] = None):
        """Updates the state of a file."""
        query = """
        UPDATE files 
        SET status = ?, po_number = COALESCE(?, po_number), error_message = ?, updated_at = ?
        WHERE file_path = ?
        """
        with self._get_connection() as conn:
            conn.execute(query, (status, po_number, error, datetime.now(), file_path))

    def get_pending_files(self) -> List[Tuple[str, str, str]]:
        """Fetches the next batch of work."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT file_path, doc_type, status FROM files WHERE status = 'PENDING'"
            )
            return cursor.fetchall()

    def get_mergeable_bundles(self):
        """Finds all PO numbers that have a complete set of documents."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT po_number, file_path, doc_type FROM files WHERE status = 'SUCCESS' AND po_number IS NOT NULL"
            )
            rows = cursor.fetchall()
        
        bundles = {}
        for po, path, type_ in rows:
            if po not in bundles:
                bundles[po] = []
            bundles[po].append({'path': path, 'type': type_})
        
        return bundles

    def save_line_items(self, items: list):
        """
        Batch inserts extracted line items.
        """
        if not items: return
        
        query = """
        INSERT INTO line_items (po_number, doc_type, line_ref, description, part_no, quantity)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        data = [
            (
                i['po_number'], 
                i.get('doc_type', 'UNKNOWN'), 
                i.get('line_ref'), 
                i.get('description'), 
                i.get('part_no'), 
                i.get('quantity')
            ) 
            for i in items
        ]
        
        try:
            with self._get_connection() as conn:
                conn.executemany(query, data)
        except Exception as e:
            logger.error(f"Failed to save line items: {e}")

    def fetch_line_items(self, po_number: str) -> List[dict]:
        """
        Returns all line items associated with a PO Number.
        Used by the Reconciler to validate bundles.
        """
        query = "SELECT * FROM line_items WHERE po_number = ?"
        try:
            with self._get_connection() as conn:
                # Use row_factory to get dict-like objects
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(query, (po_number,))
                rows = cursor.fetchall()
                # Convert to standard list of dicts
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to fetch items for {po_number}: {e}")
            return []