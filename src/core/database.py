import sqlite3
import os
import json
from datetime import datetime

class DatabaseManager:
    def __init__(self, db_path="merger_state.db"):
        self.db_path = db_path
        self._init_db()

    def connect(self):
        """Returns a raw connection (Used by Reporter)"""
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        """Creates tables if they don't exist."""
        conn = self.connect()
        cursor = conn.cursor()
        
        # 1. Files Table (Tracks status of every PDF)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE,
                filename TEXT,
                doc_type TEXT,
                po_number TEXT,
                status TEXT DEFAULT 'PENDING',
                error_message TEXT,
                last_updated TIMESTAMP
            )
        ''')

        # 2. Line Items Table (Stores extracted data)
        # ADDED 'source_file' COLUMN HERE
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
        
        conn.commit()
        conn.close()

    def register_file(self, file_path, filename, doc_type):
        """Adds a new file to the DB if it doesn't exist."""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM files WHERE file_path = ?", (file_path,))
            if cursor.fetchone() is None:
                cursor.execute('''
                    INSERT INTO files (file_path, filename, doc_type, status, last_updated)
                    VALUES (?, ?, ?, 'PENDING', ?)
                ''', (file_path, filename, doc_type, datetime.now()))
                conn.commit()
                return True
            return False
        finally:
            conn.close()

    def get_pending_files(self):
        """Returns files that need processing."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT file_path, doc_type, status FROM files WHERE status IN ('PENDING', 'FAILED')")
        rows = cursor.fetchall()
        conn.close()
        return rows

    def update_status(self, file_path, status, po_number=None, error=None):
        """Updates the status of a file."""
        conn = self.connect()
        cursor = conn.cursor()
        
        updates = ["status = ?", "last_updated = ?"]
        params = [status, datetime.now()]

        if po_number:
            updates.append("po_number = ?")
            params.append(po_number)
        
        if error:
            updates.append("error_message = ?")
            params.append(error)
            
        params.append(file_path) # For WHERE clause

        sql = f"UPDATE files SET {', '.join(updates)} WHERE file_path = ?"
        cursor.execute(sql, params)
        conn.commit()
        conn.close()

    def save_line_items(self, items):
        """Bulk saves extracted line items."""
        if not items: return
        conn = self.connect()
        cursor = conn.cursor()
        
        for item in items:
            # UPDATED INSERT STATEMENT TO INCLUDE source_file
            cursor.execute('''
                INSERT INTO line_items (po_number, doc_type, source_file, line_ref, description, part_no, quantity, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item.get('po_number'),
                item.get('doc_type'),
                item.get('source_file', ''), # Save the filename here
                item.get('line_ref'),
                item.get('description'),
                item.get('part_no'),
                item.get('quantity', 0.0),
                json.dumps(item)
            ))
        conn.commit()
        conn.close()

    def fetch_line_items(self, po_number):
        """Retrieves all items for a specific PO universe."""
        conn = self.connect()
        conn.row_factory = sqlite3.Row 
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM line_items WHERE po_number = ?", (po_number,))
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return rows
    
    def get_line_item_count(self, filename):
        """Returns the number of line items associated with a specific file."""
        try:
            # FIX: Open a fresh connection instead of looking for self.conn
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM line_items WHERE source_file=?", (filename,))
            result = cursor.fetchone()
            count = result[0] if result else 0
            conn.close()
            return count
        except Exception as e:
            print(f"⚠️ DB Count Error: {e}")
            return -1 

    def get_mergeable_bundles(self):
        """
        Finds PO numbers that have files ready to merge.
        Returns: { "PO-123": [ {path, type}, ... ] }
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("SELECT po_number, file_path, doc_type FROM files WHERE po_number IS NOT NULL")
        rows = cursor.fetchall()
        conn.close()

        bundles = {}
        for po, path, dtype in rows:
            if po not in bundles: bundles[po] = []
            bundles[po].append({"path": path, "type": dtype})
            
        return bundles