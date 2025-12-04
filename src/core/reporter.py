import pandas as pd
import os
from datetime import datetime
import logging
from src.logic.reconciler import Reconciler # We reuse your logic logic

logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self, db_manager):
        self.db = db_manager
        self.output_dir = "reports"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        # Initialize Reconciler to re-run checks for the report
        self.reconciler = Reconciler(db_manager)

    def generate_excel_report(self):
        """
        Generates a 3-Tab "Dashboard" Excel Report.
        1. Document Matrix (What is missing?)
        2. Discrepancy Log (Which items are wrong?)
        3. File Audit (Technical details)
        """
        conn = self.db.connect()
        
        # --- DATA GATHERING ---
        # 1. Get all unique PO numbers
        try:
            po_df = pd.read_sql_query("SELECT DISTINCT po_number FROM files WHERE po_number IS NOT NULL", conn)
            unique_pos = po_df['po_number'].tolist()
        except Exception:
            unique_pos = []

        matrix_data = []      # For Tab 1
        discrepancy_data = [] # For Tab 2

        logger.info(f"Generating detailed report for {len(unique_pos)} POs...")

        for po in unique_pos:
            # Run the logic again to get the "Why"
            recon_result = self.reconciler.reconcile_po(po)
            
            # --- BUILD TAB 1: DOCUMENT MATRIX ---
            status = recon_result.get('overall_status', 'UNKNOWN')
            details = recon_result.get('details', '')
            
            # Helper to parse the "Missing: X, Y" string into a cleaner format
            po_status = "✅ Found"
            dn_status = "✅ Found"
            si_status = "✅ Found"
            
            # If we are waiting, figure out who is the culprit
            if status == "WAITING_FOR_DOCS":
                if "Purchase Order" in details: po_status = "❌ MISSING"
                if "Delivery Note" in details: dn_status = "❌ MISSING"
                if "Sales Invoice" in details: si_status = "❌ MISSING"
                overall_summary = "⚠️ Missing Documents"
            elif status == "MATCH":
                overall_summary = "🟢 Ready to Merge"
            elif status in ["INCOMPLETE", "ATTENTION"]:
                overall_summary = "⚠️ Content Mismatch"
            else:
                overall_summary = status

            matrix_data.append({
                "PO Number": po,
                "Overall Status": overall_summary,
                "Purchase Order": po_status,
                "Delivery Note": dn_status,
                "Sales Invoice": si_status,
                "Detailed Message": details
            })

            # --- BUILD TAB 2: ITEM DISCREPANCIES ---
            # Only look at items if we aren't missing the files entirely
            if status not in ["WAITING_FOR_DOCS", "EMPTY"]:
                items = recon_result.get('line_items', [])
                for item in items:
                    # We only report "Bad" items
                    if item['Status'] != 'OK':
                        discrepancy_data.append({
                            "PO Number": po,
                            "Line Ref": item.get('Line'),
                            "Description": item.get('Description'),
                            "Ordered": item.get('Ordered'),
                            "Received": item.get('Received'),
                            "Invoiced": item.get('Invoiced'),
                            "Issue": self._translate_status(item['Status'])
                        })

        # --- BUILD TAB 3: TECHNICAL FILE LOG ---
        file_audit_df = pd.read_sql_query("SELECT * FROM files", conn)
        conn.close()

        # --- SAVING TO EXCEL ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Reconciliation_Report_{timestamp}.xlsx"
        path = os.path.join(self.output_dir, filename)

        try:
            with pd.ExcelWriter(path, engine='openpyxl') as writer:
                
                # Sheet 1: The Matrix (High Level)
                df_matrix = pd.DataFrame(matrix_data)
                if not df_matrix.empty:
                    # Sort so "Missing" comes first for visibility
                    df_matrix.sort_values(by="Overall Status", inplace=True)
                df_matrix.to_excel(writer, sheet_name='Document Status', index=False)

                # Sheet 2: The Problems (Deep Dive)
                df_disc = pd.DataFrame(discrepancy_data)
                df_disc.to_excel(writer, sheet_name='Item Discrepancies', index=False)

                # Sheet 3: The Raw Data (For IT/Debug)
                file_audit_df.to_excel(writer, sheet_name='System File Log', index=False)
                
            logger.info(f"Detailed Report Generated: {path}")
            return path
            
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
            return None

    def _translate_status(self, code):
        """Translates technical codes to human sentences."""
        mapping = {
            "PARTIAL_DELIVERY": "Short Shipment (Received < Ordered)",
            "OVER_DELIVERY": "Over Shipment (Received > Ordered)",
            "PARTIAL_INVOICE": "Under Invoiced",
            "OVER_INVOICED": "Over Invoiced (Check Price)",
            "UNSOLICITED": "Unordered Item (Not on PO)",
            "OK": "Match"
        }
        return mapping.get(code, code)