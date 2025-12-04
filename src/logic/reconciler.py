import logging
from collections import defaultdict
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class Reconciler:
    def __init__(self, db_manager):
        """
        Initialize with Database ONLY.
        Do NOT pass po_number here.
        """
        self.db = db_manager

    def reconcile_po(self, po_number: str) -> Dict[str, Any]:
        """
        Performs 3-Way Matching for a specific PO Universe.
        Checks for: Purchase Order, Delivery Note, AND Sales Invoice.
        """
        # 1. Fetch all items for this PO
        all_items = self.db.fetch_line_items(po_number)
        
        if not all_items:
            return {
                "po_number": po_number,
                "overall_status": "EMPTY",
                "line_items": [],
                "details": "No line items found."
            }

        # 2. Bucketize by Document Type
        po_ledger = {} 
        dn_ledger = defaultdict(float) 
        si_ledger = defaultdict(float) 

        # We track which documents we have actually seen
        seen_docs = set()

        for item in all_items:
            doc_type = item.get('doc_type', '').lower()
            seen_docs.add(doc_type)

            line_ref = str(item.get('line_ref'))
            
            # Safe float conversion
            try:
                qty = float(item.get('quantity', 0.0))
            except (ValueError, TypeError):
                qty = 0.0
            
            # Normalize Line Ref (remove decimals like '1.0' -> '1')
            if line_ref.endswith('.0'):
                line_ref = line_ref[:-2]

            if doc_type == 'po':
                po_ledger[line_ref] = {
                    "qty": qty,
                    "desc": item.get('description', 'Unknown Item'),
                    "part_no": item.get('part_no', '')
                }
            elif doc_type in ['do', 'dn', 'delivery_note']:
                dn_ledger[line_ref] += qty
            elif doc_type in ['si', 'invoice', 'sales_invoice']:
                si_ledger[line_ref] += qty

        # --- 3-WAY GATEKEEPER ---
        # Check if we have all three necessary pillars.
        # Adjust these keys to match exactly what your 'pipeline.py' saves as 'doc_type'
        # based on your folder names: "Purchase_order", "Delivery_note", "Sales_invoice"
        
        has_po = any(x in seen_docs for x in ['po', 'purchase_order'])
        has_dn = any(x in seen_docs for x in ['do', 'dn', 'delivery_note'])
        has_si = any(x in seen_docs for x in ['si', 'invoice', 'sales_invoice'])

        missing_docs = []
        if not has_po: missing_docs.append("Purchase Order")
        if not has_dn: missing_docs.append("Delivery Note")
        if not has_si: missing_docs.append("Sales Invoice")

        if missing_docs:
            return {
                "po_number": po_number,
                "overall_status": "WAITING_FOR_DOCS",
                "line_items": [],
                "details": f"Cannot merge yet. Missing: {', '.join(missing_docs)}"
            }

        # 3. The Comparison Logic (Only runs if all 3 exist)
        report = []
        universe_status = "MATCH" 

        # Compare against the PO (The Source of Truth)
        for line_ref, po_data in po_ledger.items():
            ordered = po_data['qty']
            received = dn_ledger.get(line_ref, 0.0)
            invoiced = si_ledger.get(line_ref, 0.0)
            
            line_status = "OK"
            
            # Check Delivery vs Order
            if received < ordered:
                line_status = "PARTIAL_DELIVERY"
                universe_status = "INCOMPLETE"
            elif received > ordered:
                line_status = "OVER_DELIVERY"
                universe_status = "ATTENTION"
            
            # Check Invoice vs Received (You shouldn't pay for what you didn't get)
            if invoiced < received:
                line_status = "PARTIAL_INVOICE"
                universe_status = "INCOMPLETE"
            elif invoiced > received:
                line_status = "OVER_INVOICED"
                universe_status = "ATTENTION"

            report.append({
                "Line": line_ref,
                "Description": po_data['desc'],
                "Ordered": ordered,
                "Received": received,
                "Invoiced": invoiced,
                "Status": line_status
            })

        # Check for Unsolicited items (Items delivered but not on PO)
        for line_ref, qty in dn_ledger.items():
            if line_ref not in po_ledger:
                report.append({
                    "Line": line_ref,
                    "Description": "UNKNOWN ITEM (Not in PO)",
                    "Ordered": 0,
                    "Received": qty,
                    "Invoiced": si_ledger.get(line_ref, 0.0),
                    "Status": "UNSOLICITED"
                })
                universe_status = "ATTENTION"

        return {
            "po_number": po_number,
            "overall_status": universe_status,
            "line_items": report
        }