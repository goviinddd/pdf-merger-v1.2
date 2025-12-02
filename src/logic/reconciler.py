import logging
from collections import defaultdict
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class Reconciler:
    def __init__(self, db_manager):
        self.db = db_manager

    def reconcile_po(self, po_number: str) -> Dict[str, Any]:
        """
        Performs 3-Way Matching for a specific PO Universe.
        Returns a detailed status report.
        """
        # 1. Fetch all items for this Universe
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

        for item in all_items:
            doc_type = item.get('doc_type', '').lower()
            line_ref = str(item.get('line_ref'))
            # Safe float conversion
            try:
                qty = float(item.get('quantity', 0.0))
            except:
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
            elif doc_type in ['do', 'dn']:
                dn_ledger[line_ref] += qty
            elif doc_type == 'si':
                si_ledger[line_ref] += qty

        # --- CRITICAL FIX: CIRCUIT BREAKER ---
        # If we found items for DN/SI but NO items for PO, it means PO extraction failed.
        # We cannot match against an empty list.
        if not po_ledger and (dn_ledger or si_ledger):
            return {
                "po_number": po_number,
                "overall_status": "PO_DATA_MISSING",
                "line_items": [],
                "details": "Delivery/Invoice exists, but PO line items are missing."
            }

        # 3. The Comparison Logic
        report = []
        universe_status = "MATCH" 

        # Iterate through what was ORDERED (The Truth)
        for line_ref, po_data in po_ledger.items():
            ordered = po_data['qty']
            received = dn_ledger.get(line_ref, 0.0)
            invoiced = si_ledger.get(line_ref, 0.0)
            
            line_status = "OK"
            
            # Check Delivery
            if received < ordered:
                line_status = "PARTIAL_DELIVERY"
                universe_status = "INCOMPLETE"
            elif received > ordered:
                line_status = "OVER_DELIVERY"
                universe_status = "ATTENTION" 
            
            report.append({
                "Line": line_ref,
                "Description": po_data['desc'],
                "Ordered": ordered,
                "Received": received,
                "Invoiced": invoiced,
                "Status": line_status
            })

        # Check for "Orphan" deliveries (Unsolicited)
        for line_ref, qty in dn_ledger.items():
            if line_ref not in po_ledger:
                report.append({
                    "Line": line_ref,
                    "Description": "UNKNOWN ITEM (Not in PO)",
                    "Ordered": 0,
                    "Received": qty,
                    "Invoiced": si_ledger.get(line_ref, 0),
                    "Status": "UNSOLICITED"
                })
                universe_status = "ATTENTION"

        return {
            "po_number": po_number,
            "overall_status": universe_status,
            "line_items": report
        }