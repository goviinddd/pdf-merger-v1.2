import logging
import re
from collections import defaultdict
from typing import List, Dict, Any
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

class Reconciler:
    def __init__(self, db_manager):
        self.db = db_manager

    def _normalize_key(self, raw_ref: str) -> str:
        """
        Standardizes line numbers.
        '3-1' -> '3'
        'Line Item 3' -> '3'
        """
        if not raw_ref: return "UNKNOWN"
        s = str(raw_ref).strip().upper()
        
        # Handle 3-1 or 3.0
        if '-' in s:
            parts = s.split('-')
            if parts[0].strip().isdigit(): return parts[0].strip()
        if '.' in s:
             parts = s.split('.')
             if parts[0].strip().isdigit(): return parts[0].strip()

        # Regex for "Item 3"
        match = re.search(r'(\d+)', s)
        if match: return match.group(1)
        return s

    def _strings_are_similar(self, str1, str2, threshold=0.35):
        """
        Returns True if strings are roughly similar.
        Threshold 0.35 means they need to share ~35% of characters/structure.
        """
        if not str1 or not str2: 
            return True # If data is missing, we trust the Line Number
        
        s1 = str(str1).lower().strip()
        s2 = str(str2).lower().strip()
        
        # 1. Exact Match (Fast)
        if s1 == s2: return True
        
        # 2. Containment (e.g. "Drill" inside "Drill Bit")
        if s1 in s2 or s2 in s1: return True
        
        # 3. Fuzzy Ratio
        ratio = SequenceMatcher(None, s1, s2).ratio()
        return ratio > threshold

    def reconcile_po(self, po_number: str) -> Dict[str, Any]:
        all_items = self.db.fetch_line_items(po_number)
        
        if not all_items:
            return {"overall_status": "PO_DATA_MISSING", "line_items": []}

        po_ledger = {} 
        dn_ledger = defaultdict(float) 
        si_ledger = defaultdict(float) 
        seen_docs = set()
        
        # Store PO Descriptions to compare against DO/SI later
        po_descriptions = {}

        for item in all_items:
            doc_type = item.get('doc_type', '').lower()
            if 'purchase' in doc_type or 'po' in doc_type: tag = 'po'
            elif 'delivery' in doc_type or 'do' in doc_type or 'dn' in doc_type: tag = 'dn'
            elif 'invoice' in doc_type or 'si' in doc_type: tag = 'si'
            else: tag = 'unknown'

            seen_docs.add(tag)
            
            # Normalize Line Number
            raw_ref = str(item.get('line_ref', ''))
            norm_ref = self._normalize_key(raw_ref)
            
            try: qty = float(item.get('quantity', 0.0))
            except: qty = 0.0

            if tag == 'po':
                po_ledger[norm_ref] = {"qty": qty, "desc": item.get('description', '')}
                po_descriptions[norm_ref] = item.get('description', '')
                
            elif tag == 'dn':
                dn_ledger[norm_ref] += qty
                
                # --- CONTENT VALIDATION ---
                # Even if Line Numbers match, check if Descriptions are totally different
                if norm_ref in po_descriptions:
                    po_desc = po_descriptions[norm_ref]
                    dn_desc = item.get('description', '')
                    
                    if not self._strings_are_similar(po_desc, dn_desc):
                        return {
                            "po_number": po_number, 
                            "overall_status": "MISMATCH",
                            "details": f"Content Mismatch on Line {norm_ref}: PO='{po_desc[:15]}...', DO='{dn_desc[:15]}...'"
                        }
                        
            elif tag == 'si':
                si_ledger[norm_ref] += qty

        # Check Missing Docs
        missing = []
        if 'po' not in seen_docs: missing.append("PO")
        if 'dn' not in seen_docs: missing.append("DO")
        if 'si' not in seen_docs: missing.append("SI")
        if missing:
            return {"overall_status": "WAITING_FOR_DOCS", "details": f"Missing: {','.join(missing)}"}

        # Compare Quantities
        report = []
        universe_status = "MATCH"
        
        matched_lines_count = 0

        for line_ref, po_data in po_ledger.items():
            ordered = po_data['qty']
            received = dn_ledger.get(line_ref, 0.0)
            invoiced = si_ledger.get(line_ref, 0.0)
            
            line_status = "OK"
            
            if received < ordered:
                line_status = "PARTIAL_DELIVERY"
                universe_status = "INCOMPLETE" # Stops Merge
            elif received > ordered:
                line_status = "OVER_DELIVERY"
                universe_status = "ATTENTION"
            
            if received > 0:
                matched_lines_count += 1
            
            report.append({
                "Line": line_ref, "Status": line_status,
                "Ordered": ordered, "Received": received, "Desc": po_data['desc']
            })

        # Final Safety Check: Did we actually match anything?
        if len(po_ledger) > 0 and matched_lines_count == 0:
             return {
                "po_number": po_number, 
                "overall_status": "MISMATCH",
                "details": "Documents exist but ZERO line items matched. Check parsing logic."
            }

        # Check for Unsolicited Items (Ghost Items)
        for line_ref in dn_ledger:
            if line_ref not in po_ledger:
                return {"overall_status": "MISMATCH", "details": f"Delivery contains item line {line_ref} not found in PO."}

        return {"overall_status": universe_status, "line_items": report}