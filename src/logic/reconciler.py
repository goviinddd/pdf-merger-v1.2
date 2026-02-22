import logging
import re
from collections import defaultdict
from typing import List, Dict, Any
from src.logic.smart_matcher import smart_reconcile_items  

logger = logging.getLogger(__name__)

# Pre-compile Regex for performance
DIGIT_EXTRACTOR = re.compile(r'(\d+)')

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
        
        # Fast Check: Is it already just a digit?
        if s.isdigit(): return s

        # Handle 3-1 or 3.0
        if '-' in s:
            parts = s.split('-')
            if parts[0].strip().isdigit(): return parts[0].strip()
        
        if '.' in s:
            parts = s.split('.')
            if parts[0].strip().isdigit(): return parts[0].strip()

        # Regex for "Item 3"
        match = DIGIT_EXTRACTOR.search(s)
        if match: return match.group(1)
        
        return s

    def reconcile_po(self, po_number: str) -> Dict[str, Any]:
        all_items = self.db.fetch_line_items(po_number)
        
        if not all_items:
            return {"overall_status": "PO_DATA_MISSING", "line_items": []}

        po_ledger = {} 
        dn_ledger = defaultdict(float) 
        si_ledger = defaultdict(float) 
        seen_docs = set()
        
        # We keep this just for the report descriptions, not for validation
        po_descriptions = {}

        # --- SEPARATION STEP ---
        po_items = []
        other_items = []

        for item in all_items:
            doc_type = item.get('doc_type', '').lower()
            if 'purchase' in doc_type or 'po' in doc_type:
                seen_docs.add('po')
                po_items.append(item)
            else:
                if 'delivery' in doc_type or 'do' in doc_type:
                    seen_docs.add('dn')
                    item['_tag'] = 'dn'
                elif 'invoice' in doc_type or 'si' in doc_type:
                    seen_docs.add('si')
                    item['_tag'] = 'si'
                else:
                    item['_tag'] = 'unknown'
                other_items.append(item)

        # --- PASS 1: Build PO Ledger (The Truth) ---
        for item in po_items:
            raw_ref = str(item.get('line_ref', ''))
            norm_ref = self._normalize_key(raw_ref)
            try: qty = float(item.get('quantity', 0.0))
            except: qty = 0.0
            
            po_ledger[norm_ref] = {"qty": qty, "desc": item.get('description', '')}
            po_descriptions[norm_ref] = item.get('description', '')

        # --- PASS 2: Process Delivery & Invoices (Initial Sort) ---
        for item in other_items:
            tag = item.get('_tag')
            raw_ref = str(item.get('line_ref', ''))
            norm_ref = self._normalize_key(raw_ref)
            try: qty = float(item.get('quantity', 0.0))
            except: qty = 0.0

            if tag == 'dn':
                dn_ledger[norm_ref] += qty
            elif tag == 'si':
                si_ledger[norm_ref] += qty

        # --- CHECK MISSING DOCS ---
        required = {'po', 'dn', 'si'}
        missing = required - seen_docs
        if missing:
             return {"overall_status": "WAITING_FOR_DOCS", "details": "Missing Documents"}

        # =========================================================
        # 🧠 SMART BRAIN INTERVENTION (NEW)
        # =========================================================
        # 1. Identify "Orphans" (Items in Docs that don't match any PO Line)
        # 2. Identify "Missing" (PO Lines that received nothing)
        
        unmatched_po = []
        unmatched_doc_candidates = []

        # Find PO lines that are currently empty in either ledger
        for ref, data in po_ledger.items():
            if ref not in dn_ledger and ref not in si_ledger:
                unmatched_po.append({"ref": ref, "desc": data['desc'], "qty": data['qty']})
        
        # Find Invoice/DN lines that don't exist in PO
        # (We combine them into a single list for the AI to check)
        for ref, qty in si_ledger.items():
            if ref not in po_ledger:
                unmatched_doc_candidates.append({"type": "invoice", "ref": ref, "qty": qty})
        
        for ref, qty in dn_ledger.items():
            if ref not in po_ledger:
                unmatched_doc_candidates.append({"type": "delivery", "ref": ref, "qty": qty})

        # If we have orphans on both sides, call the AI
        if unmatched_po and unmatched_doc_candidates:
            logger.info(f"🧠 Mismatch detected. Asking AI to match {len(unmatched_doc_candidates)} orphans to {len(unmatched_po)} PO lines...")
            
            matches = smart_reconcile_items(unmatched_po, unmatched_doc_candidates)
            
            for match in matches:
                po_ref = str(match.get('po_line_ref'))
                doc_ref = str(match.get('doc_line_ref'))
                confidence = match.get('confidence', 'low')

                if confidence == 'high' and po_ref in po_ledger:
                    # FIX THE LEDGERS
                    # If it was an invoice orphan, move it to the correct PO line
                    if doc_ref in si_ledger:
                        qty = si_ledger.pop(doc_ref)
                        si_ledger[po_ref] += qty
                        logger.info(f"   ✅ AI Corrected Invoice: Orphan {doc_ref} -> PO Line {po_ref}")

                    # If it was a delivery orphan, move it
                    if doc_ref in dn_ledger:
                        qty = dn_ledger.pop(doc_ref)
                        dn_ledger[po_ref] += qty
                        logger.info(f"   ✅ AI Corrected Delivery: Orphan {doc_ref} -> PO Line {po_ref}")
        
        # =========================================================

        # --- COMPARE QUANTITIES (3-WAY MATCH) ---
        report = []
        universe_status = "MATCH"
        matched_lines_count = 0

        for line_ref, po_data in po_ledger.items():
            ordered = po_data['qty']
            received = dn_ledger.get(line_ref, 0.0)
            invoiced = si_ledger.get(line_ref, 0.0)
            
            line_status = "OK"
            
            # 1. Check Delivery vs PO
            if received < ordered:
                line_status = "PARTIAL_DELIVERY"
                universe_status = "INCOMPLETE"
            elif received > ordered:
                line_status = "OVER_DELIVERY"
                universe_status = "ATTENTION"
            
            # 2. Check Invoice vs Delivery (Pay for what you got)
            if invoiced < received:
                line_status = "PARTIAL_INVOICE"
                universe_status = "INVOICE_PENDING" # Valid block
            elif invoiced > received:
                line_status = "OVER_INVOICED"
                universe_status = "ATTENTION"

            if received > 0 or invoiced > 0:
                matched_lines_count += 1
            
            report.append({
                "Line": line_ref, "Status": line_status,
                "Ordered": ordered, "Received": received, "Invoiced": invoiced,
                "Desc": po_data['desc']
            })

        if len(po_ledger) > 0 and matched_lines_count == 0:
             return {"po_number": po_number, "overall_status": "MISMATCH", "details": "Zero lines matched."}

        # Check for Ghost Items (Items in Invoice that aren't in PO)
        # (If they are still here, the AI failed to match them, so they are genuine errors)
        for line_ref in si_ledger:
            if line_ref not in po_ledger:
                return {"overall_status": "MISMATCH", "details": f"Invoice contains extra line {line_ref}"}

        return {"overall_status": universe_status, "line_items": report}