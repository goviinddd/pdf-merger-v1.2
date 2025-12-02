import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def link_extracted_data(po_number: str, raw_table_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    The 'Glue' Function.
    Takes the PO Number (found by YOLO Sniper) and the Table Data (found by API),
    and merges them into fully valid line item records.
    """
    if not raw_table_data:
        logger.warning(f"No table data to link for PO {po_number}")
        return []

    linked_items = []
    
    for row in raw_table_data:
        # 1. Inject the PO Number (The missing link)
        row['po_number'] = po_number
        
        # 2. Add other metadata if needed (e.g., source filename)
        # row['source_file'] = filename
        
        # 3. Validate/Normalize
        # Ensure quantities are numbers, not strings like "5.00 EA"
        try:
            qty_clean = str(row.get('quantity', '0')).replace(',', '').strip()
            row['quantity'] = float(qty_clean)
        except ValueError:
            # If conversion fails, default to 0.0 to prevent database errors
            row['quantity'] = 0.0

        linked_items.append(row)
        
    return linked_items