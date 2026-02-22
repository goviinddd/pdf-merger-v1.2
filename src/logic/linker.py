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
        # Create a shallow copy to prevent side-effects on the raw source list
        new_item = row.copy()
        
        # 1. Inject the PO Number
        new_item['po_number'] = po_number
        
        # 2. Validate/Normalize Quantity
        # Optimized: try float conversion directly, only clean if it fails
        try:
            # Fast path: It's already a number or a clean string
            new_item['quantity'] = float(row.get('quantity', 0))
        except (ValueError, TypeError):
            # Slow path: It's a messy string like "5.00 EA"
            try:
                raw_qty = str(row.get('quantity', '0'))
                # Simple chain is faster than regex for just comma/strip
                qty_clean = raw_qty.replace(',', '').strip()
                new_item['quantity'] = float(qty_clean)
            except ValueError:
                new_item['quantity'] = 0.0

        linked_items.append(new_item)
        
    return linked_items