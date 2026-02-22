import logging
import json
import os
from groq import Groq
from src.core.config_loader import settings

logger = logging.getLogger(__name__)

# Re-use the safe key loader logic or just import from api_connector if cleaner
API_KEY = os.getenv("GROQ_API_KEY") 
client = Groq(api_key=API_KEY) if API_KEY else None

def smart_reconcile_items(po_items_unmatched, doc_items_unmatched):
    """
    Uses Llama 3 to intelligently match items that Regex missed.
    Example: 'Apple iPhone 15' (PO) <-> 'iPhone 15 Black' (Invoice)
    """
    if not client or not po_items_unmatched or not doc_items_unmatched:
        return []

    logger.info("🧠 Brain Activated: Attempting to match orphaned items...")

    prompt = f"""
    ROLE: Supply Chain Forensic Auditor.
    TASK: Match unmatched items from a Purchase Order (PO) to items in a Delivery/Invoice document.
    
    LIST A (PO Items - The Authority):
    {json.dumps(po_items_unmatched, indent=2)}

    LIST B (Document Items - The Target):
    {json.dumps(doc_items_unmatched, indent=2)}

    INSTRUCTIONS:
    1. Compare descriptions, part numbers, and quantities.
    2. Be flexible with text (e.g. "Steel Rod" == "Rod, Steel").
    3. Return ONLY pairs that are definitely the same product.
    4. Output JSON format:
    {{
        "matches": [
            {{ "po_line_ref": "1", "doc_line_ref": "10", "confidence": "high" }}
        ]
    }}
    """

    try:
        completion = client.chat.completions.create(
            model="llama-3.1-8b-instant", # Fast text model for logic
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        
        result_text = completion.choices[0].message.content
        data = json.loads(result_text)
        return data.get("matches", [])

    except Exception as e:
        logger.error(f"🧠 Brain Freeze: {e}")
        return []