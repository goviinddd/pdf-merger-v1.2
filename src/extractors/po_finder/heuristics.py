import re
from typing import Optional

# --- CONSTANTS ---
MAX_PO_LENGTH = 18 
MIN_PO_LENGTH = 4

def aggressive_normalize(text: str) -> str:
    if not text: return ""
    text = text.upper().strip()
    text = re.sub(r'[^A-Z0-9\-]', '', text) 
    return text

def is_date(text: str) -> bool:
    date_patterns = [
        r'\d{2}-[A-Z]{3}-\d{4}', 
        r'\d{4}-\d{2}-\d{2}',    
        r'\d{2}/\d{2}/\d{4}'     
    ]
    for pat in date_patterns:
        if re.search(pat, text):
            return True
    return False

def fix_repetition(text: str) -> str:
    """Detects and fixes recursive repetition."""
    n = len(text)
    if n < 8: return text
    
    for length in range(4, n // 2 + 1):
        seed = text[:length]
        remainder = text[length:]
        if remainder.startswith(seed):
            if any(c.isdigit() for c in seed):
                return seed
    return text

def apply_strict_patterns(text: str) -> Optional[str]:
    """The 'Sieve': Enforces exact boundaries."""
    patterns = [
        r'^(10006-\d{10})', 
        r'^(P\d{5,6})',
        r'^(J\d{3,}-\d{6,})',
        r'^(90\d{6})',
        r'^(300\d{6})',
        r'^(13\d{3,})', 
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
            
    return None

def rescue_yolo_hit(raw_text: str) -> Optional[str]:
    """
    The Main Cleaning Pipeline for YOLO.
    """
    # 1. Normalize
    clean = aggressive_normalize(raw_text)
    
    clean = fix_repetition(clean)
    
    strict_hit = apply_strict_patterns(clean)
    if strict_hit:
        return strict_hit

    # 3. Final Sanity Check (Fallback)
    if MIN_PO_LENGTH <= len(clean) <= MAX_PO_LENGTH:
        if any(c.isdigit() for c in clean):
            return clean

    return None

def find_po_number_in_text(text: str) -> Optional[str]:
    """Legacy full-page search"""
    if is_date(text): return None
    
    raw_clean_text = aggressive_normalize(text)
    
    # Apply strict patterns to full page text too
    clean_text = fix_repetition(raw_clean_text) # Apply fix here too
    
    strict_hit = apply_strict_patterns(clean_text)
    if strict_hit: return strict_hit
    
    if len(clean_text) <= MAX_PO_LENGTH and len(clean_text) >= MIN_PO_LENGTH:
        if sum(c.isdigit() for c in clean_text) >= 2:
            return clean_text
            
    patterns = [r'(?:PO|ORDER|NO\.)[\s\.:-]*([A-Z0-9\-]{4,})']
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            cand = aggressive_normalize(match.group(1))
            if MIN_PO_LENGTH <= len(cand) <= MAX_PO_LENGTH:
                return cand
    return None