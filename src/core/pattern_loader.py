import yaml
import os
import logging

logger = logging.getLogger(__name__)

class PatternConfig:
    def __init__(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.dirname(os.path.dirname(current_dir))
        
        self.filepath = os.path.join(self.project_root, "patterns.yaml")
        self._patterns = self._load_patterns()

    def _load_patterns(self):
        if not os.path.exists(self.filepath):
            logger.error(f"CRITICAL: Pattern file NOT found at: {self.filepath}")
            logger.error("   (Please ensure patterns.yaml is in the project root)")
            return {}
        
        try:
            with open(self.filepath, 'r') as file:
                data = yaml.safe_load(file) or {}
                
                # --- DEBUG LOG ---
                # This proves if we actually loaded anything
                count = len(data.get('document_types', {}))
                logger.info(f"Loaded {count} document categories from {self.filepath}")
                return data
                
        except Exception as e:
            logger.error(f"❌ Failed to parse patterns.yaml: {e}")
            return {}

    def get_type_patterns(self):
        return self._patterns.get("document_types", {})

# Global instance
pattern_config = PatternConfig()