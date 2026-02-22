import yaml
import os
import logging

logger = logging.getLogger(__name__)

class PromptLoader:
    _prompts = None

    @classmethod
    def load_prompts(cls, path="prompts.yaml"):
        if cls._prompts:
            return cls._prompts
            
        if not os.path.exists(path):
            # Fallback path if running from src
            path = os.path.join(os.getcwd(), "prompts.yaml")
            
        try:
            with open(path, "r", encoding="utf-8") as f:
                cls._prompts = yaml.safe_load(f)
                logger.info("Prompts loaded from YAML.")
                return cls._prompts
        except Exception as e:
            logger.error(f"Failed to load prompts.yaml: {e}")
            return {}

    @classmethod
    def get(cls, key):
        if not cls._prompts:
            cls.load_prompts()
        return cls._prompts.get(key, "")