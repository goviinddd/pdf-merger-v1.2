import configparser
import os

class Config:
    def __init__(self, filename='config.txt'):
        self.config = configparser.ConfigParser()
        
        # Check if config exists
        if not os.path.exists(filename):
            print(f"Config file '{filename}' not found. Using defaults.")
            # We don't raise an error here to allow defaults to take over if needed,
            # but usually, you want to warn the user.
        
        self.config.read(filename)

    def get_api_key(self):
        key = self.config.get('API_KEYS', 'GEMINI_API_KEY', fallback=None)
        if not key or "paste_your_key" in key:
            raise ValueError("MISSING API KEY: Please check config.txt")
        return key

    def get_llm_settings(self):
        return {
            "model_name": self.config.get('LLM_SETTINGS', 'MODEL_NAME', fallback="gemini-2.5-flash"),
            "temperature": self.config.getfloat('LLM_SETTINGS', 'TEMPERATURE', fallback=0.7),
            "max_tokens": self.config.getint('LLM_SETTINGS', 'MAX_OUTPUT_TOKENS', fallback=2048),
        }

    def get_hardware_settings(self):
        return {
            "force_cpu": self.config.getboolean('HARDWARE', 'FORCE_CPU', fallback=False),
            "quantization": self.config.get('HARDWARE', 'QUANTIZATION_MODE', fallback="none")
        }

# Create a global instance so you can import 'settings' anywhere
try:
    settings = Config()
except Exception as e:
    print(f"Config Error: {e}")
    settings = None