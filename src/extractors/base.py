from abc import ABC, abstractmethod

class BaseTextExtractor(ABC):
    """
    The Interface (Blueprint) for all text extraction tools.
    
    As per V1 Design:
    - Focused Responsibility: Just get text.
    """
    
    @abstractmethod
    def extract(self, file_path: str) -> str:
        """
        Takes a file path and returns the raw text contained within.
        
        Args:
            file_path (str): The absolute path to the PDF file.
            
        Returns:
            str: The extracted text content.
        """
        pass