import logging 

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class TextReplacer:
    def __init__(self, file_path: str = None, replacements: dict = None):
        """
        Initialize the TextReplacer with optional file path and replacements.
        
        Args:
            file_path (str, optional): Path to the input file
            replacements (dict, optional): Dictionary with keys as strings to find 
                                        and values as replacement strings
        """
        self.file_path = file_path
        self.replacements = replacements or {}
        self.content = None
        
    def set_file_path(self, file_path: str) -> None:
        """Set or update the file path."""
        self.file_path = file_path
        
    def set_replacements(self, replacements: dict) -> None:
        """Set or update the replacements dictionary."""
        self.replacements = replacements
        
    def add_replacement(self, key: str, value: str) -> None:
        """Add a single replacement key-value pair."""
        self.replacements[key] = value
        
    def read_file(self) -> str:
        """
        Read the content of the file.
        
        Returns:
            str: Content of the file
            
        Raises:
            FileNotFoundError: If the specified file doesn't exist
            IOError: If there's an error reading the file
        """
        if not self.file_path:
            raise ValueError("File path not set")
            
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                self.content = file.read()
            return self.content
        except FileNotFoundError:
            raise FileNotFoundError(f"The file {self.file_path} was not found.")
        except IOError as e:
            raise IOError(f"Error reading the file: {str(e)}")
            
    def replace_text(self) -> str:
        """
        Replace text in the content using the replacements dictionary.
        
        Returns:
            str: Modified content with all replacements applied
            
        Raises:
            ValueError: If content is not loaded or replacements not set
        """
        if self.content is None:
            self.read_file()
            
        modified_content = self.content
        for old_text, new_text in self.replacements.items():
            logger.info(f"Replacing {old_text} with {new_text}")
            modified_content = modified_content.replace(old_text, str(new_text))
            
        return modified_content
    
    def process_file(self) -> str:
        """
        Convenience method to read file and perform replacements in one step.
        
        Returns:
            str: Modified content with all replacements applied
        """
        self.read_file()
        return self.replace_text()
    