import logging
from typing import List, Dict
import os
from .prompt_generator_text_replacer_utils import TextReplacer

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class PromptGenerator:
    def __init__(self,
                prompt_template_files: List[str],
                replacements: List[Dict],
                prompt_file_name: str,
                prompt_folder_name: str):
        """Initialize the AWS clients"""
        self.text_replacer = TextReplacer()
        self.prompt_template_files = prompt_template_files
        self.replacements = replacements
        self.prompt_file_name = prompt_file_name
        self.prompt_folder_name = prompt_folder_name

    def process_prompts(self) -> str:
        """
        Process multiple prompt files and combine them with replacements
            
        Returns:
            str: Combined and processed prompt
        """
        combined_prompt = ""
        prompt_file_id = 0
        prompt_template_files = self.prompt_template_files
        replacements = self.replacements
        
        for prompt_file in prompt_template_files:
            try:
                replacement = replacements[prompt_file_id] if len(replacements) > 0 else {}
                self.text_replacer.set_file_path("prompt_templates/"+prompt_file)
                self.text_replacer.set_replacements(replacement)
                processed_prompt = self.text_replacer.process_file()
                combined_prompt += processed_prompt + "\n"
                prompt_file_id += 1
            except Exception as e:
                raise Exception(f"Error processing prompt file {prompt_file}: {str(e)}")
                
        return combined_prompt.strip()

    def process_and_generate_prompt_file(self) -> None:
        """
        Main method to process and generate prompts
        
        """
        prompt_file_name = self.prompt_file_name 
        prompt_folder_name = self.prompt_folder_name 

        try:
            # 1. Process prompts
            combined_prompt = self.process_prompts()
            logger.info(f"Combined Prompt generated successfully",  
                        extra={"prompt_file_name": self.prompt_file_name })

            # 2. Put the combined prompt in prompt file in prompt folder
            prompt_file_path = os.path.join("output_prompt_files", prompt_folder_name, prompt_file_name)
            with open(prompt_file_path, 'w') as file:
                file.write(combined_prompt)
                logger.info(f"Prompt file {prompt_file_name} generated successfully")
            
        except Exception as e:
            raise Exception(f"Error in process_and_generate_prompt_file: {str(e)}")

