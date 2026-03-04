import logging
from utils.prompt_generator_excel_utils import ExcelToDateframe
from utils.prompt_generator_utils import PromptGenerator
from utils.prompt_generator_param_utils import PromptGeneratorParam

# Initialize logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='dal_prompt_generator.log', # Optional: logs to a file
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

input_excel_file_name = input("Enter excel file name with DynamoDB table specification: ")
input_python_ddb_library_name = input("Enter python library to use(pynamodb or boto3): ")

# Initialize and process Excel to Dataframe (includes validation)
excel_to_dataframe = ExcelToDateframe(
    excel_file_name = input_excel_file_name
)
df, batch_id = excel_to_dataframe.read_from_excel()

# Initialize and generate prompt generator paramaters
params_generator = PromptGeneratorParam(df = df,
                                        batch_id = batch_id,
                                        excel_file_name = input_excel_file_name,
                                        library_name = input_python_ddb_library_name)
prompt_params = params_generator.get_prams()
logger.info(f"Generate prompt paramaters: {prompt_params}")

# Genreate prompt files using prompt generator paramaters
for prompt_param in prompt_params:
    prompt_template_files = prompt_param.get('prompt_template_files')
    replacements = prompt_param.get('replacements')
    prompt_file_name = prompt_param.get('prompt_file_name')
    prompt_folder_name = prompt_param.get('prompt_folder_name')

    # Initialize the processor
    processor = PromptGenerator(prompt_template_files = prompt_template_files,
                                replacements = replacements,
                                prompt_file_name = prompt_file_name,
                                prompt_folder_name = prompt_folder_name)

    # Process and generate prompt file
    logger.info(f"Start prompt file generation process for {prompt_file_name}")
    processor.process_and_generate_prompt_file()
    logger.info(f"End prompt file generation process for {prompt_file_name}")
