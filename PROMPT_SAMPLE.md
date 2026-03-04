### Prompt to generate DAL code using boto3

Read the existing PROJECT_SAMPLE_FOR_REFERENCE_BOTO3 in output_code_generated folder and all its subfolders to infer coding style, dependencies and types of unit test cases. Read all prompt files in the prompt_5161ba87-b004-4bd0-8441-6efd5bf5ae86_CUSTOMER_POLICY folder in order. Generate code for all prompt files and place in PROJECT_CUSTOMER_POLICY_BOTO3 folder(to be created) in existing output_code_generate folder. At the end, generate a requirements.txt file in the PROJECT folder. Continue without interruption or user interaction. unit test scripts generated  should be compressive (test retry mechanism, test for non-existent keys, query methods with different input parameters) and similar count as sample project. Ensure all unit tests run without errors.

### Prompt to generate DAL code using PynamodDB

Read the existing PROJECT_SAMPLE_FOR_REFERENCE_PYNAMODB in output_code_generated folder and all its subfolders to infer coding style, dependencies and types of unit test cases. Read all prompt files in the prompt_89d43f59-ece9-45cd-9e80-6119eaabf7ba_CUSTOMER_POLICY folder in order. Generate code for all prompt files and place in PROJECT_CUSTOMER_POLICY_PYNAMODB folder(to be created) in existing output_code_generate folder. At the end, generate a requirements.txt file in the PROJECT folder. Continue without interruption or user interaction. unit test scripts generated  should be compressive (test retry mechanism, test for non-existent keys, query methods with different input parameters) and similar count as sample project. Ensure all unit tests run without errors.

### Prompt to modify update DAL method

In PROJECT_SAMPLE_FOR_REFERENCE_PYNAMODB, update_item dal methods has to use pynamodb update() method instead of save(). corresponding unit test should be modified. Ensure unit test works fine

### Prompt to create new count DAL method

In PROJECT_SAMPLE_FOR_REFERENCE_PYNAMODB, add additional method(in DAL) to get the count from the table using pynamodb count() method. it takes hask_key, index_name, range_key_condition and filter_condition as input and return count. Add unit test for these methods and ensure unit test runs without errors