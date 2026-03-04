import logging
import pandas as pd
from typing import Dict, List, Any
import os

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

date_format_function = f"datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')"

class PromptGeneratorParam:
    def __init__(self, df: pd.DataFrame, batch_id: str, excel_file_name: str, library_name: str):
        """Initialize datafreme, batch_id excel_file_name and library_name"""
        self.dateframe = df
        self.excel_file_name = excel_file_name
        self.batch_id = batch_id
        self.library_name = library_name
        excel_file_name_split = excel_file_name.split(".")[0]
        self.prompt_folder_name = 'prompt_' + batch_id + '_' + excel_file_name_split
        ## create folder if not exists
        output_folder = os.path.join('output_prompt_files', self.prompt_folder_name)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        logger.info(f"Prompt folder name : {self.prompt_folder_name}",  
                    extra={"batch_id": self.batch_id,"excel_file_name": self.excel_file_name}) 

    def cammel_to_snake(self, name):
        """Convert CamelCase to snake_case"""
        return ''.join(['_' + i.lower() if i.isupper() else i for i in name]).lstrip('_')
    
    def get_pynamodb_folder_structure_parmas(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Return parameters for PynamoDB folder creation"""
        table_name = item.get('TABLE_NAME', '').strip()
        table_name_lower = self.cammel_to_snake(table_name)

        logger.info(f"Folder structure (pynamodb) prompt generator parameters",  
                    extra={"batch_id": self.batch_id,"excel_file_name": self.excel_file_name,"table_name": table_name}) 
        
        return {
            'prompt_template_files': ['pynamodb_folder_structure.txt'],
            'replacements': [{}],
            'prompt_file_name': f"prompt_{table_name_lower}_0_folder_structure.md",
            'prompt_folder_name': self.prompt_folder_name
        }
    
    def get_pynamodb_model_params(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Return parameters for PynamoDB model prompt file generation"""
        table_name = item.get('TABLE_NAME', '').strip()
        table_name_lower = self.cammel_to_snake(table_name)
        table_name_upper = table_name_lower.upper()
        prompt_template_files = ['pynamodb_model.txt']
        replacements_dict= {'{{ddb_table_name}}': table_name,
                            '{{ddb_table_name_env_variable}}': table_name_upper, 
                            '{{ddb_table_attribute_names}}': item.get('ATTRIBUTES', ''),
                            '{{ddb_table_attribute_data_types}}': item.get('ATTRIBUTE_DATA_TYPES', ''),
                            '{{ddb_table_name_lower}}': table_name_lower,
                            '{{ddb_table_pk}}': item.get('TABLE_PK', ''),
                            '{{ddb_table_attribute_settings}}': item.get('ATTRIBUTE_DEFAULT_NULL', '')}
        
        ## Handle TABLE_SK
        table_sk = item.get('TABLE_SK', '')
        if table_sk != 'nan' and table_sk != '':
            replacements_dict['{{ddb_table_sk}}'] = f"and {table_sk} is the sort/range key (sk) "
        else:
            replacements_dict['{{ddb_table_sk}}'] = "and no sort/range key (sk) "
        
        ## Handle CREATED_AT
        is_created_at = item.get('CREATED_AT_REQUIRED', 'Yes').strip().lower()
        if is_created_at == 'yes':
            created_at_replacement = "created_at is additional string attributes, "
            replacements_dict['{{created_at_replacement}}'] = created_at_replacement
        else:
            replacements_dict['{{created_at_replacement}}'] = ""
        
        ## Handle UPDATED_AT
        is_updated_at = item.get('UPDATED_AT_REQUIRED', 'Yes').strip().lower()
        if is_updated_at == 'yes':
            updated_at_replacement = "updated_at is additional string attributes, "
            set_updated_at_replacement = f"updated_at is set to {date_format_function} (if updated_at not part of dto). "
            replacements_dict['{{updated_at_replacement}}'] = updated_at_replacement
            replacements_dict['{{set_updated_at_replacement}}'] = set_updated_at_replacement
        else:
            replacements_dict['{{updated_at_replacement}}'] = ""
            replacements_dict['{{set_updated_at_replacement}}'] = ""

        ## Handle TIME_TO_LIVE
        is_time_to_live = item.get('TIME_TO_LIVE_REQUIRED', 'Yes').strip().lower()
        if is_time_to_live == 'yes':
            time_to_live_replacement_a = "time_to_live attribute is TTLattribute with null=True, "
            time_to_live_replacement_1 = f"8a) create method get_ttl_days to get environment variable {table_name_upper}_TTL_DAYS or 0 if null\n"
            time_to_live_replacement_2 = f"8b) create method get_test_ttl_days to get environment variable {table_name_upper}_TEST_TTL_DAYS or 30 if null\n"
            time_to_live_replacement_3 = f"8c) create get_ttl method which checks is_test_object(method which returns true/false if partition key of the table has string TEST) if true return timedelta(get_test_ttl_days()). if false then get ttl_days from get_ttl_days() method if ttl_days != 0 then return timedelta(ttl_days) else return None. In save method to set time_to_live attribute equal to get_ttl()"
            time_to_live_replacement_b = time_to_live_replacement_1 + time_to_live_replacement_2 + time_to_live_replacement_3
            time_to_live_replacement_c = ", time_to_live"
            replacements_dict['{{time_to_live_replacement_a}}'] = time_to_live_replacement_a
            replacements_dict['{{time_to_live_replacement_b}}'] = time_to_live_replacement_b
            replacements_dict['{{time_to_live_replacement_c}}'] = time_to_live_replacement_c
        else:
            replacements_dict['{{time_to_live_replacement_a}}'] = ""
            replacements_dict['{{time_to_live_replacement_b}}'] = ""
            replacements_dict['{{time_to_live_replacement_c}}'] = ""
        
        replacements = [replacements_dict]

        ## Handle GSI
        gsi_pks = item.get('GSI_PKs','').split(',')
        gsi_sks = item.get('GSI_SKs','').split(',')
        gsi_projections = item.get('GSI_PROJECTIONs','').split(',')
        
        logger.info(f"GSI PKs: {gsi_pks}, GSI SKs: {gsi_sks}, GSI PROJECTIONS: {gsi_projections}")

        if gsi_pks != ['nan']:
            for i in range(len(gsi_pks)):
                if gsi_pks[i]:
                    gsi_pk = gsi_pks[i].strip()
                    gsi_sk = gsi_sks[i].strip()
                    gsi_projection = gsi_projections[i].strip()

                    ## prompt_template_files append
                    prompt_template_files.append('table_gsi.txt')
                    
                    ## replacements append
                    gsi_replacement_dict = {'{{sl_no}}': 9+i,
                                            '{{ddb_table_gsi_pk}}': gsi_pk}
                    ## Handle GSI SK
                    if gsi_sk != 'nan' and gsi_sk != '':
                        gsi_replacement_dict['{{ddb_table_gsi_sk_string}}'] = f" and {gsi_sk} as sort/range key (sk)"
                        gsi_replacement_dict['{{ddb_table_gsi_sk}}'] = "__" + gsi_sk
                    else:
                        gsi_replacement_dict['{{ddb_table_gsi_sk_string}}'] = "and no sort/range key (sk) "
                        gsi_replacement_dict['{{ddb_table_gsi_sk}}'] = ""
                    
                    ## Handle GSI PROJECTIONS
                    if gsi_projection != 'nan' and gsi_projection != '':
                        gsi_replacement_dict['{{ddb_table_gsi_projection}}'] = gsi_projection.replace(" ", ", ").replace("(", "").replace(")", "").replace("~", " ")
                    else:
                        gsi_replacement_dict['{{ddb_table_gsi_projection}}'] = ""

                    replacements.append(gsi_replacement_dict)

                else:
                    break
        
        logger.info(f"Model(pynamodb) prompt generator parameters",  
                    extra={"batch_id": self.batch_id,"excel_file_name": self.excel_file_name,"table_name": table_name}) 
        return {
            'prompt_template_files': prompt_template_files,
            'replacements': replacements,
            'prompt_file_name': f"prompt_{table_name_lower}_1_model.md",
            'prompt_folder_name': self.prompt_folder_name
        }

    def get_dto_parmas(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Return parameters for PynamoDB DTO prompt file generation"""
        table_name = item.get('TABLE_NAME', '').strip()
        table_pk = item.get('TABLE_PK', '')
        table_sk = item.get('TABLE_SK', '')
        table_name_lower = self.cammel_to_snake(table_name)

        logger.info(f"DTO(pynamodb) prompt generator parameters",  
                    extra={"batch_id": self.batch_id,"excel_file_name": self.excel_file_name,"table_name": table_name}) 
        
        replacements_dict = {'{{ddb_table_name}}': table_name}

        ## Handle CREATED_AT
        is_created_at = item.get('CREATED_AT_REQUIRED', 'Yes').strip().lower()
        if is_created_at == 'yes' and table_pk != 'created_at' and table_sk != 'created_at':
            created_at_replacement = " created_at,"
            replacements_dict['{{created_at_replacement}}'] = created_at_replacement
        else:
            replacements_dict['{{created_at_replacement}}'] = ""
        
        ## Handle UPDATED_AT
        is_updated_at = item.get('UPDATED_AT_REQUIRED', 'Yes').strip().lower()
        if is_updated_at == 'yes' and table_pk != 'updated_at' and table_sk != 'updated_at':
            updated_at_replacement = " updated_at,"
            replacements_dict['{{updated_at_replacement}}'] = updated_at_replacement
        else:
            replacements_dict['{{updated_at_replacement}}'] = ""

        ## Handle TIME_TO_LIVE
        is_time_to_live = item.get('TIME_TO_LIVE_REQUIRED', 'Yes').strip().lower()
        if is_time_to_live == 'yes':
            time_to_live_replacement_a = " time_to_live,"
            replacements_dict['{{time_to_live_replacement_a}}'] = time_to_live_replacement_a
        else:
            replacements_dict['{{time_to_live_replacement_a}}'] = ""
        
        replacements = [replacements_dict]    

        return {
            'prompt_template_files': ['dto.txt'],
            'replacements': replacements,
            'prompt_file_name': f"prompt_{table_name_lower}_2_dto.md",
            'prompt_folder_name': self.prompt_folder_name
        }

    def get_pynamodb_dal_params(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Return parameters for PynamoDB DAL prompt file generation"""
        table_name = item.get('TABLE_NAME', '').strip()
        table_pk = item.get('TABLE_PK', '')
        table_name_lower = self.cammel_to_snake(table_name)

        logger.info(f"DAL(pynamodb) prompt generator parameters",  
                    extra={"batch_id": self.batch_id,"excel_file_name": self.excel_file_name,"table_name": table_name}) 
        
        replacements_dict = {'{{ddb_table_name}}': table_name, 
                             '{{ddb_table_name_lower}}': table_name_lower}

        ## Handle TABLE_SK
        table_sk = item.get('TABLE_SK', '')
        if table_sk != 'nan' and table_sk != '':
            replacements_dict['{{ddb_table_sk}}'] = f" and sort/range key (sk) of the table"
        else:
            replacements_dict['{{ddb_table_sk}}'] = ""
        
        ## Handle CREATED_AT
        is_created_at = item.get('CREATED_AT_REQUIRED', 'Yes').strip().lower()
        if is_created_at == 'yes' and table_pk != 'created_at' and table_sk != 'created_at':
            created_at_replacement = f". created_at is set to {date_format_function} (if created_at not part of dto) for create of item"
            replacements_dict['{{created_at_replacement}}'] = created_at_replacement
        else:
            replacements_dict['{{created_at_replacement}}'] = ""
        
        ## Handle UPDATED_AT
        is_updated_at = item.get('UPDATED_AT_REQUIRED', 'Yes').strip().lower()
        if is_updated_at == 'yes' and table_pk != 'updated_at' and table_sk != 'updated_at':
            updated_at_replacement = f". updated_at is set to {date_format_function} (if updated_at not part of dto) for update of item"
            replacements_dict['{{updated_at_replacement}}'] = updated_at_replacement
        else:
            replacements_dict['{{updated_at_replacement}}'] = ""

        ## Handle TIME_TO_LIVE
        is_time_to_live = item.get('TIME_TO_LIVE_REQUIRED', 'Yes').strip().lower()
        if is_time_to_live == 'yes':
            time_to_live_method_replacement = "For this method only, time_to_live attribute should be equal to get_ttl method in the model if = 0 set to None"
            replacements_dict['{{time_to_live_method_replacement}}'] = time_to_live_method_replacement
        else:
            replacements_dict['{{time_to_live_method_replacement}}'] = ""

        replacements = [replacements_dict]
        
        return {
            'prompt_template_files': ['pynamodb_dal.txt'],
            'replacements': replacements,
            'prompt_file_name': f"prompt_{table_name_lower}_3_dal.md",
            'prompt_folder_name': self.prompt_folder_name
        }

    def get_unit_test_params(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Return parameters for PynamoDB unit test prompt file generation"""
        table_name = item.get('TABLE_NAME', '').strip()
        table_name_lower = self.cammel_to_snake(table_name)

        logger.info(f"Unit test prompt generator parameters",  
                    extra={"batch_id": self.batch_id,"excel_file_name": self.excel_file_name,"table_name": table_name}) 
        
        if self.library_name == 'pynamodb':
            prompt_file_name = f"prompt_{table_name_lower}_4_test.md"
        elif self.library_name == 'boto3':
            prompt_file_name = f"prompt_{table_name_lower}_3_test.md"

        return {
            'prompt_template_files': ['unit_test.txt'],
            'replacements': [{}],
            'prompt_file_name': prompt_file_name,
            'prompt_folder_name': self.prompt_folder_name
        }

    def get_boto3_folder_structure_parmas(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Return parameters for boto3 folder creation prompt file generation"""
        table_name = item.get('TABLE_NAME', '').strip()
        table_name_lower = self.cammel_to_snake(table_name)

        logger.info(f"Folder structure (boto3) prompt generator parameters",  
                    extra={"batch_id": self.batch_id,"excel_file_name": self.excel_file_name,"table_name": table_name}) 
        
        return {
            'prompt_template_files': ['boto3_folder_structure.txt'],
            'replacements': [{}],
            'prompt_file_name': f"prompt_{table_name_lower}_0_folder_structure.md",
            'prompt_folder_name': self.prompt_folder_name
        }
    
    def get_boto3_dto_params(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Return parameters for Boto3 table/dto prompt file generation"""
        table_name = item.get('TABLE_NAME', '').strip()
        table_name_lower = self.cammel_to_snake(table_name)
        prompt_template_files = ['boto3_table_info.txt']
        replacements_dict = {'{{ddb_table_name}}': table_name,
                            '{{ddb_table_attribute_names}}': item.get('ATTRIBUTES', ''),
                            '{{ddb_table_attribute_data_types}}': item.get('ATTRIBUTE_DATA_TYPES', ''),
                            '{{ddb_table_pk}}': item.get('TABLE_PK', ''),
                            '{{ddb_table_attribute_settings}}': item.get('ATTRIBUTE_DEFAULT_NULL', '')}
        
        ## Handle TABLE_SK
        table_sk = item.get('TABLE_SK', '')
        if table_sk != 'nan' and table_sk != '':
            replacements_dict['{{ddb_table_sk}}'] = f" and {table_sk} is the sort/range key (sk)"
        else:
            replacements_dict['{{ddb_table_sk}}'] = ""

        ## Handle CREATED_AT
        is_created_at = item.get('CREATED_AT_REQUIRED', 'Yes').strip().lower()
        if is_created_at == 'yes':
            created_at_replacement = "created_at is additional string attributes,"
            replacements_dict['{{created_at_replacement}}'] = created_at_replacement
        else:
            replacements_dict['{{created_at_replacement}}'] = ""
        
        ## Handle UPDATED_AT
        is_updated_at = item.get('UPDATED_AT_REQUIRED', 'Yes').strip().lower()
        if is_updated_at == 'yes':
            updated_at_replacement = "updated_at is additional string attributes,"
            replacements_dict['{{updated_at_replacement}}'] = updated_at_replacement
        else:
            replacements_dict['{{updated_at_replacement}}'] = ""

        ## Handle TIME_TO_LIVE
        is_time_to_live = item.get('TIME_TO_LIVE_REQUIRED', 'Yes').strip().lower()
        if is_time_to_live == 'yes':
            time_to_live_replacement_a = "time_to_live attribute is TTLattribute,"
            replacements_dict['{{time_to_live_replacement_a}}'] = time_to_live_replacement_a
        else:
            replacements_dict['{{time_to_live_replacement_a}}'] = ""
        
        replacements = [replacements_dict]

        ## Handle GSI
        gsi_pks = item.get('GSI_PKs','').split(',')
        gsi_sks = item.get('GSI_SKs','').split(',')
        gsi_projections = item.get('GSI_PROJECTIONs','').split(',')

        logger.info(f"GSI PKs: {gsi_pks}, GSI SKs: {gsi_sks}, GSI PROJECTIONS: {gsi_projections}")
        
        if gsi_pks != ['nan']:
            for i in range(len(gsi_pks)):
                if gsi_pks[i]:
                    gsi_pk = gsi_pks[i].strip()
                    gsi_sk = gsi_sks[i].strip()
                    gsi_projection = gsi_projections[i].strip()

                    ## prompt_template_files append
                    prompt_template_files.append('table_gsi.txt')

                    ## replacements append
                    gsi_replacement_dict = {'{{sl_no}}': 6+i,
                                            '{{ddb_table_gsi_pk}}': gsi_pk}
                    ## Handle GSI SK
                    if gsi_sk != 'nan' and gsi_sk != '':
                        gsi_replacement_dict['{{ddb_table_gsi_sk_string}}'] = f" and {gsi_sk} as sort/range key (sk)"
                        gsi_replacement_dict['{{ddb_table_gsi_sk}}'] = "__" + gsi_sk
                    else:
                        gsi_replacement_dict['{{ddb_table_gsi_sk_string}}'] = "and no sort/range key (sk) "
                        gsi_replacement_dict['{{ddb_table_gsi_sk}}'] = ""

                    ## Handle GSI PROJECTIONS   
                    if gsi_projection != 'nan' and gsi_projection != '':
                        gsi_replacement_dict['{{ddb_table_gsi_projection}}'] = gsi_projection.replace(" ", ", ").replace("(", "").replace(")", "").replace("~", " ")
                    else:
                        gsi_replacement_dict['{{ddb_table_gsi_projection}}'] = ""

                    replacements.append(gsi_replacement_dict)
                else:
                    break
        
        prompt_template_files.append('dto.txt')
        replacements.append({'{{ddb_table_name}}': table_name})

        logger.info(f"DTO(boto3) prompt generator parameters",  
                    extra={"batch_id": self.batch_id,"excel_file_name": self.excel_file_name,"table_name": table_name}) 
        
        return {
            'prompt_template_files': prompt_template_files,
            'replacements': replacements,
            'prompt_file_name': f"prompt_{table_name_lower}_1_dto.md",
            'prompt_folder_name': self.prompt_folder_name
        }
    
    def get_boto3_dal_params(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Return parameters for Boto3 DAL generation"""
        table_name = item.get('TABLE_NAME', '').strip()
        table_pk = item.get('TABLE_PK', '')
        table_name_lower = self.cammel_to_snake(table_name)
        table_name_upper = table_name_lower.upper()
        prompt_template_files = ['boto3_dal.txt']
        replacements_dict = {'{{ddb_table_name}}': table_name,
                            '{{ddb_table_name_env_variable}}': table_name_upper, 
                            '{{ddb_table_name_lower}}': table_name_lower,
                            '{{ddb_table_pk}}': item.get('TABLE_PK', '')}
        
        ## Handle TABLE_SK
        table_sk = item.get('TABLE_SK', '')
        if table_sk != 'nan' and table_sk != '':
            replacements_dict['{{ddb_table_sk}}'] = f" and sort/range key (sk) of the table"
        else:
            replacements_dict['{{ddb_table_sk}}'] = ""

        ## Handle CREATED_AT
        is_created_at = item.get('CREATED_AT_REQUIRED', 'Yes').strip().lower()
        if is_created_at == 'yes' and table_pk != 'created_at' and table_sk != 'created_at':
            created_at_replacement = f". created_at is set to {date_format_function} (if created_at not part of dto) for create of item"
            replacements_dict['{{created_at_replacement}}'] = created_at_replacement
        else:
            replacements_dict['{{created_at_replacement}}'] = ""
        
        ## Handle UPDATED_AT
        is_updated_at = item.get('UPDATED_AT_REQUIRED', 'Yes').strip().lower()
        if is_updated_at == 'yes' and table_pk != 'updated_at' and table_sk != 'updated_at':
            updated_at_replacement = f". updated_at is set to {date_format_function} (if updated_at not part of dto) for update of item"
            replacements_dict['{{updated_at_replacement}}'] = updated_at_replacement
        else:
            replacements_dict['{{updated_at_replacement}}'] = ""
        
        ## Handle TIME_TO_LIVE
        is_time_to_live = item.get('TIME_TO_LIVE_REQUIRED', 'Yes').strip().lower()
        if is_time_to_live == 'yes':
            time_to_live_replacement_1 = f"4a) create method get_ttl_days to get environment variable {table_name_upper}_TTL_DAYS or 30 if null\n"
            time_to_live_replacement_2 = f"4b) create method get_test_ttl_days to get environment variable {table_name_upper}_TEST_TTL_DAYS or 30 if null\n"
            time_to_live_replacement_3 = f"4c) create get_ttl method to just return datetime's timedelta of get_ttl_days or get_test_ttl_days method based on is_test_object(method which returns true/false if partition key of the table has string TEST).\n"
            time_to_live_replacement_4 = f"4d) if is_test_object() is false return timedelta(get_ttl_days()) else timedelta(get_test_ttl_days()). Use this method to set the time_to_live attribute in DynamoDB table. "
            time_to_live_replacement = time_to_live_replacement_1 + time_to_live_replacement_2 + time_to_live_replacement_3 + time_to_live_replacement_4
            time_to_live_method_replacement = "For this method only, time_to_live attribute should be equal to get_ttl method in the model"
            replacements_dict['{{time_to_live_replacement}}'] = time_to_live_replacement
            replacements_dict['{{time_to_live_method_replacement}}'] = time_to_live_method_replacement
        else:
            replacements_dict['{{time_to_live_replacement}}'] = ""
            replacements_dict['{{time_to_live_method_replacement}}'] = ""


        replacements = [replacements_dict]
        logger.info(f"DAL(boto3) prompt generator parameters",  
                    extra={"batch_id": self.batch_id,"excel_file_name": self.excel_file_name,"table_name": table_name}) 
        
        return {
            'prompt_template_files': prompt_template_files,
            'replacements': replacements,
            'prompt_file_name': f"prompt_{table_name_lower}_2_dal.md",
            'prompt_folder_name': self.prompt_folder_name
        }

    def get_sam_template_params(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Return parameters for sam template prompt generation"""
        table_name = item.get('TABLE_NAME', '').strip()
        table_name_lower = self.cammel_to_snake(table_name)
        prompt_template_files = ['sam_template.txt']
        replacements_dict = {'{{ddb_table_name}}': table_name,
                             '{{ddb_table_pk}}': item.get('TABLE_PK', '')}
        
        ## Handle TABLE_SK
        table_sk = item.get('TABLE_SK', '')
        if table_sk != 'nan' and table_sk != '':
            replacements_dict['{{ddb_table_sk}}'] = f" and {table_sk} is sort/range key (sk) "
            replacements_dict['{{ddb_table_sk_string}}'] = f" and {table_sk}" 
        else:
            replacements_dict['{{ddb_table_sk}}'] = " and no sort/range key (sk) "
            replacements_dict['{{ddb_table_sk_string}}'] = ""

        ## Handle TIME_TO_LIVE
        is_time_to_live = item.get('TIME_TO_LIVE_REQUIRED', 'Yes').strip().lower()
        if is_time_to_live == 'yes':
            time_to_live_replacement = f"8a) time_to_live is time to live attribute in the table"
            replacements_dict['{{time_to_live_replacement}}'] = time_to_live_replacement
        else:
            replacements_dict['{{time_to_live_replacement}}'] = ""
            
        replacements = [replacements_dict]

        ## Handle GSI
        gsi_pks = item.get('GSI_PKs','').split(',')
        gsi_sks = item.get('GSI_SKs','').split(',')
        gsi_projections = item.get('GSI_PROJECTIONs','').split(',')

        logger.info(f"GSI PKs: {gsi_pks}, GSI SKs: {gsi_sks}, GSI PROJECTIONS: {gsi_projections}")
        
        if gsi_pks != ['nan']:
            for i in range(len(gsi_pks)):
                if gsi_pks[i]:
                    gsi_pk = gsi_pks[i].strip()
                    gsi_sk = gsi_sks[i].strip()
                    gsi_projection = gsi_projections[i].strip()

                    ## prompt_template_files append
                    prompt_template_files.append('sam_template_gsi.txt')
                    
                    ## replacements append
                    gsi_replacement_dict = {'{{sl_no}}': 9+i,
                                            '{{ddb_table_gsi_pk}}': gsi_pk}
                    ## Handle GSI SK
                    if gsi_sk != 'nan' and gsi_sk != '':
                        gsi_replacement_dict['{{ddb_table_gsi_sk_string}}'] = f" and {gsi_sk} as sort/range key (sk)"
                        gsi_replacement_dict['{{ddb_table_gsi_sk}}'] = "__" + gsi_sk
                        gsi_replacement_dict['{{ddb_table_gsi_sk_attr_def}}'] = f" and {gsi_sk} as sort/range key (sk) to attribute defination"
                    else:
                        gsi_replacement_dict['{{ddb_table_gsi_sk_string}}'] = "and no sort/range key (sk) "
                        gsi_replacement_dict['{{ddb_table_gsi_sk}}'] = ""
                        gsi_replacement_dict['{{ddb_table_gsi_sk_attr_def}}'] = ""

                    ## Handle GSI PROJECTIONS
                    if gsi_projection != 'nan' and gsi_projection != '':
                        gsi_replacement_dict['{{ddb_table_gsi_projection}}'] = gsi_projection.replace(" ", ", ").replace("(", "").replace(")", "").replace("~", " ")
                    else:
                        gsi_replacement_dict['{{ddb_table_gsi_projection}}'] = ""

                    replacements.append(gsi_replacement_dict)

                else:
                    break

        logger.info(f"SAM template prompt generator parameters",  
                    extra={"batch_id": self.batch_id,"excel_file_name": self.excel_file_name,"table_name": table_name}) 
        
        if self.library_name == 'pynamodb':
            prompt_file_name = f"prompt_{table_name_lower}_5_sam_template.md"
        elif self.library_name == 'boto3':
            prompt_file_name = f"prompt_{table_name_lower}_4_sam_template.md"

        return {
            'prompt_template_files': prompt_template_files,
            'replacements': replacements,
            'prompt_file_name': prompt_file_name,
            'prompt_folder_name': self.prompt_folder_name
        }

    def pynamodb_method(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Call PynamoDB methods and create list of dicts"""
        return [
            self.get_pynamodb_folder_structure_parmas(item),
            self.get_pynamodb_model_params(item),
            self.get_dto_parmas(item),
            self.get_pynamodb_dal_params(item),
            self.get_unit_test_params(item),
            self.get_sam_template_params(item)
        ]

    def boto3_method(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Call Boto3 methods and create list of dicts"""
        return [
            self.get_boto3_folder_structure_parmas(item),
            self.get_boto3_dto_params(item),
            self.get_boto3_dal_params(item),
            self.get_unit_test_params(item),
            self.get_sam_template_params(item)
        ]

    def get_prams(self) -> List[Dict[str, Any]]:
        """Main method to process items based on library_name"""
        items = self.dateframe.to_dict('records')
        logger.info(f"Items in the dataframe : {items}")
        all_params = []
        
        for item in items:
            if self.library_name == "pynamodb":
                all_params.extend(self.pynamodb_method(item))
            elif self.library_name == "boto3":
                all_params.extend(self.boto3_method(item))
                
        logger.info(f"Prompt file genration prameters : {all_params}")
        return all_params