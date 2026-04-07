import pandas as pd
import os
import uuid
import logging

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ExcelValidationError(Exception):
    """Custom exception for Excel data validation errors"""
    pass

class ExcelToDateframe:
    def __init__(self, excel_file_name: str):
        """
        Initialize excel_file
        """
        self.batch_id = str(uuid.uuid4())
        self.excel_file_name = excel_file_name

    def validate_data(self, df: pd.DataFrame) -> None:
        """
        Validate the Excel data
        """
        try:
            # Check if TABLE_NAME is unique
            if df['TABLE_NAME'].duplicated().any():
                duplicate_tables = df[df['TABLE_NAME'].duplicated()]['TABLE_NAME'].tolist()
                error_msg = f"Duplicate TABLE_NAME found: {duplicate_tables}"
                logger.error(error_msg, extra={"batch_id": self.batch_id})
                raise ExcelValidationError(error_msg)

            # Check if TABLE_PK is not empty
            empty_pk_records = df[df['TABLE_PK'].isna() | (df['TABLE_PK'] == '')]
            if not empty_pk_records.empty:
                error_msg = f"Empty TABLE_PK found in records: {empty_pk_records.index.tolist()}"
                logger.error(error_msg, extra={"batch_id": self.batch_id})
                raise ExcelValidationError(error_msg)

            # Check comma count match in ATTRIBUTES, ATTRIBUTE_DATA_TYPES and ATTRIBUTE_DEFAULT_NULL
            # check CREATED_AT_REQUIRED, UPDATED_AT_REQUIRED, TIME_TO_LIVE_REQUIRED are either 'Yes' or 'No'
            for index, row in df.iterrows():
                attr_count = row['ATTRIBUTES'].count(',') + 1 if pd.notna(row['ATTRIBUTES']) else 0
                type_count = row['ATTRIBUTE_DATA_TYPES'].count(',') + 1 if pd.notna(row['ATTRIBUTE_DATA_TYPES']) else 0
                null_count = row['ATTRIBUTE_DEFAULT_NULL'].count(',') + 1 if pd.notna(row['ATTRIBUTE_DEFAULT_NULL']) else 0
                created_at_required = row['CREATED_AT_REQUIRED'].lower()
                updated_at_required = row['UPDATED_AT_REQUIRED'].lower()
                time_to_live_required = row['TIME_TO_LIVE_REQUIRED'].lower()
                gsi_pk_count = row['GSI_PKs'].count(',') + 1 if pd.notna(row['GSI_PKs']) else 0
                gsi_sk_count = row['GSI_SKs'].count(',') + 1 if pd.notna(row['GSI_SKs']) else 0
                gsi_projection_count = row['GSI_PROJECTIONs'].count(',') + 1 if pd.notna(row['GSI_PROJECTIONs']) else 0
                gsi_projection_fp_count = row['GSI_PROJECTIONs'].count('(') if pd.notna(row['GSI_PROJECTIONs']) else 0
                gsi_projection_bp_count = row['GSI_PROJECTIONs'].count(')') if pd.notna(row['GSI_PROJECTIONs']) else 0

                if not (attr_count == type_count == null_count):
                    error_msg = (f"Mismatch in comma count for record {row['TABLE_NAME']}. "
                               f"ATTRIBUTES: {attr_count}, "
                               f"ATTRIBUTE_DATA_TYPES: {type_count}, "
                               f"ATTRIBUTE_DEFAULT_NULL: {null_count}")
                    logger.error(error_msg, extra={
                        "batch_id": self.batch_id,
                        "record_index": index
                    })
                    raise ExcelValidationError("Check ATTRIBUTES, ATTRIBUTE_DATA_TYPES and ATTRIBUTE_DEFAULT_NULL are correct")
                
                if not (gsi_pk_count == gsi_sk_count == gsi_projection_count == gsi_projection_fp_count == gsi_projection_bp_count):
                    error_msg = (f"Mismatch in comma count for record {row['TABLE_NAME']}. "
                               f"GSI_PKs: {gsi_pk_count}, "
                               f"GSI_SKs: {gsi_sk_count}, "
                               f"GSI_PROJECTIONs: {gsi_projection_count}, "
                               f"GSI_PROJECTIONs '(': {gsi_projection_fp_count}, "
                               f"GSI_PROJECTIONs ')': {gsi_projection_bp_count}")
                    logger.error(error_msg, extra={
                        "batch_id": self.batch_id,
                        "record_index": index
                    })
                    raise ExcelValidationError("Check GSI_PKs, GSI_SKs and GSI_PROJECTIONs are correct")
                
                # Validate LSI columns have matching comma counts
                lsi_sk_count = row['LSI_SKs'].count(',') + 1 if pd.notna(row['LSI_SKs']) else 0
                lsi_projection_count = row['LSI_PROJECTIONs'].count(',') + 1 if pd.notna(row['LSI_PROJECTIONs']) else 0
                lsi_projection_fp_count = row['LSI_PROJECTIONs'].count('(') if pd.notna(row['LSI_PROJECTIONs']) else 0
                lsi_projection_bp_count = row['LSI_PROJECTIONs'].count(')') if pd.notna(row['LSI_PROJECTIONs']) else 0

                if not (lsi_sk_count == lsi_projection_count == lsi_projection_fp_count == lsi_projection_bp_count):
                    error_msg = (f"Mismatch in comma count for record {row['TABLE_NAME']}. "
                               f"LSI_SKs: {lsi_sk_count}, "
                               f"LSI_PROJECTIONs: {lsi_projection_count}, "
                               f"LSI_PROJECTIONs '(': {lsi_projection_fp_count}, "
                               f"LSI_PROJECTIONs ')': {lsi_projection_bp_count}")
                    logger.error(error_msg, extra={
                        "batch_id": self.batch_id,
                        "record_index": index
                    })
                    raise ExcelValidationError("Check LSI_SKs and LSI_PROJECTIONs are correct")
                
                if created_at_required not in ['yes', 'no']:
                    error_msg = f"Invalid value for CREATED_AT_REQUIRED in record {row['TABLE_NAME']}: {row['CREATED_AT_REQUIRED']}"
                    logger.error(error_msg, extra={
                        "batch_id": self.batch_id,
                        "record_index": index
                    })
                    raise ExcelValidationError("CREATED_AT_REQUIRED must be either 'Yes' or 'No'")
               
                if updated_at_required not in ['yes', 'no']:
                    error_msg = f"Invalid value for UPDATED_AT_REQUIRED in record {row['TABLE_NAME']}: {row['UPDATED_AT_REQUIRED']}"
                    logger.error(error_msg, extra={
                        "batch_id": self.batch_id,
                        "record_index": index
                    })
                    raise ExcelValidationError("UPDATED_AT_REQUIRED must be either 'Yes' or 'No'")

                if time_to_live_required not in ['yes', 'no']:
                    error_msg = f"Invalid value for TIME_TO_LIVE_REQUIRED in record {row['TABLE_NAME']}: {row['TIME_TO_LIVE_REQUIRED']}"
                    logger.error(error_msg, extra={
                        "batch_id": self.batch_id,
                        "record_index": index
                    })
                    raise ExcelValidationError("TIME_TO_LIVE_REQUIRED must be either 'Yes' or 'No'")

            logger.info("Data validation successful", extra={"batch_id": self.batch_id})

        except ExcelValidationError:
            raise
        except Exception as e:
            logger.error("Validation error", extra={
                "error": str(e),
                "batch_id": self.batch_id
            })
            raise

    def read_from_excel(self):
        """
        Read Excel file from S3 and return DataFrame
        """
        try:
            logger.info("Reading excel file", extra={
                "file_name": self.excel_file_name,
                "batch_id": self.batch_id
            })
            
            excel_path = os.path.join("input_specification_files", self.excel_file_name)
            df = pd.read_excel(excel_path, sheet_name=0)
            
            # Validate data before processing
            self.validate_data(df)
            
            # Convert all columns to string
            df = df.astype(str)
            
            # Add batch_id column
            df['batch_id'] = self.batch_id
            
            logger.info("Successfully read and validated Excel file", extra={
                "record_count": len(df),
                "batch_id": self.batch_id
            })
            
            return df, self.batch_id
            
        except Exception as e:
            logger.error("Error reading from Excel File", extra={
                "error": str(e),
                "batch_id": self.batch_id
            })
            raise
