Generate SAM template for DynamoDB table follow step by step
1) Create SAM template.yaml file in PROJECT folder. Create one sam template file which includes code for all the tables.
2) Take the StackTag, ComponentCode, AppCode as parameters
3) Create DynamoDB table resource ddb_table_two 
4) string data type attribute definition for pk_attr_str_1 and sk_attr_str_2
5) pk_attr_str_1 is Partition/Hash key and sk_attr_str_2 is sort/range key (sk) 
6) Table should be PAY_PER_REQUEST. Encrypt the table with KMS Customer Managed CMK which is provided as parameter. TableName property should not be provided.
7) Add StackTag, ComponentCode, AppCode as tags to resource. AWS Account ID part of the tags.
8) server side encryption enabled and PITR enabled. Exclude key attributes in GSI projections(NonKeyAtributes) if any.
8a) time_to_live is time to live attribute in the table