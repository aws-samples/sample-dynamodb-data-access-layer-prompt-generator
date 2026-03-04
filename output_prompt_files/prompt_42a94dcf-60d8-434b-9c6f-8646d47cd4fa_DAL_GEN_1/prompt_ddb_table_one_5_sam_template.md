Generate SAM template for DynamoDB table follow step by step
1) Create SAM template.yaml file in PROJECT folder. Create one sam template file which includes code for all the tables.
2) Take the StackTag, ComponentCode, AppCode as parameters
3) Create DynamoDB table resource ddb_table_one 
4) string data type attribute definition for pk_attribute_str_1 and sk_attribute_str_2
5) pk_attribute_str_1 is Partition/Hash key and sk_attribute_str_2 is sort/range key (sk) 
6) Table should be PAY_PER_REQUEST
7) Add StackTag, ComponentCode, AppCode as tags to resource
8) server side encryption enabled and PITR enabled. Exclude key attributes in GSI projections(NonKeyAtributes) if any.
8a) time_to_live is time to live attribute in the table
9) create GSI for gsipk_attribute_str_7 as partition/hash key (pk) and gsisk_attribute_str_8 as sort/range key (sk). only pk_attribute_str_1, sk_attribute_str_2, attribute_str_3, attribute_str_4, attribute_num_5 in GSI. index name to be of the format gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index. add gsipk_attribute_str_7 as partition/hash key (pk) and gsisk_attribute_str_8 as sort/range key (sk) to attribute defination. both attributes are string