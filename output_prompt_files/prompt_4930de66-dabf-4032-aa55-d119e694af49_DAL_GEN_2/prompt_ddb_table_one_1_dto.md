Table structure of DynamoDB table as below. This is information do not create any code or response for DynamoDB form 1) to 5):
1) ddb_table_one is the name of DynamoDB table
2) table with attributes pk_attribute_str_1,sk_attribute_str_2,attribute_str_3,attribute_str_4,attribute_num_5,attribute_map_6 of data type string,string,string,string,number,map respectively. table attributes pk_attribute_str_1,sk_attribute_str_2,attribute_str_3,attribute_str_4,attribute_num_5,attribute_map_6 have settings hash_key=True,null=False,null=False,null=False,null=True,null=True respectively
3) pk_attribute_str_1 is the partition/hash key (pk) and sk_attribute_str_2 is the sort/range key (sk) for base table
4) created_at is additional string attributes, updated_at is additional string attributes, time_to_live attribute is TTLattribute, version attribute is Version attribute for optimistic locking
5) Both base table and GSIs are OnDemand capacity
Follow below step-by-step to create data transfer object(dto) for ddb_table_one DynamoDB table in separate file in dto folder. Code has to created here:
1) ddb_table_oneDto is data transfer object name
2) exclude{{created_at_replacement}}{{updated_at_replacement}}{{time_to_live_replacement_a}} version attributes in ddb_table_oneDto
3) Class should have __init__ and __str__ functions. __str__ function creates dict with table attributes and converts to string using json.dumps and returns string