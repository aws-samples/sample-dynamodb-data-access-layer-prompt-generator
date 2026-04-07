Table structure of DynamoDB table as below. This is information do not create any code or response for DynamoDB form 1) to 5):
1) ddb_table_two is the name of DynamoDB table
2) table with attributes pk_attr_str_1,sk_attr_str_2,attr_str_3,attr_str_4,attr_str_5,attr_str_6,gsi1pk_attr_str_7,gsi1sk_attr_str_8,gsi2pk_attr_str_9,gsi2sk_attr_str_10,gsi3pk_attr_str_11 of data type string,string,string,string,string,string,string,string,string,string,string respectively. table attributes pk_attr_str_1,sk_attr_str_2,attr_str_3,attr_str_4,attr_str_5,attr_str_6,gsi1pk_attr_str_7,gsi1sk_attr_str_8,gsi2pk_attr_str_9,gsi2sk_attr_str_10,gsi3pk_attr_str_11 have settings hash_key=True,null=False,null=False,null=False,null=False,null=False,null=False,null=False,null=False,null=False,null=True respectively
3) pk_attr_str_1 is the partition/hash key (pk) and sk_attr_str_2 is the sort/range key (sk) for base table
4)   time_to_live attribute is TTLattribute, version attribute is Version attribute for optimistic locking
5) Both base table and GSIs are OnDemand capacity
6) create GSI for gsi1pk_attr_str_7 as partition/hash key (pk) and gsi1sk_attr_str_8 as sort/range key (sk). only pk_attr_str_1, sk_attr_str_2, attr_str_3, attr_str_4, attr_str_5 in GSI. index name to be of the format gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index.
7) create GSI for gsi2pk_attr_str_9 as partition/hash key (pk) and gsi2sk_attr_str_10 as sort/range key (sk). only keys projected in GSI. index name to be of the format gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index.
8) create GSI for gsi3pk_attr_str_11 as partition/hash key (pk)and no sort/range key (sk) . only all attributes projected in GSI. index name to be of the format gsi__gsi3pk_attr_str_11__index.
Follow below step-by-step to create data transfer object(dto) for ddb_table_two DynamoDB table in separate file in dto folder. Code has to created here:
1) ddb_table_twoDto is data transfer object name
2) exclude{{created_at_replacement}}{{updated_at_replacement}}{{time_to_live_replacement_a}} version attributes in ddb_table_twoDto
3) Class should have __init__ and __str__ functions. __str__ function creates dict with table attributes and converts to string using json.dumps and returns string