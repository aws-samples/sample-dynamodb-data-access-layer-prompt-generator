Generate SAM template for DynamoDB table follow step by step
1) Create SAM template.yaml file in PROJECT folder. Create one sam template file which includes code for all the tables.
2) Take the StackTag, ComponentCode, AppCode as parameters
3) Create DynamoDB table resource ddb_table_two 
4) string data type attribute definition for pk_attr_str_1 and sk_attr_str_2
5) pk_attr_str_1 is Partition/Hash key and sk_attr_str_2 is sort/range key (sk) 
6) Table should be PAY_PER_REQUEST
7) Add StackTag, ComponentCode, AppCode as tags to resource
8) server side encryption enabled and PITR enabled. Exclude key attributes in GSI projections(NonKeyAtributes) if any.
8a) time_to_live is time to live attribute in the table
9) create GSI for gsi1pk_attr_str_7 as partition/hash key (pk) and gsi1sk_attr_str_8 as sort/range key (sk). only pk_attr_str_1, sk_attr_str_2, attr_str_3, attr_str_4, attr_str_5 in GSI. index name to be of the format gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index. add gsi1pk_attr_str_7 as partition/hash key (pk) and gsi1sk_attr_str_8 as sort/range key (sk) to attribute defination. both attributes are string
10) create GSI for gsi2pk_attr_str_9 as partition/hash key (pk) and gsi2sk_attr_str_10 as sort/range key (sk). only keys projected in GSI. index name to be of the format gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index. add gsi2pk_attr_str_9 as partition/hash key (pk) and gsi2sk_attr_str_10 as sort/range key (sk) to attribute defination. both attributes are string
11) create GSI for gsi3pk_attr_str_11 as partition/hash key (pk)and no sort/range key (sk) . only all attributes projected in GSI. index name to be of the format gsi__gsi3pk_attr_str_11__index. add gsi3pk_attr_str_11 as partition/hash key (pk). both attributes are string