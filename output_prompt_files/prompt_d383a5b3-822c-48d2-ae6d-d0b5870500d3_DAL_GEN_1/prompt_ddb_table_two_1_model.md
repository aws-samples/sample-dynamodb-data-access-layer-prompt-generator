Follow below step-by-step to create pynamodb model for ddb_table_two DynamoDB table in separate file in model folder:
0) Import datetime and timedelta from datetime, pynamodb
1) Create pynamodb model only not methods for the ddb_table_two DynamoDB table
2) Table name coming from environment variable DDB_TABLE_TWO using python os library or hard code to "ddb_table_two"
3) region coming from environment variable AWS_REGION using python os library or hard code "us-east-1" 
4) table with pk_attr_str_1,sk_attr_str_2,attr_str_3,attr_str_4,attr_str_5,attr_str_6,gsi1pk_attr_str_7,gsi1sk_attr_str_8,gsi2pk_attr_str_9,gsi2sk_attr_str_10,gsi3pk_attr_str_11 of data type string,string,string,string,string,string,string,string,string,string,string respectively. table attributes pk_attr_str_1,sk_attr_str_2,attr_str_3,attr_str_4,attr_str_5,attr_str_6,gsi1pk_attr_str_7,gsi1sk_attr_str_8,gsi2pk_attr_str_9,gsi2sk_attr_str_10,gsi3pk_attr_str_11 have settings hash_key=True,null=False,null=False,null=False,null=False,null=False,null=False,null=False,null=False,null=False,null=True respectively
5) pk_attr_str_1 is the partition/hash key (pk) and sk_attr_str_2 is the sort/range key (sk) for base table. set null = True for all the attributes except pk, sk, version, time_to_live 
6) time_to_live attribute is TTLattribute with null=True, version attribute is Versionattribute
7) Both base table and GSIs are OnDemand capacity. Create GSI class in table's model class
8) create save method. save method should not have conditional_operator and **expected_values as parameters
8a) create method get_ttl_days to get environment variable DDB_TABLE_TWO_TTL_DAYS or 0 if null
8b) create method get_test_ttl_days to get environment variable DDB_TABLE_TWO_TEST_TTL_DAYS or 30 if null
8c) create get_ttl method which checks is_test_object(method which returns true/false if partition key of the table has string TEST) if true return timedelta(get_test_ttl_days()). if false then get ttl_days from get_ttl_days() method if ttl_days != 0 then return timedelta(ttl_days) else return None. In save method to set time_to_live attribute equal to get_ttl()
9) create GSI for gsi1pk_attr_str_7 as partition/hash key (pk) and gsi1sk_attr_str_8 as sort/range key (sk). only pk_attr_str_1, sk_attr_str_2, attr_str_3, attr_str_4, attr_str_5 in GSI. index name to be of the format gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index.
10) create GSI for gsi2pk_attr_str_9 as partition/hash key (pk) and gsi2sk_attr_str_10 as sort/range key (sk). only keys projected in GSI. index name to be of the format gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index.
11) create GSI for gsi3pk_attr_str_11 as partition/hash key (pk)and no sort/range key (sk) . only all attributes projected in GSI. index name to be of the format gsi__gsi3pk_attr_str_11__index.