Follow below step-by-step to create pynamodb model for PolicyManager DynamoDB table in separate file in model folder:
0) Import datetime and timedelta from datetime, pynamodb
1) Create pynamodb model only not methods for the PolicyManager DynamoDB table
2) Table name coming from environment variable POLICY_MANAGER using python os library or hard code to "PolicyManager"
3) region coming from environment variable AWS_REGION using python os library or hard code "us-east-1" 
4) table with pk,sk,policy_id,policy_name,risk_id,address,premium,sum_insured,policy_version,start_date,end_date,limit,deductible,amount,gsipk1,gsisk1,gsipk2,gsisk2 of data type string,string,string,string,string,string,number,number,string,string,string,number,string,number,string,string,string,string respectively. table attributes pk,sk,policy_id,policy_name,risk_id,address,premium,sum_insured,policy_version,start_date,end_date,limit,deductible,amount,gsipk1,gsisk1,gsipk2,gsisk2 have settings hash_key=True,range_key=True,null=False,null=False,null=False,null=False,null=False,null=False,null=False,null=False,null=True,null=True,null=True,null=True,null=True,null=True,null=True,null=True respectively
5) pk is the partition/hash key (pk) and sk is the sort/range key (sk) for base table. set null = True for all the attributes except pk, sk, version, time_to_live 
6) created_at is additional string attributes, updated_at is additional string attributes, time_to_live attribute is TTLattribute with null=True, version attribute is Versionattribute
7) Both base table and GSIs are OnDemand capacity. Create GSI class in table's model class
8) create save method. updated_at is set to datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z') (if updated_at not part of dto). save method should not have conditional_operator and **expected_values as parameters
8a) create method get_ttl_days to get environment variable POLICY_MANAGER_TTL_DAYS or 0 if null
8b) create method get_test_ttl_days to get environment variable POLICY_MANAGER_TEST_TTL_DAYS or 30 if null
8c) create get_ttl method which checks is_test_object(method which returns true/false if partition key of the table has string TEST) if true return timedelta(get_test_ttl_days()). if false then get ttl_days from get_ttl_days() method if ttl_days != 0 then return timedelta(ttl_days) else return None. In save method to set time_to_live attribute equal to get_ttl()
9) create GSI for gsipk1 as partition/hash key (pk) and gsisk1 as sort/range key (sk). only policy_name, risk_id, address, premium, sum_insured, policy_version, start_date, end_date in GSI. index name to be of the format gsi__gsipk1__gsisk1__index.
10) create GSI for gsipk2 as partition/hash key (pk) and gsisk2 as sort/range key (sk). only keys projected in GSI. index name to be of the format gsi__gsipk2__gsisk2__index.
3) create LSI for policy_version as sort/range key (sk). only policy_name, risk_id in LSI. index name to be of the format lsi__policy_version__index.