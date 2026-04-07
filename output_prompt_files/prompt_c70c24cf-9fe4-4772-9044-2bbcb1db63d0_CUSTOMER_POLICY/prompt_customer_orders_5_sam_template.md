Generate SAM template for DynamoDB table follow step by step
1) Create SAM template.yaml file in PROJECT folder. Create one sam template file which includes code for all the tables.
2) Take the StackTag, ComponentCode, AppCode as parameters
3) Create DynamoDB table resource CustomerOrders 
4) string data type attribute definition for pk and sk
5) pk is Partition/Hash key and sk is sort/range key (sk) 
6) Table should be PAY_PER_REQUEST. Encrypt the table with KMS Customer Managed CMK which is provided as parameter. TableName property should not be provided.
7) Add StackTag, ComponentCode, AppCode as tags to resource. AWS Account ID part of the tags.
8) server side encryption enabled and PITR enabled. Exclude key attributes in GSI projections(NonKeyAtributes) if any.
8a) time_to_live is time to live attribute in the table
9) create GSI for gsipk1 as partition/hash key (pk) and gsisk1 as sort/range key (sk). only order_date, order_quantity, sku in GSI. index name to be of the format gsi__gsipk1__gsisk1__index. add gsipk1 as partition/hash key (pk) and gsisk1 as sort/range key (sk) to attribute defination. both attributes are string