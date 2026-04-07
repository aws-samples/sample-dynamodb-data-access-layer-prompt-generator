Table structure of DynamoDB table as below. This is information do not create any code or response for DynamoDB form 1) to 5):
1) CustomerOrders is the name of DynamoDB table
2) table with attributes pk,sk,cust_address,first_name,last_name,phone,is_premium_member,order_date,order_quantity,sku,gsipk1,gsisk1 of data type string,string,string,string,string,string,string,string,number,string,string,string respectively. table attributes pk,sk,cust_address,first_name,last_name,phone,is_premium_member,order_date,order_quantity,sku,gsipk1,gsisk1 have settings hash_key=True,range_key=True,null=False,null=False,null=True,null=True,null=True,null=True,null=True,null=True,null=True,null=True respectively
3) pk is the partition/hash key (pk) and sk is the sort/range key (sk) for base table
4) created_at is additional string attributes, updated_at is additional string attributes, time_to_live attribute is TTLattribute, version attribute is Version attribute for optimistic locking
5) Both base table and GSIs are OnDemand capacity
6) create GSI for gsipk1 as partition/hash key (pk) and gsisk1 as sort/range key (sk). only order_date, order_quantity, sku in GSI. index name to be of the format gsi__gsipk1__gsisk1__index.
Follow below step-by-step to create data transfer object(dto) for CustomerOrders DynamoDB table in separate file in dto folder. Code has to created here:
1) CustomerOrdersDto is data transfer object name
2) exclude{{created_at_replacement}}{{updated_at_replacement}}{{time_to_live_replacement_a}} version attributes in CustomerOrdersDto
3) Class should have __init__ and __str__ functions. __str__ function creates dict with table attributes and converts to string using json.dumps and returns string