Follow below step-by-step to create data transfer object(dto) for ddb_table_one DynamoDB table in separate file in dto folder. Code has to created here:
1) ddb_table_oneDto is data transfer object name
2) exclude created_at, updated_at, time_to_live, version attributes in ddb_table_oneDto
3) Class should have __init__ and __str__ functions. __str__ function creates dict with table attributes and converts to string using json.dumps and returns string