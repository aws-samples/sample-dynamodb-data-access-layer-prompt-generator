Follow below step-by-step to create data transfer object(dto) for ddb_table_two DynamoDB table in separate file in dto folder. Code has to created here:
1) ddb_table_twoDto is data transfer object name
2) exclude time_to_live, version attributes in ddb_table_twoDto
3) Class should have __init__ and __str__ functions. __str__ function creates dict with table attributes and converts to string using json.dumps and returns string