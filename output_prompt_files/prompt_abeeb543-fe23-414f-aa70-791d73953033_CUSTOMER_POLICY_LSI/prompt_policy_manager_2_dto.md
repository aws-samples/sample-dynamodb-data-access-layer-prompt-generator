Follow below step-by-step to create data transfer object(dto) for PolicyManager DynamoDB table in separate file in dto folder. Code has to created here:
1) PolicyManagerDto is data transfer object name
2) exclude created_at, updated_at, time_to_live, version attributes in PolicyManagerDto
3) Class should have __init__ and __str__ functions. __str__ function creates dict with table attributes and converts to string using json.dumps and returns string