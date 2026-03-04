import os
import boto3
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
from decimal import Decimal
from retry import retry
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from aws_lambda_powertools.logging.logger import Logger 
logger = Logger()

from data.dto.policy_manager_dto import PolicyManagerDto

# Configuration
TABLE_NAME = os.environ.get('POLICY_MANAGER', 'PolicyManager')
REGION = os.environ.get('AWS_REGION', 'us-east-1')
RETRY_ATTEMPTS = 3
BACKOFF_DELAY = 2

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(TABLE_NAME)


def get_ttl_days() -> int:
    """
    Get TTL days from environment variable or default to 30.
    
    Returns:
        int: Number of days for TTL
    """
    ttl_days = os.environ.get('POLICY_MANAGER_TTL_DAYS', '30')
    return int(ttl_days) if ttl_days else 30


def get_test_ttl_days() -> int:
    """
    Get test TTL days from environment variable or default to 30.
    
    Returns:
        int: Number of days for test TTL
    """
    test_ttl_days = os.environ.get('POLICY_MANAGER_TEST_TTL_DAYS', '30')
    return int(test_ttl_days) if test_ttl_days else 30


def is_test_object(partition_key: str) -> bool:
    """
    Check if the partition key contains 'TEST' string (case-insensitive).
    
    Args:
        partition_key (str): Partition/hash key value to check
        
    Returns:
        bool: True if partition key contains 'TEST', False otherwise
    """
    if not partition_key:
        return False
    
    return 'TEST' in partition_key.upper()


def get_ttl(pk: str) -> Optional[int]:
    """
    Get TTL timestamp based on whether object is a test object.
    
    Args:
        pk (str): Partition key to check
        
    Returns:
        Optional[int]: Unix timestamp for TTL
    """
    if is_test_object(pk):
        ttl_days = get_test_ttl_days()
    else:
        ttl_days = get_ttl_days()
    
    ttl_datetime = datetime.now(timezone.utc) + timedelta(days=ttl_days)
    return int(ttl_datetime.timestamp())


def dto_to_item(dto: PolicyManagerDto) -> Dict[str, Any]:
    """
    Convert DTO to DynamoDB item dictionary.
    
    Args:
        dto (PolicyManagerDto): DTO instance to convert
        
    Returns:
        Dict[str, Any]: DynamoDB item dictionary
    """
    item = {}
    if dto.pk is not None:
        item['pk'] = dto.pk
    if dto.sk is not None:
        item['sk'] = dto.sk
    if dto.policy_id is not None:
        item['policy_id'] = dto.policy_id
    if dto.policy_name is not None:
        item['policy_name'] = dto.policy_name
    if dto.risk_id is not None:
        item['risk_id'] = dto.risk_id
    if dto.address is not None:
        item['address'] = dto.address
    if dto.premium is not None:
        item['premium'] = Decimal(str(dto.premium))
    if dto.sum_insured is not None:
        item['sum_insured'] = Decimal(str(dto.sum_insured))
    if dto.policy_version is not None:
        item['policy_version'] = dto.policy_version
    if dto.start_date is not None:
        item['start_date'] = dto.start_date
    if dto.end_date is not None:
        item['end_date'] = dto.end_date
    if dto.limit is not None:
        item['limit'] = Decimal(str(dto.limit))
    if dto.deductible is not None:
        item['deductible'] = dto.deductible
    if dto.amount is not None:
        item['amount'] = Decimal(str(dto.amount))
    if dto.gsipk1 is not None:
        item['gsipk1'] = dto.gsipk1
    if dto.gsisk1 is not None:
        item['gsisk1'] = dto.gsisk1
    if dto.gsipk2 is not None:
        item['gsipk2'] = dto.gsipk2
    if dto.gsisk2 is not None:
        item['gsisk2'] = dto.gsisk2
    return item


def item_to_dto(item: Dict[str, Any]) -> PolicyManagerDto:
    """
    Convert DynamoDB item to DTO.
    
    Args:
        item (Dict[str, Any]): DynamoDB item dictionary
        
    Returns:
        PolicyManagerDto: DTO instance
    """
    return PolicyManagerDto(
        pk=item.get('pk'),
        sk=item.get('sk'),
        policy_id=item.get('policy_id'),
        policy_name=item.get('policy_name'),
        risk_id=item.get('risk_id'),
        address=item.get('address'),
        premium=int(item['premium']) if 'premium' in item else None,
        sum_insured=int(item['sum_insured']) if 'sum_insured' in item else None,
        policy_version=item.get('policy_version'),
        start_date=item.get('start_date'),
        end_date=item.get('end_date'),
        limit=int(item['limit']) if 'limit' in item else None,
        deductible=item.get('deductible'),
        amount=int(item['amount']) if 'amount' in item else None,
        gsipk1=item.get('gsipk1'),
        gsisk1=item.get('gsisk1'),
        gsipk2=item.get('gsipk2'),
        gsisk2=item.get('gsisk2')
    )


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def policy_manager_put_item(dto: PolicyManagerDto) -> None:
    """
    Put an item into the table.
    
    Args:
        dto (PolicyManagerDto): DTO containing item data
        
    Raises:
        ClientError: If put operation fails
        Exception: If any other error occurs
    """
    try:
        item = dto_to_item(dto)
        item['created_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        table.put_item(Item=item)
        logger.info("Successfully put item %s %s", dto.pk, dto.sk)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning("Conditional check failed for item %s %s: %s", dto.pk, dto.sk, e)
        else:
            logger.error("Error putting item %s %s: %s", dto.pk, dto.sk, e)
            raise
    except Exception as e:
        logger.error("Error putting item %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def policy_manager_update_item(dto: PolicyManagerDto) -> None:
    """
    Update an item in the table.
    
    Args:
        dto (PolicyManagerDto): DTO containing updated item data
        
    Raises:
        ClientError: If update operation fails
        Exception: If any other error occurs
    """
    try:
        update_expression_parts = []
        expression_attribute_values = {}
        expression_attribute_names = {}
        
        if dto.policy_id is not None:
            update_expression_parts.append("#policy_id = :policy_id")
            expression_attribute_values[':policy_id'] = dto.policy_id
            expression_attribute_names['#policy_id'] = 'policy_id'
        if dto.policy_name is not None:
            update_expression_parts.append("#policy_name = :policy_name")
            expression_attribute_values[':policy_name'] = dto.policy_name
            expression_attribute_names['#policy_name'] = 'policy_name'
        if dto.risk_id is not None:
            update_expression_parts.append("#risk_id = :risk_id")
            expression_attribute_values[':risk_id'] = dto.risk_id
            expression_attribute_names['#risk_id'] = 'risk_id'
        if dto.address is not None:
            update_expression_parts.append("#address = :address")
            expression_attribute_values[':address'] = dto.address
            expression_attribute_names['#address'] = 'address'
        if dto.premium is not None:
            update_expression_parts.append("#premium = :premium")
            expression_attribute_values[':premium'] = Decimal(str(dto.premium))
            expression_attribute_names['#premium'] = 'premium'
        if dto.sum_insured is not None:
            update_expression_parts.append("#sum_insured = :sum_insured")
            expression_attribute_values[':sum_insured'] = Decimal(str(dto.sum_insured))
            expression_attribute_names['#sum_insured'] = 'sum_insured'
        if dto.policy_version is not None:
            update_expression_parts.append("#policy_version = :policy_version")
            expression_attribute_values[':policy_version'] = dto.policy_version
            expression_attribute_names['#policy_version'] = 'policy_version'
        if dto.start_date is not None:
            update_expression_parts.append("#start_date = :start_date")
            expression_attribute_values[':start_date'] = dto.start_date
            expression_attribute_names['#start_date'] = 'start_date'
        if dto.end_date is not None:
            update_expression_parts.append("#end_date = :end_date")
            expression_attribute_values[':end_date'] = dto.end_date
            expression_attribute_names['#end_date'] = 'end_date'
        if dto.limit is not None:
            update_expression_parts.append("#limit = :limit")
            expression_attribute_values[':limit'] = Decimal(str(dto.limit))
            expression_attribute_names['#limit'] = 'limit'
        if dto.deductible is not None:
            update_expression_parts.append("#deductible = :deductible")
            expression_attribute_values[':deductible'] = dto.deductible
            expression_attribute_names['#deductible'] = 'deductible'
        if dto.amount is not None:
            update_expression_parts.append("#amount = :amount")
            expression_attribute_values[':amount'] = Decimal(str(dto.amount))
            expression_attribute_names['#amount'] = 'amount'
        if dto.gsipk1 is not None:
            update_expression_parts.append("#gsipk1 = :gsipk1")
            expression_attribute_values[':gsipk1'] = dto.gsipk1
            expression_attribute_names['#gsipk1'] = 'gsipk1'
        if dto.gsisk1 is not None:
            update_expression_parts.append("#gsisk1 = :gsisk1")
            expression_attribute_values[':gsisk1'] = dto.gsisk1
            expression_attribute_names['#gsisk1'] = 'gsisk1'
        if dto.gsipk2 is not None:
            update_expression_parts.append("#gsipk2 = :gsipk2")
            expression_attribute_values[':gsipk2'] = dto.gsipk2
            expression_attribute_names['#gsipk2'] = 'gsipk2'
        if dto.gsisk2 is not None:
            update_expression_parts.append("#gsisk2 = :gsisk2")
            expression_attribute_values[':gsisk2'] = dto.gsisk2
            expression_attribute_names['#gsisk2'] = 'gsisk2'
        
        # Add updated_at
        update_expression_parts.append("#updated_at = :updated_at")
        expression_attribute_values[':updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        expression_attribute_names['#updated_at'] = 'updated_at'
        
        update_expression = "SET " + ", ".join(update_expression_parts)
        
        table.update_item(
            Key={'pk': dto.pk, 'sk': dto.sk},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names
        )
        logger.info("Successfully updated item %s %s", dto.pk, dto.sk)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning("Conditional check failed for item %s %s: %s", dto.pk, dto.sk, e)
        else:
            logger.error("Error updating item %s %s: %s", dto.pk, dto.sk, e)
            raise
    except Exception as e:
        logger.error("Error updating item %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def policy_manager_get_item(pk: str, sk: str) -> Optional[PolicyManagerDto]:
    """
    Get an item from the table.
    
    Args:
        pk (str): Partition/hash key
        sk (str): Sort/range key
        
    Returns:
        Optional[PolicyManagerDto]: DTO if item found, None otherwise
        
    Raises:
        Exception: If any error occurs during get operation
    """
    try:
        response = table.get_item(Key={'pk': pk, 'sk': sk})
        if 'Item' in response:
            dto = item_to_dto(response['Item'])
            logger.info("Successfully got item %s %s", pk, sk)
            return dto
        else:
            logger.warning("Item not found %s %s", pk, sk)
            return None
    except ClientError as e:
        logger.error("Error getting item %s %s: %s", pk, sk, e)
        raise
    except Exception as e:
        logger.error("Error getting item %s %s: %s", pk, sk, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def policy_manager_delete_item(pk: str, sk: str) -> None:
    """
    Delete an item from the table.
    
    Args:
        pk (str): Partition/hash key
        sk (str): Sort/range key
        
    Raises:
        ClientError: If delete operation fails
        Exception: If any other error occurs
    """
    try:
        table.delete_item(Key={'pk': pk, 'sk': sk})
        logger.info("Successfully deleted item %s %s", pk, sk)
    except ClientError as e:
        logger.error("Error deleting item %s %s: %s", pk, sk, e)
        raise
    except Exception as e:
        logger.error("Error deleting item %s %s: %s", pk, sk, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def policy_manager_create_or_update_item(dto: PolicyManagerDto) -> None:
    """
    Create or update an item depending on existence.
    
    Args:
        dto (PolicyManagerDto): DTO containing item data
        
    Raises:
        ClientError: If save operation fails
        Exception: If any other error occurs
    """
    try:
        existing_item = policy_manager_get_item(dto.pk, dto.sk)
        
        if existing_item is not None:
            # Update path
            logger.info("Updating existing item %s %s", dto.pk, dto.sk)
            policy_manager_update_item(dto)
        else:
            # Create path
            logger.info("Creating new item %s %s", dto.pk, dto.sk)
            item = dto_to_item(dto)
            item['created_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            item['updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            table.put_item(Item=item)
        
        logger.info("Successfully created or updated %s %s", dto.pk, dto.sk)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning("Conditional check failed for item %s %s: %s", dto.pk, dto.sk, e)
        else:
            logger.error("Error create/update %s %s: %s", dto.pk, dto.sk, e)
            raise
    except Exception as e:
        logger.error("Error create/update %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def policy_manager_query(hash_key: str, range_key_and_condition=None, filter_key_condition=None,
                        consistent_read: bool = False, index_name: str = None, query_limit: int = None,
                        scan_index_forward: bool = True, attribute_to_get=None, last_evaluated_key: dict = None) -> List[PolicyManagerDto]:
    """
    Query the table using specified parameters until all pages are retrieved.
    
    Args:
        hash_key (str): Hash key value to query
        range_key_and_condition: Optional range key condition (boto3.dynamodb.conditions)
        filter_key_condition: Optional filter condition (boto3.dynamodb.conditions)
        consistent_read (bool): Whether to use consistent read (default: False)
        index_name (str): Optional GSI name
        query_limit (int): Optional query limit
        scan_index_forward (bool): Sort order (default: True)
        attribute_to_get: Optional attributes to retrieve
        last_evaluated_key (dict): Optional last evaluated key for pagination
        
    Returns:
        List[PolicyManagerDto]: List of DTOs matching the query
        
    Raises:
        ClientError: If query operation fails
        Exception: If any other error occurs
    """
    try:
        results: List[PolicyManagerDto] = []
        
        # Build query parameters
        query_kwargs = {
            'ConsistentRead': consistent_read,
            'ScanIndexForward': scan_index_forward
        }
        
        # Build key condition expression
        if index_name:
            query_kwargs['IndexName'] = index_name
            if index_name == "gsi__gsipk1__gsisk1__index":
                key_condition = Key('gsipk1').eq(hash_key)
            elif index_name == "gsi__gsipk2__gsisk2__index":
                key_condition = Key('gsipk2').eq(hash_key)
            else:
                key_condition = Key('pk').eq(hash_key)
        else:
            key_condition = Key('pk').eq(hash_key)
        
        if range_key_and_condition is not None:
            key_condition = key_condition & range_key_and_condition
        
        query_kwargs['KeyConditionExpression'] = key_condition
        
        if filter_key_condition is not None:
            query_kwargs['FilterExpression'] = filter_key_condition
        
        if query_limit is not None:
            query_kwargs['Limit'] = query_limit
        
        if attribute_to_get is not None:
            query_kwargs['ProjectionExpression'] = ', '.join(attribute_to_get)
        
        if last_evaluated_key is not None:
            query_kwargs['ExclusiveStartKey'] = last_evaluated_key
        
        # Query with pagination
        while True:
            response = table.query(**query_kwargs)
            
            for item in response.get('Items', []):
                results.append(item_to_dto(item))
            
            # Check for more pages
            if 'LastEvaluatedKey' in response and query_limit is None:
                query_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            else:
                break
        
        logger.info("Query returned %d items for hash_key=%s", len(results), hash_key)
        return results
    except ClientError as e:
        logger.error("Error querying for hash_key=%s: %s", hash_key, e)
        raise
    except Exception as e:
        logger.error("Error querying for hash_key=%s: %s", hash_key, e)
        raise


def policy_manager_query_base_table(pk: str, sort_key_condition=None, filter_key_condition=None,
                                    last_evaluated_key: dict = None) -> Dict[str, Any]:
    """
    Wrapper to query base table by pk and optional sort key condition.
    
    Args:
        pk (str): Partition/hash key
        sort_key_condition: Optional sort key condition
        filter_key_condition: Optional filter key condition
        last_evaluated_key (dict): Optional last evaluated key for pagination
        
    Returns:
        Dict[str, Any]: Dictionary with 'items' and 'last_evaluated_key'
    """
    try:
        query_kwargs = {
            'ConsistentRead': False,
            'ScanIndexForward': True,
            'KeyConditionExpression': Key('pk').eq(pk)
        }
        
        if sort_key_condition is not None:
            query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & sort_key_condition
        
        if filter_key_condition is not None:
            query_kwargs['FilterExpression'] = filter_key_condition
        
        if last_evaluated_key is not None:
            query_kwargs['ExclusiveStartKey'] = last_evaluated_key
        
        response = table.query(**query_kwargs)
        
        results = [item_to_dto(item) for item in response.get('Items', [])]
        
        return {
            'items': results,
            'last_evaluated_key': response.get('LastEvaluatedKey')
        }
    except Exception as e:
        logger.error("Error querying base table for pk=%s: %s", pk, e)
        raise


@retry(exceptions=(Exception,), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def policy_manager_batch_write(dtos: List[PolicyManagerDto]) -> None:
    """
    Batch write items; set TTL attribute for each item prior to save.
    
    Args:
        dtos (List[PolicyManagerDto]): List of DTOs to write
        
    Raises:
        Exception: If batch write operation fails
    """
    try:
        error_items = []
        
        with table.batch_writer() as batch:
            for dto in dtos:
                try:
                    item = dto_to_item(dto)
                    item['created_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                    item['updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                    
                    # Set TTL
                    ttl_value = get_ttl(dto.pk)
                    if ttl_value is not None:
                        item['time_to_live'] = ttl_value
                    
                    batch.put_item(Item=item)
                except Exception as e:
                    logger.error("Failed to write item %s %s: %s", dto.pk, dto.sk, e)
                    error_items.append({'dto': dto, 'error': str(e)})
        
        if error_items:
            logger.error("Failed to write %d items", len(error_items))
            raise Exception(f"Batch write failed for {len(error_items)} items: {error_items}")
        
        logger.info("Batch wrote %d items", len(dtos))
    except Exception as e:
        logger.error("Error in batch write, processing items individually: %s", e)
        # Process failed items individually
        for dto in dtos:
            try:
                policy_manager_create_or_update_item(dto)
            except Exception as item_error:
                logger.error("Failed to write item %s %s: %s", dto.pk, dto.sk, item_error)
                error_items.append({'dto': dto, 'error': str(item_error)})
        
        if error_items:
            logger.error("Failed to write %d items", len(error_items))
            raise Exception(f"Batch write failed for {len(error_items)} items: {error_items}")


@retry(exceptions=(Exception,), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def policy_manager_batch_get(keys: List[Dict[str, str]]) -> List[PolicyManagerDto]:
    """
    Batch get items by list of key dictionaries.
    
    Args:
        keys (List[Dict[str, str]]): List of dictionaries containing {'pk': value, 'sk': value}
        
    Returns:
        List[PolicyManagerDto]: List of DTOs retrieved
        
    Raises:
        Exception: If batch get operation fails
    """
    try:
        results: List[PolicyManagerDto] = []
        
        # Batch get in chunks of 100 (DynamoDB limit)
        for i in range(0, len(keys), 100):
            batch_keys = keys[i:i + 100]
            
            response = dynamodb.batch_get_item(
                RequestItems={
                    TABLE_NAME: {
                        'Keys': batch_keys
                    }
                }
            )
            
            for item in response.get('Responses', {}).get(TABLE_NAME, []):
                results.append(item_to_dto(item))
        
        logger.info("Batch got %d items", len(results))
        return results
    except Exception as e:
        logger.error("Error batch getting items: %s", e)
        raise


def gsi__gsipk1__gsisk1__index_query(gsipk1: str, sort_key_condition=None, 
                                     filter_key_condition=None, last_evaluated_key: dict = None) -> Dict[str, Any]:
    """
    Wrapper for GSI query by gsipk1 and optional gsisk1 condition.
    
    Args:
        gsipk1 (str): GSI partition key to query
        sort_key_condition: Optional gsisk1 condition
        filter_key_condition: Optional filter key condition
        last_evaluated_key (dict): Optional last evaluated key for pagination
        
    Returns:
        Dict[str, Any]: Dictionary with 'items' and 'last_evaluated_key'
    """
    try:
        query_kwargs = {
            'IndexName': "gsi__gsipk1__gsisk1__index",
            'ConsistentRead': False,
            'ScanIndexForward': True,
            'KeyConditionExpression': Key('gsipk1').eq(gsipk1)
        }
        
        if sort_key_condition is not None:
            query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & sort_key_condition
        
        if filter_key_condition is not None:
            query_kwargs['FilterExpression'] = filter_key_condition
        
        if last_evaluated_key is not None:
            query_kwargs['ExclusiveStartKey'] = last_evaluated_key
        
        response = table.query(**query_kwargs)
        
        results = [item_to_dto(item) for item in response.get('Items', [])]
        
        return {
            'items': results,
            'last_evaluated_key': response.get('LastEvaluatedKey')
        }
    except Exception as e:
        logger.error("Error querying GSI for gsipk1=%s: %s", gsipk1, e)
        raise


def gsi__gsipk2__gsisk2__index_query(gsipk2: str, sort_key_condition=None, 
                                     filter_key_condition=None, last_evaluated_key: dict = None) -> Dict[str, Any]:
    """
    Wrapper for GSI query by gsipk2 and optional gsisk2 condition.
    
    Args:
        gsipk2 (str): GSI partition key to query
        sort_key_condition: Optional gsisk2 condition
        filter_key_condition: Optional filter key condition
        last_evaluated_key (dict): Optional last evaluated key for pagination
        
    Returns:
        Dict[str, Any]: Dictionary with 'items' and 'last_evaluated_key'
    """
    try:
        query_kwargs = {
            'IndexName': "gsi__gsipk2__gsisk2__index",
            'ConsistentRead': False,
            'ScanIndexForward': True,
            'KeyConditionExpression': Key('gsipk2').eq(gsipk2)
        }
        
        if sort_key_condition is not None:
            query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & sort_key_condition
        
        if filter_key_condition is not None:
            query_kwargs['FilterExpression'] = filter_key_condition
        
        if last_evaluated_key is not None:
            query_kwargs['ExclusiveStartKey'] = last_evaluated_key
        
        response = table.query(**query_kwargs)
        
        results = [item_to_dto(item) for item in response.get('Items', [])]
        
        return {
            'items': results,
            'last_evaluated_key': response.get('LastEvaluatedKey')
        }
    except Exception as e:
        logger.error("Error querying GSI for gsipk2=%s: %s", gsipk2, e)
        raise
