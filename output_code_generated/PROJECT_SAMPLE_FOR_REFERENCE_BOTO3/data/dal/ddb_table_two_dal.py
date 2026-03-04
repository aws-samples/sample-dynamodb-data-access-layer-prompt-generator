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

from data.dto.ddb_table_two_dto import DdbTableTwoDto

# Configuration
TABLE_NAME = os.environ.get('DDB_TABLE_TWO', 'ddb_table_two')
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
    ttl_days = os.environ.get('DDB_TABLE_TWO_TTL_DAYS', '30')
    return int(ttl_days) if ttl_days else 30


def get_test_ttl_days() -> int:
    """
    Get test TTL days from environment variable or default to 30.
    
    Returns:
        int: Number of days for test TTL
    """
    test_ttl_days = os.environ.get('DDB_TABLE_TWO_TEST_TTL_DAYS', '30')
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


def dto_to_item(dto: DdbTableTwoDto) -> Dict[str, Any]:
    """
    Convert DTO to DynamoDB item dictionary.
    
    Args:
        dto (DdbTableTwoDto): DTO instance to convert
        
    Returns:
        Dict[str, Any]: DynamoDB item dictionary
    """
    item = {}
    if dto.pk_attr_str_1 is not None:
        item['pk_attr_str_1'] = dto.pk_attr_str_1
    if dto.sk_attr_str_2 is not None:
        item['sk_attr_str_2'] = dto.sk_attr_str_2
    if dto.attr_str_3 is not None:
        item['attr_str_3'] = dto.attr_str_3
    if dto.attr_str_4 is not None:
        item['attr_str_4'] = dto.attr_str_4
    if dto.attr_str_5 is not None:
        item['attr_str_5'] = dto.attr_str_5
    if dto.attr_str_6 is not None:
        item['attr_str_6'] = dto.attr_str_6
    if dto.gsi1pk_attr_str_7 is not None:
        item['gsi1pk_attr_str_7'] = dto.gsi1pk_attr_str_7
    if dto.gsi1sk_attr_str_8 is not None:
        item['gsi1sk_attr_str_8'] = dto.gsi1sk_attr_str_8
    if dto.gsi2pk_attr_str_9 is not None:
        item['gsi2pk_attr_str_9'] = dto.gsi2pk_attr_str_9
    if dto.gsi2sk_attr_str_10 is not None:
        item['gsi2sk_attr_str_10'] = dto.gsi2sk_attr_str_10
    if dto.gsi3pk_attr_str_11 is not None:
        item['gsi3pk_attr_str_11'] = dto.gsi3pk_attr_str_11
    return item


def item_to_dto(item: Dict[str, Any]) -> DdbTableTwoDto:
    """
    Convert DynamoDB item to DTO.
    
    Args:
        item (Dict[str, Any]): DynamoDB item dictionary
        
    Returns:
        DdbTableTwoDto: DTO instance
    """
    return DdbTableTwoDto(
        pk_attr_str_1=item.get('pk_attr_str_1'),
        sk_attr_str_2=item.get('sk_attr_str_2'),
        attr_str_3=item.get('attr_str_3'),
        attr_str_4=item.get('attr_str_4'),
        attr_str_5=item.get('attr_str_5'),
        attr_str_6=item.get('attr_str_6'),
        gsi1pk_attr_str_7=item.get('gsi1pk_attr_str_7'),
        gsi1sk_attr_str_8=item.get('gsi1sk_attr_str_8'),
        gsi2pk_attr_str_9=item.get('gsi2pk_attr_str_9'),
        gsi2sk_attr_str_10=item.get('gsi2sk_attr_str_10'),
        gsi3pk_attr_str_11=item.get('gsi3pk_attr_str_11')
    )


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_two_put_item(dto: DdbTableTwoDto) -> None:
    """
    Put an item into the table.
    
    Args:
        dto (DdbTableTwoDto): DTO containing item data
        
    Raises:
        ClientError: If put operation fails
        Exception: If any other error occurs
    """
    try:
        item = dto_to_item(dto)
        table.put_item(Item=item)
        logger.info("Successfully put item %s %s", dto.pk_attr_str_1, dto.sk_attr_str_2)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning("Conditional check failed for item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        else:
            logger.error("Error putting item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
            raise
    except Exception as e:
        logger.error("Error putting item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        raise



@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_two_update_item(dto: DdbTableTwoDto) -> None:
    """Update an item in the table."""
    try:
        update_expression_parts = []
        expression_attribute_values = {}
        expression_attribute_names = {}
        
        for attr in ['attr_str_3', 'attr_str_4', 'attr_str_5', 'attr_str_6', 
                     'gsi1pk_attr_str_7', 'gsi1sk_attr_str_8', 'gsi2pk_attr_str_9', 
                     'gsi2sk_attr_str_10', 'gsi3pk_attr_str_11']:
            value = getattr(dto, attr, None)
            if value is not None:
                update_expression_parts.append(f"#{attr} = :{attr}")
                expression_attribute_values[f':{attr}'] = value
                expression_attribute_names[f'#{attr}'] = attr
        
        update_expression_parts.append("#updated_at = :updated_at")
        expression_attribute_values[':updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        expression_attribute_names['#updated_at'] = 'updated_at'
        
        update_expression = "SET " + ", ".join(update_expression_parts)
        
        table.update_item(
            Key={'pk_attr_str_1': dto.pk_attr_str_1, 'sk_attr_str_2': dto.sk_attr_str_2},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names
        )
        logger.info("Successfully updated item %s %s", dto.pk_attr_str_1, dto.sk_attr_str_2)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning("Conditional check failed for item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        else:
            logger.error("Error updating item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
            raise
    except Exception as e:
        logger.error("Error updating item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_two_get_item(pk_attr_str_1: str, sk_attr_str_2: str) -> Optional[DdbTableTwoDto]:
    """Get an item from the table."""
    try:
        response = table.get_item(Key={'pk_attr_str_1': pk_attr_str_1, 'sk_attr_str_2': sk_attr_str_2})
        if 'Item' in response:
            dto = item_to_dto(response['Item'])
            logger.info("Successfully got item %s %s", pk_attr_str_1, sk_attr_str_2)
            return dto
        else:
            logger.warning("Item not found %s %s", pk_attr_str_1, sk_attr_str_2)
            return None
    except ClientError as e:
        logger.error("Error getting item %s %s: %s", pk_attr_str_1, sk_attr_str_2, e)
        raise
    except Exception as e:
        logger.error("Error getting item %s %s: %s", pk_attr_str_1, sk_attr_str_2, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_two_delete_item(pk_attr_str_1: str, sk_attr_str_2: str) -> None:
    """Delete an item from the table."""
    try:
        table.delete_item(Key={'pk_attr_str_1': pk_attr_str_1, 'sk_attr_str_2': sk_attr_str_2})
        logger.info("Successfully deleted item %s %s", pk_attr_str_1, sk_attr_str_2)
    except ClientError as e:
        logger.error("Error deleting item %s %s: %s", pk_attr_str_1, sk_attr_str_2, e)
        raise
    except Exception as e:
        logger.error("Error deleting item %s %s: %s", pk_attr_str_1, sk_attr_str_2, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_two_create_or_update_item(dto: DdbTableTwoDto) -> None:
    """Create or update an item depending on existence."""
    try:
        existing_item = ddb_table_two_get_item(dto.pk_attr_str_1, dto.sk_attr_str_2)
        
        if existing_item is not None:
            logger.info("Updating existing item %s %s", dto.pk_attr_str_1, dto.sk_attr_str_2)
            ddb_table_two_update_item(dto)
        else:
            logger.info("Creating new item %s %s", dto.pk_attr_str_1, dto.sk_attr_str_2)
            item = dto_to_item(dto)
            item['updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            table.put_item(Item=item)
        
        logger.info("Successfully created or updated %s %s", dto.pk_attr_str_1, dto.sk_attr_str_2)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning("Conditional check failed for item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        else:
            logger.error("Error create/update %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
            raise
    except Exception as e:
        logger.error("Error create/update %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_two_query(hash_key: str, range_key_and_condition=None, filter_key_condition=None,
                        consistent_read: bool = False, index_name: str = None, query_limit: int = None,
                        scan_index_forward: bool = True, attribute_to_get=None, last_evaluated_key: dict = None) -> List[DdbTableTwoDto]:
    """Query the table using specified parameters."""
    try:
        results: List[DdbTableTwoDto] = []
        query_kwargs = {
            'ConsistentRead': consistent_read,
            'ScanIndexForward': scan_index_forward
        }
        
        if index_name:
            query_kwargs['IndexName'] = index_name
            if 'gsi1' in index_name:
                key_condition = Key('gsi1pk_attr_str_7').eq(hash_key)
            elif 'gsi2' in index_name:
                key_condition = Key('gsi2pk_attr_str_9').eq(hash_key)
            elif 'gsi3' in index_name:
                key_condition = Key('gsi3pk_attr_str_11').eq(hash_key)
            else:
                key_condition = Key('pk_attr_str_1').eq(hash_key)
        else:
            key_condition = Key('pk_attr_str_1').eq(hash_key)
        
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
        
        while True:
            response = table.query(**query_kwargs)
            for item in response.get('Items', []):
                results.append(item_to_dto(item))
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


def ddb_table_two_query_base_table(pk_attr_str_1: str, sort_key_condition=None, filter_key_condition=None,
                                    last_evaluated_key: dict = None) -> Dict[str, Any]:
    """Wrapper to query base table."""
    try:
        query_kwargs = {
            'ConsistentRead': False,
            'ScanIndexForward': True,
            'KeyConditionExpression': Key('pk_attr_str_1').eq(pk_attr_str_1)
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
        logger.error("Error querying base table for pk=%s: %s", pk_attr_str_1, e)
        raise


@retry(exceptions=(Exception,), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_two_batch_write(dtos: List[DdbTableTwoDto]) -> None:
    """Batch write items."""
    try:
        error_items = []
        with table.batch_writer() as batch:
            for dto in dtos:
                try:
                    item = dto_to_item(dto)
                    item['updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                    ttl_value = get_ttl(dto.pk_attr_str_1)
                    if ttl_value is not None:
                        item['time_to_live'] = ttl_value
                    batch.put_item(Item=item)
                except Exception as e:
                    logger.error("Failed to write item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
                    error_items.append({'dto': dto, 'error': str(e)})
        
        if error_items:
            raise Exception(f"Batch write failed for {len(error_items)} items: {error_items}")
        logger.info("Batch wrote %d items", len(dtos))
    except Exception as e:
        logger.error("Error in batch write: %s", e)
        raise


@retry(exceptions=(Exception,), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_two_batch_get(keys: List[Dict[str, str]]) -> List[DdbTableTwoDto]:
    """Batch get items."""
    try:
        results: List[DdbTableTwoDto] = []
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


def gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index_query(gsi1pk_attr_str_7: str, sort_key_condition=None, 
                                                             filter_key_condition=None, last_evaluated_key: dict = None) -> Dict[str, Any]:
    """Wrapper for GSI1 query."""
    try:
        query_kwargs = {
            'IndexName': "gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index",
            'ConsistentRead': False,
            'ScanIndexForward': True,
            'KeyConditionExpression': Key('gsi1pk_attr_str_7').eq(gsi1pk_attr_str_7)
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
        logger.error("Error querying GSI1: %s", e)
        raise


def gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index_query(gsi2pk_attr_str_9: str, sort_key_condition=None, 
                                                              filter_key_condition=None, last_evaluated_key: dict = None) -> Dict[str, Any]:
    """Wrapper for GSI2 query."""
    try:
        query_kwargs = {
            'IndexName': "gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index",
            'ConsistentRead': False,
            'ScanIndexForward': True,
            'KeyConditionExpression': Key('gsi2pk_attr_str_9').eq(gsi2pk_attr_str_9)
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
        logger.error("Error querying GSI2: %s", e)
        raise


def gsi__gsi3pk_attr_str_11__index_query(gsi3pk_attr_str_11: str, sort_key_condition=None, 
                                          filter_key_condition=None, last_evaluated_key: dict = None) -> Dict[str, Any]:
    """Wrapper for GSI3 query."""
    try:
        query_kwargs = {
            'IndexName': "gsi__gsi3pk_attr_str_11__index",
            'ConsistentRead': False,
            'ScanIndexForward': True,
            'KeyConditionExpression': Key('gsi3pk_attr_str_11').eq(gsi3pk_attr_str_11)
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
        logger.error("Error querying GSI3: %s", e)
        raise
