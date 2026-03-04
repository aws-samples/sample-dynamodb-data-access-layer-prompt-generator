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

from data.dto.ddb_table_one_dto import DdbTableOneDto

# Configuration
TABLE_NAME = os.environ.get('DDB_TABLE_ONE', 'ddb_table_one')
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
    ttl_days = os.environ.get('DDB_TABLE_ONE_TTL_DAYS', '30')
    return int(ttl_days) if ttl_days else 30


def get_test_ttl_days() -> int:
    """
    Get test TTL days from environment variable or default to 30.
    
    Returns:
        int: Number of days for test TTL
    """
    test_ttl_days = os.environ.get('DDB_TABLE_ONE_TEST_TTL_DAYS', '30')
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


def dto_to_item(dto: DdbTableOneDto) -> Dict[str, Any]:
    """
    Convert DTO to DynamoDB item dictionary.
    
    Args:
        dto (DdbTableOneDto): DTO instance to convert
        
    Returns:
        Dict[str, Any]: DynamoDB item dictionary
    """
    item = {}
    if dto.pk_attribute_str_1 is not None:
        item['pk_attribute_str_1'] = dto.pk_attribute_str_1
    if dto.sk_attribute_str_2 is not None:
        item['sk_attribute_str_2'] = dto.sk_attribute_str_2
    if dto.attribute_str_3 is not None:
        item['attribute_str_3'] = dto.attribute_str_3
    if dto.attribute_str_4 is not None:
        item['attribute_str_4'] = dto.attribute_str_4
    if dto.attribute_num_5 is not None:
        item['attribute_num_5'] = Decimal(str(dto.attribute_num_5))
    if dto.attribute_map_6 is not None:
        item['attribute_map_6'] = dto.attribute_map_6
    if dto.gsipk_attribute_str_7 is not None:
        item['gsipk_attribute_str_7'] = dto.gsipk_attribute_str_7
    if dto.gsisk_attribute_str_8 is not None:
        item['gsisk_attribute_str_8'] = dto.gsisk_attribute_str_8
    return item


def item_to_dto(item: Dict[str, Any]) -> DdbTableOneDto:
    """
    Convert DynamoDB item to DTO.
    
    Args:
        item (Dict[str, Any]): DynamoDB item dictionary
        
    Returns:
        DdbTableOneDto: DTO instance
    """
    return DdbTableOneDto(
        pk_attribute_str_1=item.get('pk_attribute_str_1'),
        sk_attribute_str_2=item.get('sk_attribute_str_2'),
        attribute_str_3=item.get('attribute_str_3'),
        attribute_str_4=item.get('attribute_str_4'),
        attribute_num_5=int(item['attribute_num_5']) if 'attribute_num_5' in item else None,
        attribute_map_6=item.get('attribute_map_6'),
        gsipk_attribute_str_7=item.get('gsipk_attribute_str_7'),
        gsisk_attribute_str_8=item.get('gsisk_attribute_str_8')
    )


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_one_put_item(dto: DdbTableOneDto) -> None:
    """
    Put an item into the table.
    
    Args:
        dto (DdbTableOneDto): DTO containing item data
        
    Raises:
        ClientError: If put operation fails
        Exception: If any other error occurs
    """
    try:
        item = dto_to_item(dto)
        item['created_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        table.put_item(Item=item)
        logger.info("Successfully put item %s %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning("Conditional check failed for item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        else:
            logger.error("Error putting item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
            raise
    except Exception as e:
        logger.error("Error putting item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        raise



@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_one_update_item(dto: DdbTableOneDto) -> None:
    """
    Update an item in the table.
    
    Args:
        dto (DdbTableOneDto): DTO containing updated item data
        
    Raises:
        ClientError: If update operation fails
        Exception: If any other error occurs
    """
    try:
        update_expression_parts = []
        expression_attribute_values = {}
        expression_attribute_names = {}
        
        if dto.attribute_str_3 is not None:
            update_expression_parts.append("#attribute_str_3 = :attribute_str_3")
            expression_attribute_values[':attribute_str_3'] = dto.attribute_str_3
            expression_attribute_names['#attribute_str_3'] = 'attribute_str_3'
        if dto.attribute_str_4 is not None:
            update_expression_parts.append("#attribute_str_4 = :attribute_str_4")
            expression_attribute_values[':attribute_str_4'] = dto.attribute_str_4
            expression_attribute_names['#attribute_str_4'] = 'attribute_str_4'
        if dto.attribute_num_5 is not None:
            update_expression_parts.append("#attribute_num_5 = :attribute_num_5")
            expression_attribute_values[':attribute_num_5'] = Decimal(str(dto.attribute_num_5))
            expression_attribute_names['#attribute_num_5'] = 'attribute_num_5'
        if dto.attribute_map_6 is not None:
            update_expression_parts.append("#attribute_map_6 = :attribute_map_6")
            expression_attribute_values[':attribute_map_6'] = dto.attribute_map_6
            expression_attribute_names['#attribute_map_6'] = 'attribute_map_6'
        if dto.gsipk_attribute_str_7 is not None:
            update_expression_parts.append("#gsipk_attribute_str_7 = :gsipk_attribute_str_7")
            expression_attribute_values[':gsipk_attribute_str_7'] = dto.gsipk_attribute_str_7
            expression_attribute_names['#gsipk_attribute_str_7'] = 'gsipk_attribute_str_7'
        if dto.gsisk_attribute_str_8 is not None:
            update_expression_parts.append("#gsisk_attribute_str_8 = :gsisk_attribute_str_8")
            expression_attribute_values[':gsisk_attribute_str_8'] = dto.gsisk_attribute_str_8
            expression_attribute_names['#gsisk_attribute_str_8'] = 'gsisk_attribute_str_8'
        
        # Add updated_at
        update_expression_parts.append("#updated_at = :updated_at")
        expression_attribute_values[':updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        expression_attribute_names['#updated_at'] = 'updated_at'
        
        update_expression = "SET " + ", ".join(update_expression_parts)
        
        table.update_item(
            Key={'pk_attribute_str_1': dto.pk_attribute_str_1, 'sk_attribute_str_2': dto.sk_attribute_str_2},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names
        )
        logger.info("Successfully updated item %s %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning("Conditional check failed for item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        else:
            logger.error("Error updating item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
            raise
    except Exception as e:
        logger.error("Error updating item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_one_get_item(pk_attribute_str_1: str, sk_attribute_str_2: str) -> Optional[DdbTableOneDto]:
    """
    Get an item from the table.
    
    Args:
        pk_attribute_str_1 (str): Partition/hash key
        sk_attribute_str_2 (str): Sort/range key
        
    Returns:
        Optional[DdbTableOneDto]: DTO if item found, None otherwise
        
    Raises:
        Exception: If any error occurs during get operation
    """
    try:
        response = table.get_item(Key={'pk_attribute_str_1': pk_attribute_str_1, 'sk_attribute_str_2': sk_attribute_str_2})
        if 'Item' in response:
            dto = item_to_dto(response['Item'])
            logger.info("Successfully got item %s %s", pk_attribute_str_1, sk_attribute_str_2)
            return dto
        else:
            logger.warning("Item not found %s %s", pk_attribute_str_1, sk_attribute_str_2)
            return None
    except ClientError as e:
        logger.error("Error getting item %s %s: %s", pk_attribute_str_1, sk_attribute_str_2, e)
        raise
    except Exception as e:
        logger.error("Error getting item %s %s: %s", pk_attribute_str_1, sk_attribute_str_2, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_one_delete_item(pk_attribute_str_1: str, sk_attribute_str_2: str) -> None:
    """
    Delete an item from the table.
    
    Args:
        pk_attribute_str_1 (str): Partition/hash key
        sk_attribute_str_2 (str): Sort/range key
        
    Raises:
        ClientError: If delete operation fails
        Exception: If any other error occurs
    """
    try:
        table.delete_item(Key={'pk_attribute_str_1': pk_attribute_str_1, 'sk_attribute_str_2': sk_attribute_str_2})
        logger.info("Successfully deleted item %s %s", pk_attribute_str_1, sk_attribute_str_2)
    except ClientError as e:
        logger.error("Error deleting item %s %s: %s", pk_attribute_str_1, sk_attribute_str_2, e)
        raise
    except Exception as e:
        logger.error("Error deleting item %s %s: %s", pk_attribute_str_1, sk_attribute_str_2, e)
        raise


@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_one_create_or_update_item(dto: DdbTableOneDto) -> None:
    """
    Create or update an item depending on existence.
    
    Args:
        dto (DdbTableOneDto): DTO containing item data
        
    Raises:
        ClientError: If save operation fails
        Exception: If any other error occurs
    """
    try:
        existing_item = ddb_table_one_get_item(dto.pk_attribute_str_1, dto.sk_attribute_str_2)
        
        if existing_item is not None:
            # Update path
            logger.info("Updating existing item %s %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2)
            ddb_table_one_update_item(dto)
        else:
            # Create path
            logger.info("Creating new item %s %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2)
            item = dto_to_item(dto)
            item['created_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            item['updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            table.put_item(Item=item)
        
        logger.info("Successfully created or updated %s %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning("Conditional check failed for item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        else:
            logger.error("Error create/update %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
            raise
    except Exception as e:
        logger.error("Error create/update %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        raise



@retry(exceptions=(Exception, ClientError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_one_query(hash_key: str, range_key_and_condition=None, filter_key_condition=None,
                        consistent_read: bool = False, index_name: str = None, query_limit: int = None,
                        scan_index_forward: bool = True, attribute_to_get=None, last_evaluated_key: dict = None) -> List[DdbTableOneDto]:
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
        List[DdbTableOneDto]: List of DTOs matching the query
        
    Raises:
        ClientError: If query operation fails
        Exception: If any other error occurs
    """
    try:
        results: List[DdbTableOneDto] = []
        
        # Build query parameters
        query_kwargs = {
            'ConsistentRead': consistent_read,
            'ScanIndexForward': scan_index_forward
        }
        
        # Build key condition expression
        if index_name:
            query_kwargs['IndexName'] = index_name
            if index_name == "gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index":
                key_condition = Key('gsipk_attribute_str_7').eq(hash_key)
            else:
                key_condition = Key('pk_attribute_str_1').eq(hash_key)
        else:
            key_condition = Key('pk_attribute_str_1').eq(hash_key)
        
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


def ddb_table_one_query_base_table(pk_attribute_str_1: str, sort_key_condition=None, filter_key_condition=None,
                                    last_evaluated_key: dict = None) -> Dict[str, Any]:
    """
    Wrapper to query base table by pk_attribute_str_1 and optional sort key condition.
    
    Args:
        pk_attribute_str_1 (str): Partition/hash key
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
            'KeyConditionExpression': Key('pk_attribute_str_1').eq(pk_attribute_str_1)
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
        logger.error("Error querying base table for pk=%s: %s", pk_attribute_str_1, e)
        raise


@retry(exceptions=(Exception,), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_one_batch_write(dtos: List[DdbTableOneDto]) -> None:
    """
    Batch write items; set TTL attribute for each item prior to save.
    
    Args:
        dtos (List[DdbTableOneDto]): List of DTOs to write
        
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
                    ttl_value = get_ttl(dto.pk_attribute_str_1)
                    if ttl_value is not None:
                        item['time_to_live'] = ttl_value
                    
                    batch.put_item(Item=item)
                except Exception as e:
                    logger.error("Failed to write item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
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
                ddb_table_one_create_or_update_item(dto)
            except Exception as item_error:
                logger.error("Failed to write item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, item_error)
                error_items.append({'dto': dto, 'error': str(item_error)})
        
        if error_items:
            logger.error("Failed to write %d items", len(error_items))
            raise Exception(f"Batch write failed for {len(error_items)} items: {error_items}")


@retry(exceptions=(Exception,), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def ddb_table_one_batch_get(keys: List[Dict[str, str]]) -> List[DdbTableOneDto]:
    """
    Batch get items by list of key dictionaries.
    
    Args:
        keys (List[Dict[str, str]]): List of dictionaries containing {'pk_attribute_str_1': value, 'sk_attribute_str_2': value}
        
    Returns:
        List[DdbTableOneDto]: List of DTOs retrieved
        
    Raises:
        Exception: If batch get operation fails
    """
    try:
        results: List[DdbTableOneDto] = []
        
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


def gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query(gsipk_attribute_str_7: str, sort_key_condition=None, 
                                                                     filter_key_condition=None, last_evaluated_key: dict = None) -> Dict[str, Any]:
    """
    Wrapper for GSI query by gsipk_attribute_str_7 and optional gsisk_attribute_str_8 condition.
    
    Args:
        gsipk_attribute_str_7 (str): GSI partition key to query
        sort_key_condition: Optional gsisk_attribute_str_8 condition
        filter_key_condition: Optional filter key condition
        last_evaluated_key (dict): Optional last evaluated key for pagination
        
    Returns:
        Dict[str, Any]: Dictionary with 'items' and 'last_evaluated_key'
    """
    try:
        query_kwargs = {
            'IndexName': "gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index",
            'ConsistentRead': False,
            'ScanIndexForward': True,
            'KeyConditionExpression': Key('gsipk_attribute_str_7').eq(gsipk_attribute_str_7)
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
        logger.error("Error querying GSI for gsipk_attribute_str_7=%s: %s", gsipk_attribute_str_7, e)
        raise
