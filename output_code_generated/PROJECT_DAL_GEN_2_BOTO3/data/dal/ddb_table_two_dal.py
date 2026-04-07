import os
import boto3
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
from decimal import Decimal
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
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
    Check if the partition key contains 'TEST' string.

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
    If is_test_object is false, uses get_ttl_days(); otherwise uses get_test_ttl_days().

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
        attr_str_6=item.get('attr_str_6')
    )


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def ddb_table_two_put_item(dto: DdbTableTwoDto) -> None:
    """
    Put an item into the ddb_table_two table. Sets version to 1 and TTL attribute.
    Does NOT set created_at (table doesn't have it).

    Args:
        dto (DdbTableTwoDto): DTO containing item data

    Raises:
        ClientError: If put operation fails
        Exception: If any other error occurs
    """
    try:
        item = dto_to_item(dto)
        item['version'] = 1
        ttl_value = get_ttl(dto.pk_attr_str_1)
        if ttl_value is not None:
            item['time_to_live'] = ttl_value
        table.put_item(Item=item)
        logger.info("Successfully put item %s %s", dto.pk_attr_str_1, dto.sk_attr_str_2)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.error("Conditional check failed for item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        else:
            logger.error("Error putting item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        raise
    except Exception as e:
        logger.error("Error putting item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def ddb_table_two_update_item(dto: DdbTableTwoDto) -> None:
    """
    Update an item in the ddb_table_two table using optimistic locking
    with the version attribute. Does NOT set updated_at.

    Args:
        dto (DdbTableTwoDto): DTO containing updated item data

    Raises:
        ClientError: If update fails (including version conflict)
        Exception: If any other error occurs
    """
    try:
        current = table.get_item(Key={'pk_attr_str_1': dto.pk_attr_str_1, 'sk_attr_str_2': dto.sk_attr_str_2}).get('Item')
        if not current:
            raise Exception(f"Item not found for update: {dto.pk_attr_str_1} {dto.sk_attr_str_2}")
        current_version = current.get('version', 0)

        update_parts = []
        expr_values = {}
        expr_names = {}

        if dto.attr_str_3 is not None:
            update_parts.append("#attr_str_3 = :attr_str_3")
            expr_values[':attr_str_3'] = dto.attr_str_3
            expr_names['#attr_str_3'] = 'attr_str_3'
        if dto.attr_str_4 is not None:
            update_parts.append("#attr_str_4 = :attr_str_4")
            expr_values[':attr_str_4'] = dto.attr_str_4
            expr_names['#attr_str_4'] = 'attr_str_4'
        if dto.attr_str_5 is not None:
            update_parts.append("#attr_str_5 = :attr_str_5")
            expr_values[':attr_str_5'] = dto.attr_str_5
            expr_names['#attr_str_5'] = 'attr_str_5'
        if dto.attr_str_6 is not None:
            update_parts.append("#attr_str_6 = :attr_str_6")
            expr_values[':attr_str_6'] = dto.attr_str_6
            expr_names['#attr_str_6'] = 'attr_str_6'

        # Increment version for optimistic locking
        update_parts.append("#version = :new_version")
        expr_values[':new_version'] = current_version + 1
        expr_values[':current_version'] = current_version
        expr_names['#version'] = 'version'

        update_expression = "SET " + ", ".join(update_parts)

        table.update_item(
            Key={'pk_attr_str_1': dto.pk_attr_str_1, 'sk_attr_str_2': dto.sk_attr_str_2},
            UpdateExpression=update_expression,
            ConditionExpression='#version = :current_version',
            ExpressionAttributeValues=expr_values,
            ExpressionAttributeNames=expr_names
        )
        logger.info("Successfully updated item %s %s", dto.pk_attr_str_1, dto.sk_attr_str_2)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.error("Optimistic lock conflict for item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
            raise
        else:
            logger.error("Error updating item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
            raise
    except Exception as e:
        logger.error("Error updating item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def ddb_table_two_get_item(pk_attr_str_1: str, sk_attr_str_2: str) -> Optional[DdbTableTwoDto]:
    """
    Get an item from the ddb_table_two table.

    Args:
        pk_attr_str_1 (str): Partition/hash key
        sk_attr_str_2 (str): Sort/range key

    Returns:
        Optional[DdbTableTwoDto]: DTO if item found, None otherwise

    Raises:
        ClientError: If get operation fails
        Exception: If any other error occurs
    """
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


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def ddb_table_two_delete_item(pk_attr_str_1: str, sk_attr_str_2: str) -> None:
    """
    Delete an item from the ddb_table_two table using optimistic locking.

    Args:
        pk_attr_str_1 (str): Partition/hash key
        sk_attr_str_2 (str): Sort/range key

    Raises:
        ClientError: If delete operation fails
        Exception: If any other error occurs
    """
    try:
        current = table.get_item(Key={'pk_attr_str_1': pk_attr_str_1, 'sk_attr_str_2': sk_attr_str_2}).get('Item')
        if not current:
            logger.warning("Item not found for delete: %s %s", pk_attr_str_1, sk_attr_str_2)
            return
        current_version = current.get('version', 0)

        table.delete_item(
            Key={'pk_attr_str_1': pk_attr_str_1, 'sk_attr_str_2': sk_attr_str_2},
            ConditionExpression='#version = :current_version',
            ExpressionAttributeNames={'#version': 'version'},
            ExpressionAttributeValues={':current_version': current_version}
        )
        logger.info("Successfully deleted item %s %s", pk_attr_str_1, sk_attr_str_2)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.error("Optimistic lock conflict on delete %s %s: %s", pk_attr_str_1, sk_attr_str_2, e)
            raise
        else:
            logger.error("Error deleting item %s %s: %s", pk_attr_str_1, sk_attr_str_2, e)
            raise
    except Exception as e:
        logger.error("Error deleting item %s %s: %s", pk_attr_str_1, sk_attr_str_2, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def ddb_table_two_create_or_update_item(dto: DdbTableTwoDto) -> None:
    """
    Create or update an item. Gets the item first; if it does not exist,
    creates it. If it exists, updates using the update method which already
    uses optimistic locking. Does NOT set created_at or updated_at.

    Args:
        dto (DdbTableTwoDto): DTO containing item data

    Raises:
        ClientError: If save operation fails
        Exception: If any other error occurs
    """
    try:
        existing_item = ddb_table_two_get_item(dto.pk_attr_str_1, dto.sk_attr_str_2)

        if existing_item is not None:
            logger.info("Updating existing item %s %s", dto.pk_attr_str_1, dto.sk_attr_str_2)
            ddb_table_two_update_item(dto)
        else:
            logger.info("Creating new item %s %s", dto.pk_attr_str_1, dto.sk_attr_str_2)
            item = dto_to_item(dto)
            item['version'] = 1
            ttl_value = get_ttl(dto.pk_attr_str_1)
            if ttl_value is not None:
                item['time_to_live'] = ttl_value
            table.put_item(Item=item)

        logger.info("Successfully created or updated %s %s", dto.pk_attr_str_1, dto.sk_attr_str_2)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.error("Conditional check failed for item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        else:
            logger.error("Error create/update %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        raise
    except Exception as e:
        logger.error("Error create/update %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def ddb_table_two_query(hash_key: str, range_key_and_condition=None, filter_key_condition=None,
                        consistent_read: bool = False, index_name: str = None, query_limit: int = None,
                        scan_index_forward: bool = True, attribute_to_get=None,
                        last_evaluated_key: dict = None) -> List[DdbTableTwoDto]:
    """
    Query the ddb_table_two table using specified parameters until all pages
    are retrieved (when query_limit is None).

    Args:
        hash_key (str): Hash key value to query (mandatory)
        range_key_and_condition: Optional range key condition
        filter_key_condition: Optional filter condition
        consistent_read (bool): Whether to use consistent read (default: False)
        index_name (str): Optional index name (GSI)
        query_limit (int): Optional query limit
        scan_index_forward (bool): Sort order (default: True)
        attribute_to_get: Optional attributes to retrieve
        last_evaluated_key (dict): Optional last evaluated key for pagination

    Returns:
        List[DdbTableTwoDto]: List of DTOs matching the query

    Raises:
        ClientError: If query operation fails
        Exception: If any other error occurs
    """
    try:
        results: List[DdbTableTwoDto] = []
        query_kwargs = {
            'ConsistentRead': consistent_read,
            'ScanIndexForward': scan_index_forward
        }

        key_condition = Key('pk_attr_str_1').eq(hash_key)

        if index_name:
            query_kwargs['IndexName'] = index_name

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
    """
    Wrapper around query for the base table.

    Args:
        pk_attr_str_1 (str): Partition/hash key
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
        logger.error("Error querying base table for pk_attr_str_1=%s: %s", pk_attr_str_1, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type(Exception), reraise=True)
def ddb_table_two_batch_write(dtos: List[DdbTableTwoDto]) -> None:
    """
    Batch write items into the base table. Sets TTL for each item.
    Does NOT set created_at or updated_at. On batch failure, processes
    items individually.

    Args:
        dtos (List[DdbTableTwoDto]): List of DTOs to write

    Raises:
        Exception: If batch write operation fails with error items
    """
    error_items = []
    try:
        with table.batch_writer() as batch:
            for dto in dtos:
                try:
                    item = dto_to_item(dto)
                    ttl_value = get_ttl(dto.pk_attr_str_1)
                    if ttl_value is not None:
                        item['time_to_live'] = ttl_value
                    batch.put_item(Item=item)
                except Exception as e:
                    logger.error("Failed to write item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, e)
                    error_items.append({'dto': dto, 'error': str(e)})

        if error_items:
            logger.error("Failed to write %d items", len(error_items))
            raise Exception(f"Batch write failed for {len(error_items)} items: {error_items}")

        logger.info("Batch wrote %d items", len(dtos))
    except Exception as e:
        logger.error("Error in batch write, processing items individually: %s", e)
        error_items = []
        for dto in dtos:
            try:
                ddb_table_two_create_or_update_item(dto)
            except Exception as item_error:
                logger.error("Failed to write item %s %s: %s", dto.pk_attr_str_1, dto.sk_attr_str_2, item_error)
                error_items.append({'dto': dto, 'error': str(item_error)})

        if error_items:
            logger.error("Failed to write %d items individually", len(error_items))
            raise Exception(f"Batch write failed for {len(error_items)} items: {error_items}")


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type(Exception), reraise=True)
def ddb_table_two_batch_get(keys: List[Dict[str, str]]) -> List[DdbTableTwoDto]:
    """
    Batch get items by list of key dictionaries.

    Args:
        keys (List[Dict[str, str]]): List of dicts with {'pk_attr_str_1': value, 'sk_attr_str_2': value}

    Returns:
        List[DdbTableTwoDto]: List of DTOs retrieved

    Raises:
        Exception: If batch get operation fails
    """
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
