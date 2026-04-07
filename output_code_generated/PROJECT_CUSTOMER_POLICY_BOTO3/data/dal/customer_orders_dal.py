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

from data.dto.customer_orders_dto import CustomerOrdersDto

# Configuration
TABLE_NAME = os.environ.get('CUSTOMER_ORDERS', 'CustomerOrders')
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
    ttl_days = os.environ.get('CUSTOMER_ORDERS_TTL_DAYS', '30')
    return int(ttl_days) if ttl_days else 30


def get_test_ttl_days() -> int:
    """
    Get test TTL days from environment variable or default to 30.

    Returns:
        int: Number of days for test TTL
    """
    test_ttl_days = os.environ.get('CUSTOMER_ORDERS_TEST_TTL_DAYS', '30')
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


def dto_to_item(dto: CustomerOrdersDto) -> Dict[str, Any]:
    """
    Convert DTO to DynamoDB item dictionary.

    Args:
        dto (CustomerOrdersDto): DTO instance to convert

    Returns:
        Dict[str, Any]: DynamoDB item dictionary
    """
    item = {}
    if dto.pk is not None:
        item['pk'] = dto.pk
    if dto.sk is not None:
        item['sk'] = dto.sk
    if dto.cust_address is not None:
        item['cust_address'] = dto.cust_address
    if dto.first_name is not None:
        item['first_name'] = dto.first_name
    if dto.last_name is not None:
        item['last_name'] = dto.last_name
    if dto.phone is not None:
        item['phone'] = dto.phone
    if dto.is_premium_member is not None:
        item['is_premium_member'] = dto.is_premium_member
    if dto.order_date is not None:
        item['order_date'] = dto.order_date
    if dto.order_quantity is not None:
        item['order_quantity'] = Decimal(str(dto.order_quantity))
    if dto.sku is not None:
        item['sku'] = dto.sku
    if dto.gsipk1 is not None:
        item['gsipk1'] = dto.gsipk1
    if dto.gsisk1 is not None:
        item['gsisk1'] = dto.gsisk1
    return item


def item_to_dto(item: Dict[str, Any]) -> CustomerOrdersDto:
    """
    Convert DynamoDB item to DTO.

    Args:
        item (Dict[str, Any]): DynamoDB item dictionary

    Returns:
        CustomerOrdersDto: DTO instance
    """
    return CustomerOrdersDto(
        pk=item.get('pk'),
        sk=item.get('sk'),
        cust_address=item.get('cust_address'),
        first_name=item.get('first_name'),
        last_name=item.get('last_name'),
        phone=item.get('phone'),
        is_premium_member=item.get('is_premium_member'),
        order_date=item.get('order_date'),
        order_quantity=int(item['order_quantity']) if 'order_quantity' in item else None,
        sku=item.get('sku'),
        gsipk1=item.get('gsipk1'),
        gsisk1=item.get('gsisk1')
    )


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def customer_orders_put_item(dto: CustomerOrdersDto) -> None:
    """
    Put an item into the CustomerOrders table. Sets created_at timestamp,
    version to 1, and TTL attribute.

    Args:
        dto (CustomerOrdersDto): DTO containing item data

    Raises:
        ClientError: If put operation fails
        Exception: If any other error occurs
    """
    try:
        item = dto_to_item(dto)
        item['created_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        item['version'] = 1
        ttl_value = get_ttl(dto.pk)
        if ttl_value is not None:
            item['time_to_live'] = ttl_value
        table.put_item(Item=item)
        logger.info("Successfully put item %s %s", dto.pk, dto.sk)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.error("Conditional check failed for item %s %s: %s", dto.pk, dto.sk, e)
        else:
            logger.error("Error putting item %s %s: %s", dto.pk, dto.sk, e)
        raise
    except Exception as e:
        logger.error("Error putting item %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def customer_orders_update_item(dto: CustomerOrdersDto) -> None:
    """
    Update an item in the CustomerOrders table using optimistic locking
    with the version attribute.

    Args:
        dto (CustomerOrdersDto): DTO containing updated item data

    Raises:
        ClientError: If update fails (including version conflict)
        Exception: If any other error occurs
    """
    try:
        current = table.get_item(Key={'pk': dto.pk, 'sk': dto.sk}).get('Item')
        if not current:
            raise Exception(f"Item not found for update: {dto.pk} {dto.sk}")
        current_version = current.get('version', 0)

        update_parts = []
        expr_values = {}
        expr_names = {}

        if dto.cust_address is not None:
            update_parts.append("#cust_address = :cust_address")
            expr_values[':cust_address'] = dto.cust_address
            expr_names['#cust_address'] = 'cust_address'
        if dto.first_name is not None:
            update_parts.append("#first_name = :first_name")
            expr_values[':first_name'] = dto.first_name
            expr_names['#first_name'] = 'first_name'
        if dto.last_name is not None:
            update_parts.append("#last_name = :last_name")
            expr_values[':last_name'] = dto.last_name
            expr_names['#last_name'] = 'last_name'
        if dto.phone is not None:
            update_parts.append("#phone = :phone")
            expr_values[':phone'] = dto.phone
            expr_names['#phone'] = 'phone'
        if dto.is_premium_member is not None:
            update_parts.append("#is_premium_member = :is_premium_member")
            expr_values[':is_premium_member'] = dto.is_premium_member
            expr_names['#is_premium_member'] = 'is_premium_member'
        if dto.order_date is not None:
            update_parts.append("#order_date = :order_date")
            expr_values[':order_date'] = dto.order_date
            expr_names['#order_date'] = 'order_date'
        if dto.order_quantity is not None:
            update_parts.append("#order_quantity = :order_quantity")
            expr_values[':order_quantity'] = Decimal(str(dto.order_quantity))
            expr_names['#order_quantity'] = 'order_quantity'
        if dto.sku is not None:
            update_parts.append("#sku = :sku")
            expr_values[':sku'] = dto.sku
            expr_names['#sku'] = 'sku'
        if dto.gsipk1 is not None:
            update_parts.append("#gsipk1 = :gsipk1")
            expr_values[':gsipk1'] = dto.gsipk1
            expr_names['#gsipk1'] = 'gsipk1'
        if dto.gsisk1 is not None:
            update_parts.append("#gsisk1 = :gsisk1")
            expr_values[':gsisk1'] = dto.gsisk1
            expr_names['#gsisk1'] = 'gsisk1'

        # Add updated_at
        update_parts.append("#updated_at = :updated_at")
        expr_values[':updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        expr_names['#updated_at'] = 'updated_at'

        # Increment version for optimistic locking
        update_parts.append("#version = :new_version")
        expr_values[':new_version'] = current_version + 1
        expr_values[':current_version'] = current_version
        expr_names['#version'] = 'version'

        update_expression = "SET " + ", ".join(update_parts)

        table.update_item(
            Key={'pk': dto.pk, 'sk': dto.sk},
            UpdateExpression=update_expression,
            ConditionExpression='#version = :current_version',
            ExpressionAttributeValues=expr_values,
            ExpressionAttributeNames=expr_names
        )
        logger.info("Successfully updated item %s %s", dto.pk, dto.sk)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.error("Optimistic lock conflict for item %s %s: %s", dto.pk, dto.sk, e)
            raise
        else:
            logger.error("Error updating item %s %s: %s", dto.pk, dto.sk, e)
            raise
    except Exception as e:
        logger.error("Error updating item %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def customer_orders_get_item(pk: str, sk: str) -> Optional[CustomerOrdersDto]:
    """
    Get an item from the CustomerOrders table.

    Args:
        pk (str): Partition/hash key
        sk (str): Sort/range key

    Returns:
        Optional[CustomerOrdersDto]: DTO if item found, None otherwise

    Raises:
        ClientError: If get operation fails
        Exception: If any other error occurs
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


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def customer_orders_delete_item(pk: str, sk: str) -> None:
    """
    Delete an item from the CustomerOrders table using optimistic locking.

    Args:
        pk (str): Partition/hash key
        sk (str): Sort/range key

    Raises:
        ClientError: If delete operation fails
        Exception: If any other error occurs
    """
    try:
        current = table.get_item(Key={'pk': pk, 'sk': sk}).get('Item')
        if not current:
            logger.warning("Item not found for delete: %s %s", pk, sk)
            return
        current_version = current.get('version', 0)

        table.delete_item(
            Key={'pk': pk, 'sk': sk},
            ConditionExpression='#version = :current_version',
            ExpressionAttributeNames={'#version': 'version'},
            ExpressionAttributeValues={':current_version': current_version}
        )
        logger.info("Successfully deleted item %s %s", pk, sk)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.error("Optimistic lock conflict on delete %s %s: %s", pk, sk, e)
            raise
        else:
            logger.error("Error deleting item %s %s: %s", pk, sk, e)
            raise
    except Exception as e:
        logger.error("Error deleting item %s %s: %s", pk, sk, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def customer_orders_create_or_update_item(dto: CustomerOrdersDto) -> None:
    """
    Create or update an item. Gets the item first; if it does not exist,
    creates it. If it exists, updates using the update method which already
    uses optimistic locking.

    Args:
        dto (CustomerOrdersDto): DTO containing item data

    Raises:
        ClientError: If save operation fails
        Exception: If any other error occurs
    """
    try:
        existing_item = customer_orders_get_item(dto.pk, dto.sk)

        if existing_item is not None:
            logger.info("Updating existing item %s %s", dto.pk, dto.sk)
            customer_orders_update_item(dto)
        else:
            logger.info("Creating new item %s %s", dto.pk, dto.sk)
            item = dto_to_item(dto)
            item['created_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            item['updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            item['version'] = 1
            ttl_value = get_ttl(dto.pk)
            if ttl_value is not None:
                item['time_to_live'] = ttl_value
            table.put_item(Item=item)

        logger.info("Successfully created or updated %s %s", dto.pk, dto.sk)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.error("Conditional check failed for item %s %s: %s", dto.pk, dto.sk, e)
        else:
            logger.error("Error create/update %s %s: %s", dto.pk, dto.sk, e)
        raise
    except Exception as e:
        logger.error("Error create/update %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type((Exception, ClientError)), reraise=True)
def customer_orders_query(hash_key: str, range_key_and_condition=None, filter_key_condition=None,
                          consistent_read: bool = False, index_name: str = None, query_limit: int = None,
                          scan_index_forward: bool = True, attribute_to_get=None,
                          last_evaluated_key: dict = None) -> List[CustomerOrdersDto]:
    """
    Query the CustomerOrders table using specified parameters until all pages
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
        List[CustomerOrdersDto]: List of DTOs matching the query

    Raises:
        ClientError: If query operation fails
        Exception: If any other error occurs
    """
    try:
        results: List[CustomerOrdersDto] = []
        query_kwargs = {
            'ConsistentRead': consistent_read,
            'ScanIndexForward': scan_index_forward
        }

        if index_name:
            query_kwargs['IndexName'] = index_name
            if index_name == "gsi__gsipk1__gsisk1__index":
                key_condition = Key('gsipk1').eq(hash_key)
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


def customer_orders_query_base_table(pk: str, sort_key_condition=None, filter_key_condition=None,
                                     last_evaluated_key: dict = None) -> Dict[str, Any]:
    """
    Wrapper around query for the base table.

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


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type(Exception), reraise=True)
def customer_orders_batch_write(dtos: List[CustomerOrdersDto]) -> None:
    """
    Batch write items into the base table. Sets created_at, updated_at,
    and TTL for each item. On batch failure, processes items individually.

    Args:
        dtos (List[CustomerOrdersDto]): List of DTOs to write

    Raises:
        Exception: If batch write operation fails with error items
    """
    error_items = []
    try:
        with table.batch_writer() as batch:
            for dto in dtos:
                try:
                    item = dto_to_item(dto)
                    item['created_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                    item['updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
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
        error_items = []
        for dto in dtos:
            try:
                customer_orders_create_or_update_item(dto)
            except Exception as item_error:
                logger.error("Failed to write item %s %s: %s", dto.pk, dto.sk, item_error)
                error_items.append({'dto': dto, 'error': str(item_error)})

        if error_items:
            logger.error("Failed to write %d items individually", len(error_items))
            raise Exception(f"Batch write failed for {len(error_items)} items: {error_items}")


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY),
       retry=retry_if_exception_type(Exception), reraise=True)
def customer_orders_batch_get(keys: List[Dict[str, str]]) -> List[CustomerOrdersDto]:
    """
    Batch get items by list of key dictionaries.

    Args:
        keys (List[Dict[str, str]]): List of dicts with {'pk': value, 'sk': value}

    Returns:
        List[CustomerOrdersDto]: List of DTOs retrieved

    Raises:
        Exception: If batch get operation fails
    """
    try:
        results: List[CustomerOrdersDto] = []
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
                                     filter_key_condition=None,
                                     last_evaluated_key: dict = None) -> Dict[str, Any]:
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