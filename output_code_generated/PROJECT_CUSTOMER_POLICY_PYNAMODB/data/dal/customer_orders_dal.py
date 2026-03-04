from datetime import datetime, timezone
from typing import List, Optional

from retry import retry
from pynamodb.exceptions import PutError, UpdateError, DoesNotExist, DeleteError, QueryError

from aws_lambda_powertools.logging.logger import Logger 
logger = Logger()

from data.model.customer_orders import CustomerOrders
from data.dto.customer_orders_dto import CustomerOrdersDto

RETRY_ATTEMPTS = 3
BACKOFF_DELAY = 2


def dto_to_model(dto: CustomerOrdersDto) -> CustomerOrders:
    """
    Convert DTO to model instance (without saving).
    
    Args:
        dto (CustomerOrdersDto): DTO instance to convert
        
    Returns:
        CustomerOrders: Model instance
    """
    return CustomerOrders(
        pk=dto.pk,
        sk=dto.sk,
        cust_address=dto.cust_address,
        first_name=dto.first_name,
        last_name=dto.last_name,
        phone=dto.phone,
        is_premium_member=dto.is_premium_member,
        order_date=dto.order_date,
        order_quantity=dto.order_quantity,
        sku=dto.sku,
        gsipk1=dto.gsipk1,
        gsisk1=dto.gsisk1
    )


def model_to_dto(item: CustomerOrders) -> CustomerOrdersDto:
    """
    Convert model instance to DTO.
    
    Args:
        item (CustomerOrders): Model instance to convert
        
    Returns:
        CustomerOrdersDto: DTO instance
    """
    return CustomerOrdersDto(
        pk=item.pk,
        sk=item.sk,
        cust_address=item.cust_address,
        first_name=item.first_name,
        last_name=item.last_name,
        phone=item.phone,
        is_premium_member=item.is_premium_member,
        order_date=item.order_date,
        order_quantity=item.order_quantity,
        sku=item.sku,
        gsipk1=item.gsipk1,
        gsisk1=item.gsisk1
    )


@retry(exceptions=(Exception, PutError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def customer_orders_put_item(dto: CustomerOrdersDto) -> None:
    """
    Put an item into the table.
    
    Args:
        dto (CustomerOrdersDto): DTO containing item data
        
    Raises:
        PutError: If put operation fails
        Exception: If any other error occurs
    """
    try:
        item = dto_to_model(dto)
        item.created_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        item.save()
        logger.info("Successfully put item %s %s", dto.pk, dto.sk)
    except PutError as e:
        if hasattr(e, 'cause_response_code') and e.cause_response_code == 'ConditionalCheckFailedException':
            logger.warning("Conditional check failed for item %s %s: %s", dto.pk, dto.sk, e)
        else:
            logger.error("Error putting item %s %s: %s", dto.pk, dto.sk, e)
            raise
    except Exception as e:
        logger.error("Error putting item %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(exceptions=(Exception, UpdateError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def customer_orders_update_item(dto: CustomerOrdersDto) -> None:
    """
    Update an item in the table.
    
    Args:
        dto (CustomerOrdersDto): DTO containing updated item data
        
    Raises:
        UpdateError: If update operation fails
        DoesNotExist: If item does not exist
        Exception: If any other error occurs
    """
    try:
        item = CustomerOrders.get(dto.pk, dto.sk)
        if dto.cust_address is not None:
            item.cust_address = dto.cust_address
        if dto.first_name is not None:
            item.first_name = dto.first_name
        if dto.last_name is not None:
            item.last_name = dto.last_name
        if dto.phone is not None:
            item.phone = dto.phone
        if dto.is_premium_member is not None:
            item.is_premium_member = dto.is_premium_member
        if dto.order_date is not None:
            item.order_date = dto.order_date
        if dto.order_quantity is not None:
            item.order_quantity = dto.order_quantity
        if dto.sku is not None:
            item.sku = dto.sku
        if dto.gsipk1 is not None:
            item.gsipk1 = dto.gsipk1
        if dto.gsisk1 is not None:
            item.gsisk1 = dto.gsisk1
        item.updated_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        item.save()
        logger.info("Successfully updated item %s %s", dto.pk, dto.sk)
    except UpdateError as e:
        logger.error("Error updating item %s %s: %s", dto.pk, dto.sk, e)
        raise
    except Exception as e:
        logger.error("Error updating item %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(exceptions=(Exception, DoesNotExist), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def customer_orders_get_item(pk: str, sk: str) -> Optional[CustomerOrdersDto]:
    """
    Get an item from the table.
    
    Args:
        pk (str): Partition/hash key
        sk (str): Sort/range key
        
    Returns:
        Optional[CustomerOrdersDto]: DTO if item found, None otherwise
        
    Raises:
        Exception: If any error occurs during get operation
    """
    try:
        item = CustomerOrders.get(pk, sk)
        dto = model_to_dto(item)
        logger.info("Successfully got item %s %s", pk, sk)
        return dto
    except DoesNotExist:
        logger.warning("Item not found %s %s", pk, sk)
        return None
    except Exception as e:
        logger.error("Error getting item %s %s: %s", pk, sk, e)
        raise


@retry(exceptions=(Exception, DeleteError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def customer_orders_delete_item(pk: str, sk: str) -> None:
    """
    Delete an item from the table.
    
    Args:
        pk (str): Partition/hash key
        sk (str): Sort/range key
        
    Raises:
        DeleteError: If delete operation fails
        DoesNotExist: If item does not exist
        Exception: If any other error occurs
    """
    try:
        item = CustomerOrders.get(pk, sk)
        item.delete()
        logger.info("Successfully deleted item %s %s", pk, sk)
    except DoesNotExist as e:
        logger.error("Item not found %s %s: %s", pk, sk, e)
        raise
    except DeleteError as e:
        logger.error("Error deleting item %s %s: %s", pk, sk, e)
        raise
    except Exception as e:
        logger.error("Error deleting item %s %s: %s", pk, sk, e)
        raise


@retry(exceptions=(Exception, PutError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def customer_orders_create_or_update_item(dto: CustomerOrdersDto) -> None:
    """
    Create or update an item depending on existence.
    
    Args:
        dto (CustomerOrdersDto): DTO containing item data
        
    Raises:
        PutError: If save operation fails
        Exception: If any other error occurs
    """
    try:
        try:
            item = CustomerOrders.get(dto.pk, dto.sk)
            # update path
            if dto.cust_address is not None:
                item.cust_address = dto.cust_address
            if dto.first_name is not None:
                item.first_name = dto.first_name
            if dto.last_name is not None:
                item.last_name = dto.last_name
            if dto.phone is not None:
                item.phone = dto.phone
            if dto.is_premium_member is not None:
                item.is_premium_member = dto.is_premium_member
            if dto.order_date is not None:
                item.order_date = dto.order_date
            if dto.order_quantity is not None:
                item.order_quantity = dto.order_quantity
            if dto.sku is not None:
                item.sku = dto.sku
            if dto.gsipk1 is not None:
                item.gsipk1 = dto.gsipk1
            if dto.gsisk1 is not None:
                item.gsisk1 = dto.gsisk1
            item.updated_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            logger.info("Updating existing item %s %s", dto.pk, dto.sk)
        except DoesNotExist:
            # create path
            item = dto_to_model(dto)
            item.created_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            logger.info("Creating new item %s %s", dto.pk, dto.sk)
        item.save()
        logger.info("Successfully created or updated %s %s", dto.pk, dto.sk)
    except PutError as e:
        if hasattr(e, 'cause_response_code') and e.cause_response_code == 'ConditionalCheckFailedException':
            logger.warning("Conditional check failed for item %s %s: %s", dto.pk, dto.sk, e)
        else:
            logger.error("Error create/update %s %s: %s", dto.pk, dto.sk, e)
            raise
    except Exception as e:
        logger.error("Error create/update %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(exceptions=(Exception, QueryError), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def customer_orders_query(hash_key: str, range_key_and_condition=None, filter_key_condition=None,
                        consistent_read: bool = False, index_name: str = None, query_limit: int = None,
                        scan_index_forward: bool = True, attribute_to_get=None, last_evaluated_key: dict = None,
                        page_size: int = None, rate_limit: int = None) -> List[CustomerOrdersDto]:
    """
    Query the table using specified parameters until all pages are retrieved.
    
    Args:
        hash_key (str): Hash key value to query
        range_key_and_condition: Optional range key condition
        filter_key_condition: Optional filter condition
        consistent_read (bool): Whether to use consistent read (default: False)
        index_name (str): Optional GSI name
        query_limit (int): Optional query limit
        scan_index_forward (bool): Sort order (default: True)
        attribute_to_get: Optional attributes to retrieve
        last_evaluated_key (dict): Optional last evaluated key for pagination
        page_size (int): Optional page size
        rate_limit (int): Optional rate limit
        
    Returns:
        List[CustomerOrdersDto]: List of DTOs matching the query
        
    Raises:
        QueryError: If query operation fails
        Exception: If any other error occurs
    """
    try:
        results: List[CustomerOrdersDto] = []
        query_kwargs = {
            'hash_key': hash_key,
            'scan_index_forward': scan_index_forward,
            'consistent_read': consistent_read
        }
        if range_key_and_condition is not None:
            query_kwargs['range_key_condition'] = range_key_and_condition
        if filter_key_condition is not None:
            query_kwargs['filter_condition'] = filter_key_condition
        if index_name is not None:
            query_kwargs['index_name'] = index_name
        if query_limit is not None:
            query_kwargs['limit'] = query_limit
        if attribute_to_get is not None:
            query_kwargs['attributes_to_get'] = attribute_to_get
        if last_evaluated_key is not None:
            query_kwargs['last_evaluated_key'] = last_evaluated_key
        if page_size is not None:
            query_kwargs['page_size'] = page_size
        if rate_limit is not None:
            query_kwargs['rate_limit'] = rate_limit

        for item in CustomerOrders.query(**query_kwargs):
            results.append(model_to_dto(item))

        logger.info("Query returned %d items for hash_key=%s", len(results), hash_key)
        return results
    except QueryError as e:
        logger.error("Error querying for hash_key=%s: %s", hash_key, e)
        raise
    except Exception as e:
        logger.error("Error querying for hash_key=%s: %s", hash_key, e)
        raise


def customer_orders_query_base_table(pk: str, sort_key_condition=None) -> List[CustomerOrdersDto]:
    """
    Wrapper to query base table by pk and optional sort key condition.
    
    Args:
        pk (str): Partition/hash key
        sort_key_condition: Optional sort key condition
        
    Returns:
        List[CustomerOrdersDto]: List of DTOs matching the query
    """
    return customer_orders_query(
        hash_key=pk,
        range_key_and_condition=sort_key_condition
    )


@retry(exceptions=(Exception,), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def customer_orders_batch_write(dtos: List[CustomerOrdersDto]) -> None:
    """
    Batch write items; set TTL attribute for each item prior to save.
    
    Args:
        dtos (List[CustomerOrdersDto]): List of DTOs to write
        
    Raises:
        Exception: If batch write operation fails
    """
    try:
        items = []
        error_items = []
        
        for dto in dtos:
            item = dto_to_model(dto)
            item.created_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            ttl_value = item.get_ttl()
            if ttl_value == 0:
                item.time_to_live = None
            else:
                item.time_to_live = ttl_value
            item.updated_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            items.append(item)
        
        with CustomerOrders.batch_write() as batch:
            for item in items:
                batch.save(item)
        
        logger.info("Batch wrote %d items", len(dtos))
    except Exception as e:
        logger.error("Error in batch write, processing items individually: %s", e)
        # Process failed items individually
        for dto in dtos:
            try:
                customer_orders_create_or_update_item(dto)
            except Exception as item_error:
                logger.error("Failed to write item %s %s: %s", dto.pk, dto.sk, item_error)
                error_items.append({'dto': dto, 'error': str(item_error)})
        
        if error_items:
            logger.error("Failed to write %d items", len(error_items))
            raise Exception(f"Batch write failed for {len(error_items)} items: {error_items}")

@retry(exceptions=(Exception,), tries=RETRY_ATTEMPTS, backoff=BACKOFF_DELAY)
def customer_orders_batch_get(keys: List[tuple]) -> List[CustomerOrdersDto]:
    """
    Batch get items by list of (pk, sk) keys.
    
    Args:
        keys (List[tuple]): List of tuples containing (pk, sk)
        
    Returns:
        List[CustomerOrdersDto]: List of DTOs retrieved
        
    Raises:
        Exception: If batch get operation fails
    """
    try:
        results: List[CustomerOrdersDto] = []
        for item in CustomerOrders.batch_get(keys):
            results.append(model_to_dto(item))
        logger.info("Batch got %d items", len(results))
        return results
    except Exception as e:
        logger.error("Error batch getting items: %s", e)
        raise


def gsi__gsipk1__gsisk1__index_query(gsipk1: str, sort_key_condition=None) -> List[CustomerOrdersDto]:
    """
    Wrapper for GSI query by gsipk1 and optional gsisk1 condition.
    
    Args:
        gsipk1 (str): GSI partition key to query
        sort_key_condition: Optional gsisk1 condition
        
    Returns:
        List[CustomerOrdersDto]: List of DTOs matching the query
    """
    return customer_orders_query(
        hash_key=gsipk1,
        range_key_and_condition=sort_key_condition,
        index_name="gsi__gsipk1__gsisk1__index"
    )
