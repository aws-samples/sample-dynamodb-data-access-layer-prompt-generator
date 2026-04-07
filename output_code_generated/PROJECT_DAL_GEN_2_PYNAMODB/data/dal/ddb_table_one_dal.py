from datetime import datetime, timezone
from typing import List, Optional

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pynamodb.exceptions import PutError, UpdateError, DoesNotExist, DeleteError, QueryError

from aws_lambda_powertools.logging.logger import Logger
logger = Logger()

from data.model.ddb_table_one import DdbTableOne
from data.dto.ddb_table_one_dto import DdbTableOneDto

RETRY_ATTEMPTS = 3
BACKOFF_DELAY = 2


def dto_to_model(dto: DdbTableOneDto) -> DdbTableOne:
    """
    Convert DTO to model instance (without saving).
    
    Args:
        dto (DdbTableOneDto): DTO instance to convert
        
    Returns:
        DdbTableOne: Model instance
    """
    return DdbTableOne(
        pk_attribute_str_1=dto.pk_attribute_str_1,
        sk_attribute_str_2=dto.sk_attribute_str_2,
        attribute_str_3=dto.attribute_str_3,
        attribute_str_4=dto.attribute_str_4,
        attribute_num_5=dto.attribute_num_5,
        attribute_map_6=dto.attribute_map_6
    )


def model_to_dto(item: DdbTableOne) -> DdbTableOneDto:
    """
    Convert model instance to DTO.
    
    Args:
        item (DdbTableOne): Model instance to convert
        
    Returns:
        DdbTableOneDto: DTO instance
    """
    return DdbTableOneDto(
        pk_attribute_str_1=item.pk_attribute_str_1,
        sk_attribute_str_2=item.sk_attribute_str_2,
        attribute_str_3=item.attribute_str_3,
        attribute_str_4=item.attribute_str_4,
        attribute_num_5=item.attribute_num_5,
        attribute_map_6=item.attribute_map_6
    )


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception, PutError)), reraise=True)
def ddb_table_one_put_item(dto: DdbTableOneDto) -> None:
    """
    Put an item into the table.
    
    Args:
        dto (DdbTableOneDto): DTO containing item data
        
    Raises:
        PutError: If put operation fails (including ConditionalCheckFailedException)
        Exception: If any other error occurs
    """
    try:
        item = dto_to_model(dto)
        item.created_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        item.save()
        logger.info("Successfully put item %s %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2)
    except PutError as e:
        if hasattr(e, 'cause_response_code') and e.cause_response_code == 'ConditionalCheckFailedException':
            logger.error("Conditional check failed for item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        else:
            logger.error("Error putting item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        raise
    except Exception as e:
        logger.error("Error putting item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception, PutError)), reraise=True)
def ddb_table_one_update_item(dto: DdbTableOneDto) -> None:
    """
    Update an item in the table.
    
    Args:
        dto (DdbTableOneDto): DTO containing updated item data
        
    Raises:
        PutError: If save operation fails (including ConditionalCheckFailedException)
        DoesNotExist: If item does not exist
        Exception: If any other error occurs
    """
    try:
        item = DdbTableOne.get(dto.pk_attribute_str_1, dto.sk_attribute_str_2)
        if dto.attribute_str_3 is not None:
            item.attribute_str_3 = dto.attribute_str_3
        if dto.attribute_str_4 is not None:
            item.attribute_str_4 = dto.attribute_str_4
        if dto.attribute_num_5 is not None:
            item.attribute_num_5 = dto.attribute_num_5
        if dto.attribute_map_6 is not None:
            item.attribute_map_6 = dto.attribute_map_6
        item.updated_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        item.save()
        logger.info("Successfully updated item %s %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2)
    except DoesNotExist as e:
        logger.error("Item not found for update %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        raise
    except PutError as e:
        if hasattr(e, 'cause_response_code') and e.cause_response_code == 'ConditionalCheckFailedException':
            logger.error("Conditional check failed for update %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        else:
            logger.error("Error updating item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        raise
    except Exception as e:
        logger.error("Error updating item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception,)), reraise=True)
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
        item = DdbTableOne.get(pk_attribute_str_1, sk_attribute_str_2)
        dto = model_to_dto(item)
        logger.info("Successfully got item %s %s", pk_attribute_str_1, sk_attribute_str_2)
        return dto
    except DoesNotExist:
        logger.warning("Item not found %s %s", pk_attribute_str_1, sk_attribute_str_2)
        return None
    except Exception as e:
        logger.error("Error getting item %s %s: %s", pk_attribute_str_1, sk_attribute_str_2, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception, DeleteError)), reraise=True)
def ddb_table_one_delete_item(pk_attribute_str_1: str, sk_attribute_str_2: str) -> None:
    """
    Delete an item from the table.
    
    Args:
        pk_attribute_str_1 (str): Partition/hash key
        sk_attribute_str_2 (str): Sort/range key
        
    Raises:
        DeleteError: If delete operation fails
        DoesNotExist: If item does not exist
        Exception: If any other error occurs
    """
    try:
        item = DdbTableOne.get(pk_attribute_str_1, sk_attribute_str_2)
        item.delete()
        logger.info("Successfully deleted item %s %s", pk_attribute_str_1, sk_attribute_str_2)
    except DoesNotExist as e:
        logger.error("Item not found %s %s: %s", pk_attribute_str_1, sk_attribute_str_2, e)
        raise
    except DeleteError as e:
        logger.error("Error deleting item %s %s: %s", pk_attribute_str_1, sk_attribute_str_2, e)
        raise
    except Exception as e:
        logger.error("Error deleting item %s %s: %s", pk_attribute_str_1, sk_attribute_str_2, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception, PutError)), reraise=True)
def ddb_table_one_create_or_update_item(dto: DdbTableOneDto) -> None:
    """
    Create or update an item depending on existence.
    
    Args:
        dto (DdbTableOneDto): DTO containing item data
        
    Raises:
        PutError: If save operation fails (including ConditionalCheckFailedException)
        Exception: If any other error occurs
    """
    try:
        try:
            item = DdbTableOne.get(dto.pk_attribute_str_1, dto.sk_attribute_str_2)
            # update path
            if dto.attribute_str_3 is not None:
                item.attribute_str_3 = dto.attribute_str_3
            if dto.attribute_str_4 is not None:
                item.attribute_str_4 = dto.attribute_str_4
            if dto.attribute_num_5 is not None:
                item.attribute_num_5 = dto.attribute_num_5
            if dto.attribute_map_6 is not None:
                item.attribute_map_6 = dto.attribute_map_6
            item.updated_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            logger.info("Updating existing item %s %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2)
        except DoesNotExist:
            # create path
            item = dto_to_model(dto)
            item.created_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            logger.info("Creating new item %s %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2)
        item.save()
        logger.info("Successfully created or updated %s %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2)
    except PutError as e:
        if hasattr(e, 'cause_response_code') and e.cause_response_code == 'ConditionalCheckFailedException':
            logger.error("Conditional check failed for item %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        else:
            logger.error("Error create/update %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        raise
    except Exception as e:
        logger.error("Error create/update %s %s: %s", dto.pk_attribute_str_1, dto.sk_attribute_str_2, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception, QueryError)), reraise=True)
def ddb_table_one_query(hash_key: str, range_key_and_condition=None, filter_key_condition=None,
                        consistent_read: bool = False, index_name: str = None, query_limit: int = None,
                        scan_index_forward: bool = True, attribute_to_get=None, last_evaluated_key: dict = None,
                        page_size: int = None, rate_limit: int = None) -> List[DdbTableOneDto]:
    """
    Query the table using specified parameters until all pages are retrieved.
    
    Args:
        hash_key (str): Hash key value to query
        range_key_and_condition: Optional range key condition
        filter_key_condition: Optional filter condition
        consistent_read (bool): Whether to use consistent read (default: False)
        index_name (str): Optional index name (GSI)
        query_limit (int): Optional query limit
        scan_index_forward (bool): Sort order (default: True)
        attribute_to_get: Optional attributes to retrieve
        last_evaluated_key (dict): Optional last evaluated key for pagination
        page_size (int): Optional page size
        rate_limit (int): Optional rate limit
        
    Returns:
        List[DdbTableOneDto]: List of DTOs matching the query
        
    Raises:
        QueryError: If query operation fails
        Exception: If any other error occurs
    """
    try:
        results: List[DdbTableOneDto] = []
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

        for item in DdbTableOne.query(**query_kwargs):
            results.append(model_to_dto(item))

        logger.info("Query returned %d items for hash_key=%s", len(results), hash_key)
        return results
    except QueryError as e:
        logger.error("Error querying for hash_key=%s: %s", hash_key, e)
        raise
    except Exception as e:
        logger.error("Error querying for hash_key=%s: %s", hash_key, e)
        raise


def ddb_table_one_query_base_table(pk_attribute_str_1: str, sort_key_condition=None) -> List[DdbTableOneDto]:
    """
    Wrapper to query base table by pk_attribute_str_1 and optional sort key condition.
    
    Args:
        pk_attribute_str_1 (str): Partition/hash key
        sort_key_condition: Optional sort key condition
        
    Returns:
        List[DdbTableOneDto]: List of DTOs matching the query
    """
    return ddb_table_one_query(
        hash_key=pk_attribute_str_1,
        range_key_and_condition=sort_key_condition
    )


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception,)), reraise=True)
def ddb_table_one_batch_write(dtos: List[DdbTableOneDto]) -> None:
    """
    Batch write items; set TTL attribute for each item prior to save.
    Falls back to individual writes on failure.
    
    Args:
        dtos (List[DdbTableOneDto]): List of DTOs to write
        
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
        
        with DdbTableOne.batch_write() as batch:
            for item in items:
                batch.save(item)
        
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


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception,)), reraise=True)
def ddb_table_one_batch_get(keys: List[tuple]) -> List[DdbTableOneDto]:
    """
    Batch get items by list of (pk_attribute_str_1, sk_attribute_str_2) keys.
    
    Args:
        keys (List[tuple]): List of tuples containing (pk_attribute_str_1, sk_attribute_str_2)
        
    Returns:
        List[DdbTableOneDto]: List of DTOs retrieved
        
    Raises:
        Exception: If batch get operation fails
    """
    try:
        results: List[DdbTableOneDto] = []
        for item in DdbTableOne.batch_get(keys):
            results.append(model_to_dto(item))
        logger.info("Batch got %d items", len(results))
        return results
    except Exception as e:
        logger.error("Error batch getting items: %s", e)
        raise
