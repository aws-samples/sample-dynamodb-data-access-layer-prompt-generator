from datetime import datetime, timezone
from typing import List, Optional

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pynamodb.exceptions import PutError, UpdateError, DoesNotExist, DeleteError, QueryError

from aws_lambda_powertools.logging.logger import Logger
logger = Logger()

from data.model.policy_manager import PolicyManager
from data.dto.policy_manager_dto import PolicyManagerDto

RETRY_ATTEMPTS = 3
BACKOFF_DELAY = 2


def dto_to_model(dto: PolicyManagerDto) -> PolicyManager:
    """
    Convert DTO to model instance (without saving).
    
    Args:
        dto (PolicyManagerDto): DTO instance to convert
        
    Returns:
        PolicyManager: Model instance
    """
    return PolicyManager(
        pk=dto.pk,
        sk=dto.sk,
        policy_id=dto.policy_id,
        policy_name=dto.policy_name,
        risk_id=dto.risk_id,
        address=dto.address,
        premium=dto.premium,
        sum_insured=dto.sum_insured,
        policy_version=dto.policy_version,
        start_date=dto.start_date,
        end_date=dto.end_date,
        limit=dto.limit,
        deductible=dto.deductible,
        amount=dto.amount,
        gsipk1=dto.gsipk1,
        gsisk1=dto.gsisk1,
        gsipk2=dto.gsipk2,
        gsisk2=dto.gsisk2
    )


def model_to_dto(item: PolicyManager) -> PolicyManagerDto:
    """
    Convert model instance to DTO.
    
    Args:
        item (PolicyManager): Model instance to convert
        
    Returns:
        PolicyManagerDto: DTO instance
    """
    return PolicyManagerDto(
        pk=item.pk,
        sk=item.sk,
        policy_id=item.policy_id,
        policy_name=item.policy_name,
        risk_id=item.risk_id,
        address=item.address,
        premium=item.premium,
        sum_insured=item.sum_insured,
        policy_version=item.policy_version,
        start_date=item.start_date,
        end_date=item.end_date,
        limit=item.limit,
        deductible=item.deductible,
        amount=item.amount,
        gsipk1=item.gsipk1,
        gsisk1=item.gsisk1,
        gsipk2=item.gsipk2,
        gsisk2=item.gsisk2
    )


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception, PutError)), reraise=True)
def policy_manager_put_item(dto: PolicyManagerDto) -> None:
    """
    Put an item into the table.
    
    Args:
        dto (PolicyManagerDto): DTO containing item data
        
    Raises:
        PutError: If put operation fails (including ConditionalCheckFailedException)
        Exception: If any other error occurs
    """
    try:
        item = dto_to_model(dto)
        item.created_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        item.save()
        logger.info("Successfully put item %s %s", dto.pk, dto.sk)
    except PutError as e:
        if hasattr(e, 'cause_response_code') and e.cause_response_code == 'ConditionalCheckFailedException':
            logger.error("Conditional check failed for item %s %s: %s", dto.pk, dto.sk, e)
        else:
            logger.error("Error putting item %s %s: %s", dto.pk, dto.sk, e)
        raise
    except Exception as e:
        logger.error("Error putting item %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception, PutError)), reraise=True)
def policy_manager_update_item(dto: PolicyManagerDto) -> None:
    """
    Update an item in the table.
    
    Args:
        dto (PolicyManagerDto): DTO containing updated item data
        
    Raises:
        PutError: If save operation fails (including ConditionalCheckFailedException)
        DoesNotExist: If item does not exist
        Exception: If any other error occurs
    """
    try:
        item = PolicyManager.get(dto.pk, dto.sk)
        if dto.policy_id is not None:
            item.policy_id = dto.policy_id
        if dto.policy_name is not None:
            item.policy_name = dto.policy_name
        if dto.risk_id is not None:
            item.risk_id = dto.risk_id
        if dto.address is not None:
            item.address = dto.address
        if dto.premium is not None:
            item.premium = dto.premium
        if dto.sum_insured is not None:
            item.sum_insured = dto.sum_insured
        if dto.policy_version is not None:
            item.policy_version = dto.policy_version
        if dto.start_date is not None:
            item.start_date = dto.start_date
        if dto.end_date is not None:
            item.end_date = dto.end_date
        if dto.limit is not None:
            item.limit = dto.limit
        if dto.deductible is not None:
            item.deductible = dto.deductible
        if dto.amount is not None:
            item.amount = dto.amount
        if dto.gsipk1 is not None:
            item.gsipk1 = dto.gsipk1
        if dto.gsisk1 is not None:
            item.gsisk1 = dto.gsisk1
        if dto.gsipk2 is not None:
            item.gsipk2 = dto.gsipk2
        if dto.gsisk2 is not None:
            item.gsisk2 = dto.gsisk2
        item.updated_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        item.save()
        logger.info("Successfully updated item %s %s", dto.pk, dto.sk)
    except DoesNotExist as e:
        logger.error("Item not found for update %s %s: %s", dto.pk, dto.sk, e)
        raise
    except PutError as e:
        if hasattr(e, 'cause_response_code') and e.cause_response_code == 'ConditionalCheckFailedException':
            logger.error("Conditional check failed for update %s %s: %s", dto.pk, dto.sk, e)
        else:
            logger.error("Error updating item %s %s: %s", dto.pk, dto.sk, e)
        raise
    except Exception as e:
        logger.error("Error updating item %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception,)), reraise=True)
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
        item = PolicyManager.get(pk, sk)
        dto = model_to_dto(item)
        logger.info("Successfully got item %s %s", pk, sk)
        return dto
    except DoesNotExist:
        logger.warning("Item not found %s %s", pk, sk)
        return None
    except Exception as e:
        logger.error("Error getting item %s %s: %s", pk, sk, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception, DeleteError)), reraise=True)
def policy_manager_delete_item(pk: str, sk: str) -> None:
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
        item = PolicyManager.get(pk, sk)
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


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception, PutError)), reraise=True)
def policy_manager_create_or_update_item(dto: PolicyManagerDto) -> None:
    """
    Create or update an item depending on existence.
    
    Args:
        dto (PolicyManagerDto): DTO containing item data
        
    Raises:
        PutError: If save operation fails (including ConditionalCheckFailedException)
        Exception: If any other error occurs
    """
    try:
        try:
            item = PolicyManager.get(dto.pk, dto.sk)
            # update path
            if dto.policy_id is not None:
                item.policy_id = dto.policy_id
            if dto.policy_name is not None:
                item.policy_name = dto.policy_name
            if dto.risk_id is not None:
                item.risk_id = dto.risk_id
            if dto.address is not None:
                item.address = dto.address
            if dto.premium is not None:
                item.premium = dto.premium
            if dto.sum_insured is not None:
                item.sum_insured = dto.sum_insured
            if dto.policy_version is not None:
                item.policy_version = dto.policy_version
            if dto.start_date is not None:
                item.start_date = dto.start_date
            if dto.end_date is not None:
                item.end_date = dto.end_date
            if dto.limit is not None:
                item.limit = dto.limit
            if dto.deductible is not None:
                item.deductible = dto.deductible
            if dto.amount is not None:
                item.amount = dto.amount
            if dto.gsipk1 is not None:
                item.gsipk1 = dto.gsipk1
            if dto.gsisk1 is not None:
                item.gsisk1 = dto.gsisk1
            if dto.gsipk2 is not None:
                item.gsipk2 = dto.gsipk2
            if dto.gsisk2 is not None:
                item.gsisk2 = dto.gsisk2
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
            logger.error("Conditional check failed for item %s %s: %s", dto.pk, dto.sk, e)
        else:
            logger.error("Error create/update %s %s: %s", dto.pk, dto.sk, e)
        raise
    except Exception as e:
        logger.error("Error create/update %s %s: %s", dto.pk, dto.sk, e)
        raise


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception, QueryError)), reraise=True)
def policy_manager_query(hash_key: str, range_key_and_condition=None, filter_key_condition=None,
                        consistent_read: bool = False, index_name: str = None, query_limit: int = None,
                        scan_index_forward: bool = True, attribute_to_get=None, last_evaluated_key: dict = None,
                        page_size: int = None, rate_limit: int = None) -> List[PolicyManagerDto]:
    """
    Query the table using specified parameters until all pages are retrieved.
    
    Args:
        hash_key (str): Hash key value to query
        range_key_and_condition: Optional range key condition
        filter_key_condition: Optional filter condition
        consistent_read (bool): Whether to use consistent read (default: False)
        index_name (str): Optional index name (GSI or LSI)
        query_limit (int): Optional query limit
        scan_index_forward (bool): Sort order (default: True)
        attribute_to_get: Optional attributes to retrieve
        last_evaluated_key (dict): Optional last evaluated key for pagination
        page_size (int): Optional page size
        rate_limit (int): Optional rate limit
        
    Returns:
        List[PolicyManagerDto]: List of DTOs matching the query
        
    Raises:
        QueryError: If query operation fails
        Exception: If any other error occurs
    """
    try:
        results: List[PolicyManagerDto] = []
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

        for item in PolicyManager.query(**query_kwargs):
            results.append(model_to_dto(item))

        logger.info("Query returned %d items for hash_key=%s", len(results), hash_key)
        return results
    except QueryError as e:
        logger.error("Error querying for hash_key=%s: %s", hash_key, e)
        raise
    except Exception as e:
        logger.error("Error querying for hash_key=%s: %s", hash_key, e)
        raise


def policy_manager_query_base_table(pk: str, sort_key_condition=None) -> List[PolicyManagerDto]:
    """
    Wrapper to query base table by pk and optional sort key condition.
    
    Args:
        pk (str): Partition/hash key
        sort_key_condition: Optional sort key condition
        
    Returns:
        List[PolicyManagerDto]: List of DTOs matching the query
    """
    return policy_manager_query(
        hash_key=pk,
        range_key_and_condition=sort_key_condition
    )


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception,)), reraise=True)
def policy_manager_batch_write(dtos: List[PolicyManagerDto]) -> None:
    """
    Batch write items; set TTL attribute for each item prior to save.
    Falls back to individual writes on failure.
    
    Args:
        dtos (List[PolicyManagerDto]): List of DTOs to write
        
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
        
        with PolicyManager.batch_write() as batch:
            for item in items:
                batch.save(item)
        
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


@retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_exponential(multiplier=BACKOFF_DELAY), retry=retry_if_exception_type((Exception,)), reraise=True)
def policy_manager_batch_get(keys: List[tuple]) -> List[PolicyManagerDto]:
    """
    Batch get items by list of (pk, sk) keys.
    
    Args:
        keys (List[tuple]): List of tuples containing (pk, sk)
        
    Returns:
        List[PolicyManagerDto]: List of DTOs retrieved
        
    Raises:
        Exception: If batch get operation fails
    """
    try:
        results: List[PolicyManagerDto] = []
        for item in PolicyManager.batch_get(keys):
            results.append(model_to_dto(item))
        logger.info("Batch got %d items", len(results))
        return results
    except Exception as e:
        logger.error("Error batch getting items: %s", e)
        raise


def gsi__gsipk1__gsisk1__index_query(gsipk1: str, sort_key_condition=None) -> List[PolicyManagerDto]:
    """
    Wrapper for GSI query by gsipk1 and optional gsisk1 condition.
    
    Args:
        gsipk1 (str): GSI partition key to query
        sort_key_condition: Optional gsisk1 condition
        
    Returns:
        List[PolicyManagerDto]: List of DTOs matching the query
    """
    return policy_manager_query(
        hash_key=gsipk1,
        range_key_and_condition=sort_key_condition,
        index_name="gsi__gsipk1__gsisk1__index"
    )


def gsi__gsipk2__gsisk2__index_query(gsipk2: str, sort_key_condition=None) -> List[PolicyManagerDto]:
    """
    Wrapper for GSI query by gsipk2 and optional gsisk2 condition.
    
    Args:
        gsipk2 (str): GSI partition key to query
        sort_key_condition: Optional gsisk2 condition
        
    Returns:
        List[PolicyManagerDto]: List of DTOs matching the query
    """
    return policy_manager_query(
        hash_key=gsipk2,
        range_key_and_condition=sort_key_condition,
        index_name="gsi__gsipk2__gsisk2__index"
    )


def lsi__policy_version__index_query(pk: str, sort_key_condition=None, filter_key_condition=None) -> List[PolicyManagerDto]:
    """
    Wrapper for LSI query by pk (table partition key) and optional policy_version condition.
    
    Args:
        pk (str): Table partition key to query (LSI uses same partition key as base table)
        sort_key_condition: Optional policy_version sort key condition
        filter_key_condition: Optional filter condition
        
    Returns:
        List[PolicyManagerDto]: List of DTOs matching the query
    """
    return policy_manager_query(
        hash_key=pk,
        range_key_and_condition=sort_key_condition,
        filter_key_condition=filter_key_condition,
        index_name="lsi__policy_version__index"
    )
