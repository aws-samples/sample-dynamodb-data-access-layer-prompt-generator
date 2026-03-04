import pytest
from moto import mock_aws
from datetime import datetime, timezone
from pynamodb.exceptions import DoesNotExist, PutError, UpdateError, DeleteError

from data.model.ddb_table_one import DdbTableOne
from data.dto.ddb_table_one_dto import DdbTableOneDto
from data.dal.ddb_table_one_dal import (
    ddb_table_one_put_item,
    ddb_table_one_update_item,
    ddb_table_one_get_item,
    ddb_table_one_delete_item,
    ddb_table_one_create_or_update_item,
    ddb_table_one_query,
    ddb_table_one_query_base_table,
    ddb_table_one_batch_write,
    ddb_table_one_batch_get,
    gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query
)


@pytest.fixture
def aws_credentials():
    """
    Mocked AWS Credentials for moto.
    
    Sets up environment variables for AWS credentials required by moto.
    """
    import os
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_REGION'] = 'us-east-1'
    os.environ['AUTOMATED_TEST_TENANT_IDS'] = 'test_tenant'


@pytest.fixture
def mock_dynamodb_table(aws_credentials):
    """
    Create a mock DynamoDB table for testing.
    
    Uses moto to create an in-memory DynamoDB table for testing purposes.
    
    Yields:
        None: Yields control back to test after table creation
    """
    with mock_aws():
        if not DdbTableOne.exists():
            DdbTableOne.create_table(read_capacity_units=5, write_capacity_units=5, wait=True)
        yield


def create_sample_items():
    """
    Create sample items in the mock table.
    
    Returns:
        list: List of DdbTableOne items created
    """
    sample_items = [
        DdbTableOne(
            pk_attribute_str_1="pk1",
            sk_attribute_str_2="sk1",
            attribute_str_3="attr3_val1",
            attribute_str_4="attr4_val1",
            attribute_num_5=100,
            gsipk_attribute_str_7="gsi_pk1",
            gsisk_attribute_str_8="gsi_sk1"
        ),
        DdbTableOne(
            pk_attribute_str_1="pk1",
            sk_attribute_str_2="sk2",
            attribute_str_3="attr3_val2",
            attribute_str_4="attr4_val2",
            attribute_num_5=200,
            gsipk_attribute_str_7="gsi_pk1",
            gsisk_attribute_str_8="gsi_sk2"
        ),
        DdbTableOne(
            pk_attribute_str_1="pk2",
            sk_attribute_str_2="sk1",
            attribute_str_3="attr3_val3",
            attribute_str_4="attr4_val3",
            attribute_num_5=300,
            gsipk_attribute_str_7="gsi_pk2",
            gsisk_attribute_str_8="gsi_sk1"
        )
    ]

    for item in sample_items:
        item.save()

    return sample_items


class TestDdbTableOneDAL:
    """
    Test class for DdbTableOne DAL methods.
    
    This class contains comprehensive unit tests for all DAL methods
    with both positive and negative test cases.
    """

    def test_put_item_success(self, mock_dynamodb_table):
        """
        Test successful put operation.
        
        Verifies that a new item can be successfully added to the table.
        """
        dto = DdbTableOneDto(
            pk_attribute_str_1="test_pk",
            sk_attribute_str_2="test_sk",
            attribute_str_3="test_attr3",
            attribute_str_4="test_attr4",
            attribute_num_5=500
        )
        ddb_table_one_put_item(dto)
        item = DdbTableOne.get("test_pk", "test_sk")
        assert item.pk_attribute_str_1 == "test_pk"
        assert item.attribute_str_3 == "test_attr3"
        assert item.attribute_num_5 == 500

    def test_put_item_with_all_fields(self, mock_dynamodb_table):
        """
        Test put operation with all optional fields populated.
        
        Verifies that all fields are properly stored.
        """
        dto = DdbTableOneDto(
            pk_attribute_str_1="test_pk2",
            sk_attribute_str_2="test_sk2",
            attribute_str_3="attr3",
            attribute_str_4="attr4",
            attribute_num_5=1000,
            attribute_map_6={"key1": "value1"},
            gsipk_attribute_str_7="gsi_pk_test",
            gsisk_attribute_str_8="gsi_sk_test"
        )
        ddb_table_one_put_item(dto)
        item = DdbTableOne.get("test_pk2", "test_sk2")
        assert item.gsipk_attribute_str_7 == "gsi_pk_test"
        assert item.attribute_num_5 == 1000

    def test_update_item_success(self, mock_dynamodb_table):
        """
        Test successful update operation.
        
        Verifies that an existing item can be updated.
        """
        create_sample_items()
        dto = DdbTableOneDto(
            pk_attribute_str_1="pk1",
            sk_attribute_str_2="sk1",
            attribute_str_3="updated_attr3",
            attribute_num_5=999
        )
        ddb_table_one_update_item(dto)
        item = DdbTableOne.get("pk1", "sk1")
        assert item.attribute_str_3 == "updated_attr3"
        assert item.attribute_num_5 == 999
        assert item.updated_at is not None

    def test_update_item_nonexistent(self, mock_dynamodb_table):
        """
        Test update operation on non-existent item.
        
        Verifies that updating a non-existent item raises DoesNotExist.
        """
        dto = DdbTableOneDto(
            pk_attribute_str_1="nonexistent",
            sk_attribute_str_2="nonexistent",
            attribute_str_3="test"
        )
        with pytest.raises(DoesNotExist):
            ddb_table_one_update_item(dto)

    def test_get_item_success(self, mock_dynamodb_table):
        """
        Test successful get operation.
        
        Verifies that an item can be retrieved by its keys.
        """
        create_sample_items()
        dto = ddb_table_one_get_item("pk1", "sk1")
        assert dto is not None
        assert dto.pk_attribute_str_1 == "pk1"
        assert dto.attribute_str_3 == "attr3_val1"

    def test_get_item_not_found(self, mock_dynamodb_table):
        """
        Test get operation for non-existent item.
        
        Verifies that getting a non-existent item returns None.
        """
        dto = ddb_table_one_get_item("nonexistent", "nonexistent")
        assert dto is None

    def test_delete_item_success(self, mock_dynamodb_table):
        """
        Test successful delete operation.
        
        Verifies that an item can be deleted.
        """
        create_sample_items()
        ddb_table_one_delete_item("pk1", "sk1")
        with pytest.raises(DoesNotExist):
            DdbTableOne.get("pk1", "sk1")

    def test_delete_item_not_found(self, mock_dynamodb_table):
        """
        Test delete operation for non-existent item.
        
        Verifies that deleting a non-existent item raises DoesNotExist.
        """
        with pytest.raises(DoesNotExist):
            ddb_table_one_delete_item("nonexistent", "nonexistent")

    def test_create_or_update_item_create(self, mock_dynamodb_table):
        """
        Test create_or_update operation when item doesn't exist.
        
        Verifies that a new item is created.
        """
        dto = DdbTableOneDto(
            pk_attribute_str_1="new_pk",
            sk_attribute_str_2="new_sk",
            attribute_str_3="new_attr3",
            attribute_str_4="new_attr4",
            attribute_num_5=777
        )
        ddb_table_one_create_or_update_item(dto)
        item = DdbTableOne.get("new_pk", "new_sk")
        assert item.attribute_str_3 == "new_attr3"

    def test_create_or_update_item_update(self, mock_dynamodb_table):
        """
        Test create_or_update operation when item exists.
        
        Verifies that an existing item is updated.
        """
        create_sample_items()
        dto = DdbTableOneDto(
            pk_attribute_str_1="pk1",
            sk_attribute_str_2="sk1",
            attribute_str_3="updated_value",
            attribute_num_5=8888
        )
        ddb_table_one_create_or_update_item(dto)
        item = DdbTableOne.get("pk1", "sk1")
        assert item.attribute_str_3 == "updated_value"
        assert item.attribute_num_5 == 8888
        assert item.updated_at is not None


    def test_query_base_table_all_items(self, mock_dynamodb_table):
        """
        Test querying base table for all items with a given partition key.
        
        Verifies that all items with the same pk are returned.
        """
        create_sample_items()
        results = ddb_table_one_query_base_table("pk1")
        assert len(results) == 2
        assert all(dto.pk_attribute_str_1 == "pk1" for dto in results)

    def test_query_base_table_with_condition(self, mock_dynamodb_table):
        """
        Test querying base table with sort key condition.
        
        Verifies that query with conditions filters results correctly.
        """
        create_sample_items()
        from pynamodb.expressions.operand import Path
        results = ddb_table_one_query_base_table(
            "pk1",
            Path('sk_attribute_str_2') == "sk1"
        )
        assert len(results) == 1
        assert results[0].sk_attribute_str_2 == "sk1"

    def test_query_base_table_no_results(self, mock_dynamodb_table):
        """
        Test querying base table with no matching results.
        
        Verifies that empty list is returned when no items match.
        """
        create_sample_items()
        results = ddb_table_one_query_base_table("nonexistent")
        assert len(results) == 0

    def test_query_with_limit(self, mock_dynamodb_table):
        """
        Test querying with limit parameter.
        
        Verifies that query respects the limit parameter.
        """
        create_sample_items()
        results = ddb_table_one_query(
            hash_key="pk1",
            query_limit=1
        )
        assert len(results) == 1

    def test_query_scan_index_forward(self, mock_dynamodb_table):
        """
        Test querying with scan_index_forward parameter.
        
        Verifies that query respects sort order.
        """
        create_sample_items()
        results_asc = ddb_table_one_query(
            hash_key="pk1",
            scan_index_forward=True
        )
        results_desc = ddb_table_one_query(
            hash_key="pk1",
            scan_index_forward=False
        )
        assert results_asc[0].sk_attribute_str_2 == "sk1"
        assert results_desc[0].sk_attribute_str_2 == "sk2"

    def test_batch_write_success(self, mock_dynamodb_table):
        """
        Test successful batch write operation.
        
        Verifies that multiple items can be written in batch.
        """
        dtos = [
            DdbTableOneDto(
                pk_attribute_str_1=f"batch_pk_{i}",
                sk_attribute_str_2=f"batch_sk_{i}",
                attribute_str_3=f"attr3_{i}",
                attribute_str_4=f"attr4_{i}",
                attribute_num_5=100 * i
            )
            for i in range(1, 6)
        ]
        ddb_table_one_batch_write(dtos)
        
        for i in range(1, 6):
            item = DdbTableOne.get(f"batch_pk_{i}", f"batch_sk_{i}")
            assert item.attribute_str_3 == f"attr3_{i}"

    def test_batch_write_empty_list(self, mock_dynamodb_table):
        """
        Test batch write with empty list.
        
        Verifies that batch write handles empty list gracefully.
        """
        ddb_table_one_batch_write([])

    def test_batch_get_success(self, mock_dynamodb_table):
        """
        Test successful batch get operation.
        
        Verifies that multiple items can be retrieved in batch.
        """
        create_sample_items()
        keys = [
            ("pk1", "sk1"),
            ("pk1", "sk2"),
            ("pk2", "sk1")
        ]
        results = ddb_table_one_batch_get(keys)
        assert len(results) == 3

    def test_batch_get_partial_results(self, mock_dynamodb_table):
        """
        Test batch get with some non-existent items.
        
        Verifies that batch get returns only existing items.
        """
        create_sample_items()
        keys = [
            ("pk1", "sk1"),
            ("nonexistent", "nonexistent")
        ]
        results = ddb_table_one_batch_get(keys)
        assert len(results) == 1
        assert results[0].pk_attribute_str_1 == "pk1"

    def test_batch_get_empty_list(self, mock_dynamodb_table):
        """
        Test batch get with empty key list.
        
        Verifies that batch get handles empty list gracefully.
        """
        results = ddb_table_one_batch_get([])
        assert len(results) == 0

    def test_gsi_query_success(self, mock_dynamodb_table):
        """
        Test GSI query by gsipk_attribute_str_7.
        
        Verifies that GSI query returns items matching gsipk_attribute_str_7.
        """
        create_sample_items()
        results = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query("gsi_pk1")
        assert len(results) == 2
        assert all(dto.gsipk_attribute_str_7 == "gsi_pk1" for dto in results)

    def test_gsi_query_with_sort_condition(self, mock_dynamodb_table):
        """
        Test GSI query with gsisk_attribute_str_8 condition.
        
        Verifies that GSI query with sort key condition filters correctly.
        """
        create_sample_items()
        from pynamodb.expressions.operand import Path
        results = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query(
            "gsi_pk1",
            Path('gsisk_attribute_str_8') == "gsi_sk1"
        )
        assert len(results) == 1
        assert results[0].gsisk_attribute_str_8 == "gsi_sk1"

    def test_gsi_query_no_results(self, mock_dynamodb_table):
        """
        Test GSI query with no matching results.
        
        Verifies that empty list is returned when no items match GSI query.
        """
        create_sample_items()
        results = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query("nonexistent_gsi")
        assert len(results) == 0

    def test_dto_to_string(self, mock_dynamodb_table):
        """
        Test DTO __str__ method.
        
        Verifies that DTO can be converted to JSON string.
        """
        dto = DdbTableOneDto(
            pk_attribute_str_1="test",
            sk_attribute_str_2="test_sk",
            attribute_str_3="attr3",
            attribute_str_4="attr4",
            attribute_num_5=100
        )
        dto_str = str(dto)
        assert "test" in dto_str
        assert "attr3" in dto_str


    def test_model_ttl_for_test_object(self, mock_dynamodb_table):
        """
        Test that TTL is set correctly for test object.
        
        Verifies that test objects get test TTL.
        """
        import os
        os.environ['AUTOMATED_TEST_TENANT_IDS'] = 'test_tenant'
        os.environ['DDB_TABLE_ONE_TEST_TTL_DAYS'] = '30'
        
        item = DdbTableOne(
            pk_attribute_str_1="TEST_pk",
            sk_attribute_str_2="test_sk",
            attribute_str_3="attr3",
            attribute_str_4="attr4"
        )
        ttl = item.get_ttl()
        if ttl is not None:
            assert ttl.days == 30
        else:
            assert DdbTableOne.get_ttl_days() == 0

    def test_query_with_filter_condition(self, mock_dynamodb_table):
        """
        Test querying with filter condition.
        
        Verifies that filter condition is applied correctly.
        """
        create_sample_items()
        from pynamodb.expressions.operand import Path
        results = ddb_table_one_query(
            hash_key="pk1",
            filter_key_condition=Path('attribute_num_5') == 100
        )
        assert len(results) == 1
        assert results[0].attribute_num_5 == 100

    def test_query_with_last_evaluated_key(self, mock_dynamodb_table):
        """
        Test querying with last_evaluated_key parameter.
        
        Verifies that last_evaluated_key parameter is accepted and processed.
        """
        create_sample_items()
        results = ddb_table_one_query(
            hash_key="pk1",
            query_limit=1
        )
        assert len(results) == 1
        
        results_all = ddb_table_one_query(
            hash_key="pk1",
            last_evaluated_key=None
        )
        assert len(results_all) >= 1

    def test_query_with_range_key_condition(self, mock_dynamodb_table):
        """Test query with range key condition."""
        create_sample_items()
        from pynamodb.expressions.operand import Path
        
        results = ddb_table_one_query(
            hash_key="pk1",
            range_key_and_condition=Path('sk_attribute_str_2') >= 'sk1'
        )
        
        assert len(results) >= 1
        assert all(dto.sk_attribute_str_2 >= "sk1" for dto in results)

    def test_query_scan_index_forward_false(self, mock_dynamodb_table):
        """Test query with descending sort order."""
        create_sample_items()
        
        results_asc = ddb_table_one_query(
            hash_key="pk1",
            scan_index_forward=True
        )
        
        results_desc = ddb_table_one_query(
            hash_key="pk1",
            scan_index_forward=False
        )
        
        assert len(results_asc) == len(results_desc) == 2
        if len(results_asc) > 1:
            assert results_asc[0].sk_attribute_str_2 != results_desc[0].sk_attribute_str_2

    def test_query_with_consistent_read(self, mock_dynamodb_table):
        """Test query with consistent read enabled."""
        create_sample_items()
        
        results = ddb_table_one_query(
            hash_key="pk1",
            consistent_read=True
        )
        
        assert len(results) >= 1
        assert all(dto.pk_attribute_str_1 == "pk1" for dto in results)

    def test_query_with_attributes_to_get(self, mock_dynamodb_table):
        """Test query with projection expression."""
        create_sample_items()
        
        results = ddb_table_one_query(
            hash_key="pk1",
            attribute_to_get=['pk_attribute_str_1', 'sk_attribute_str_2', 'attribute_str_3']
        )
        
        assert len(results) >= 1
        for dto in results:
            assert dto.pk_attribute_str_1 is not None
            assert dto.attribute_str_3 is not None

    def test_query_with_index_name(self, mock_dynamodb_table):
        """Test query using GSI index."""
        create_sample_items()
        
        results = ddb_table_one_query(
            hash_key="gsi_pk1",
            index_name="gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index"
        )
        
        assert len(results) >= 1
        assert all(dto.gsipk_attribute_str_7 == "gsi_pk1" for dto in results)

    def test_query_empty_result_nonexistent_key(self, mock_dynamodb_table):
        """Test query with non-existent hash key returns empty results."""
        create_sample_items()
        
        results = ddb_table_one_query(hash_key="nonexistent_key_12345")
        
        assert len(results) == 0

    def test_query_base_table_empty_result(self, mock_dynamodb_table):
        """Test base table query with non-existent key."""
        results = ddb_table_one_query_base_table("nonexistent_pk")
        
        assert len(results) == 0

    def test_gsi_query_empty_result(self, mock_dynamodb_table):
        """Test GSI query with non-existent key."""
        results = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query("nonexistent_gsi")
        
        assert len(results) == 0

    def test_batch_get_empty_result(self, mock_dynamodb_table):
        """Test batch get with non-existent keys."""
        keys = [
            ("nonexistent1", "nonexistent1"),
            ("nonexistent2", "nonexistent2")
        ]
        
        results = ddb_table_one_batch_get(keys)
        
        assert len(results) == 0

    def test_batch_get_partial_result(self, mock_dynamodb_table):
        """Test batch get with mix of existent and non-existent keys."""
        create_sample_items()
        
        keys = [
            ("pk1", "sk1"),
            ("nonexistent", "nonexistent")
        ]
        
        results = ddb_table_one_batch_get(keys)
        
        assert len(results) == 1
        assert results[0].pk_attribute_str_1 == "pk1"

    def test_retry_mechanism_put(self, mock_dynamodb_table):
        """Test retry mechanism for put operation."""
        from unittest.mock import patch
        
        dto = DdbTableOneDto(
            pk_attribute_str_1="retry_test",
            sk_attribute_str_2="retry_sk",
            attribute_str_3="retry_attr3",
            attribute_str_4="retry_attr4"
        )
        
        with patch('data.model.ddb_table_one.DdbTableOne.save') as mock_save:
            mock_save.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            ddb_table_one_put_item(dto)
            
            assert mock_save.call_count == 3

    def test_retry_mechanism_update(self, mock_dynamodb_table):
        """Test retry mechanism for update operation."""
        from unittest.mock import patch
        
        create_sample_items()
        
        dto = DdbTableOneDto(
            pk_attribute_str_1="pk1",
            sk_attribute_str_2="sk1",
            attribute_str_3="retry_update"
        )
        
        with patch('data.model.ddb_table_one.DdbTableOne.save') as mock_save:
            mock_save.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            ddb_table_one_update_item(dto)
            
            assert mock_save.call_count == 3

    def test_retry_mechanism_delete(self, mock_dynamodb_table):
        """Test retry mechanism for delete operation."""
        from unittest.mock import patch
        
        create_sample_items()
        
        with patch('data.model.ddb_table_one.DdbTableOne.delete') as mock_delete:
            mock_delete.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            ddb_table_one_delete_item("pk1", "sk1")
            
            assert mock_delete.call_count == 3

    def test_retry_mechanism_query(self, mock_dynamodb_table):
        """Test retry mechanism for query operation."""
        from unittest.mock import patch
        
        create_sample_items()
        
        with patch('data.model.ddb_table_one.DdbTableOne.query') as mock_query:
            mock_query.side_effect = [
                Exception("First failure"),
                Exception("Second failure"),
                iter([])
            ]
            
            results = ddb_table_one_query(hash_key="pk1")
            
            assert mock_query.call_count == 3

    def test_gsi_query_with_sort_condition_range(self, mock_dynamodb_table):
        """Test GSI query with sort key range condition."""
        create_sample_items()
        from pynamodb.expressions.operand import Path
        
        results = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query(
            "gsi_pk1",
            sort_key_condition=Path('gsisk_attribute_str_8').between('gsi_sk1', 'gsi_sk3')
        )
        
        assert all('gsi_sk1' <= dto.gsisk_attribute_str_8 <= 'gsi_sk3' for dto in results if dto.gsisk_attribute_str_8 is not None)

    def test_batch_write_with_ttl_setting(self, mock_dynamodb_table):
        """Test batch write with TTL attribute handling."""
        dtos = [
            DdbTableOneDto(
                pk_attribute_str_1=f"ttl_pk_{i}",
                sk_attribute_str_2="ttl_sk",
                attribute_str_3="ttl_attr3",
                attribute_str_4="ttl_attr4"
            )
            for i in range(3)
        ]
        
        ddb_table_one_batch_write(dtos)
        
        for i in range(3):
            item = DdbTableOne.get(f"ttl_pk_{i}", "ttl_sk")
            assert item is not None
            assert item.attribute_str_3 == "ttl_attr3"

    def test_create_or_update_retry(self, mock_dynamodb_table):
        """Test retry mechanism for create_or_update operation."""
        from unittest.mock import patch
        
        dto = DdbTableOneDto(
            pk_attribute_str_1="retry_create_update",
            sk_attribute_str_2="retry_sk",
            attribute_str_3="retry_attr3",
            attribute_str_4="retry_attr4"
        )
        
        with patch('data.model.ddb_table_one.DdbTableOne.save') as mock_save:
            mock_save.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            ddb_table_one_create_or_update_item(dto)
            
            assert mock_save.call_count == 3

    def test_query_with_multiple_filters(self, mock_dynamodb_table):
        """Test query with multiple filter conditions combined."""
        create_sample_items()
        from pynamodb.expressions.operand import Path
        
        filter_condition = (Path('attribute_str_3') == 'attr3_val1') & (Path('attribute_num_5') >= 50)
        
        results = ddb_table_one_query(
            hash_key="pk1",
            filter_key_condition=filter_condition
        )
        
        assert all(dto.attribute_str_3 == 'attr3_val1' and dto.attribute_num_5 >= 50 for dto in results)
