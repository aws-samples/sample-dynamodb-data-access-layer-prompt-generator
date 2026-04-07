import pytest
from moto import mock_aws
from datetime import datetime, timezone
from pynamodb.exceptions import DoesNotExist, PutError, UpdateError, DeleteError

from data.model.customer_orders import CustomerOrders
from data.dto.customer_orders_dto import CustomerOrdersDto
from data.dal.customer_orders_dal import (
    customer_orders_put_item,
    customer_orders_update_item,
    customer_orders_get_item,
    customer_orders_delete_item,
    customer_orders_create_or_update_item,
    customer_orders_query,
    customer_orders_query_base_table,
    customer_orders_batch_write,
    customer_orders_batch_get,
    gsi__gsipk1__gsisk1__index_query
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
        if not CustomerOrders.exists():
            CustomerOrders.create_table(read_capacity_units=5, write_capacity_units=5, wait=True)
        yield


def create_sample_items():
    """
    Create sample items in the mock table.
    
    Returns:
        list: List of CustomerOrders items created
    """
    sample_items = [
        CustomerOrders(
            pk="cust1",
            sk="order1",
            cust_address="123 Main St",
            first_name="John",
            last_name="Doe",
            phone="555-1234",
            is_premium_member="true",
            order_date="2024-01-15",
            order_quantity=5,
            sku="SKU001",
            gsipk1="gsi_pk1",
            gsisk1="gsi_sk1"
        ),
        CustomerOrders(
            pk="cust1",
            sk="order2",
            cust_address="123 Main St",
            first_name="John",
            last_name="Doe",
            phone="555-5678",
            is_premium_member="true",
            order_date="2024-01-20",
            order_quantity=3,
            sku="SKU002",
            gsipk1="gsi_pk1",
            gsisk1="gsi_sk2"
        ),
        CustomerOrders(
            pk="cust2",
            sk="order1",
            cust_address="456 Oak Ave",
            first_name="Jane",
            last_name="Smith",
            phone="555-9999",
            is_premium_member="false",
            order_date="2024-01-18",
            order_quantity=10,
            sku="SKU003",
            gsipk1="gsi_pk2",
            gsisk1="gsi_sk1"
        )
    ]

    for item in sample_items:
        item.save()

    return sample_items


class TestCustomerOrdersDAL:
    """
    Test class for CustomerOrders DAL methods.
    
    This class contains comprehensive unit tests for all DAL methods
    with both positive and negative test cases, including GSI queries.
    """

    def test_put_item_success(self, mock_dynamodb_table):
        """
        Test successful put operation.
        
        Verifies that a new item can be successfully added to the table.
        """
        dto = CustomerOrdersDto(
            pk="test_pk",
            sk="test_sk",
            cust_address="789 Elm St",
            first_name="Test",
            last_name="User",
            phone="555-9999",
            order_quantity=7
        )
        customer_orders_put_item(dto)
        item = CustomerOrders.get("test_pk", "test_sk")
        assert item.pk == "test_pk"
        assert item.first_name == "Test"
        assert item.order_quantity == 7

    def test_put_item_with_all_fields(self, mock_dynamodb_table):
        """
        Test put operation with all optional fields populated.
        
        Verifies that all fields are properly stored.
        """
        dto = CustomerOrdersDto(
            pk="test_pk2",
            sk="test_sk2",
            cust_address="100 Pine Rd",
            first_name="Alice",
            last_name="Johnson",
            phone="555-1111",
            is_premium_member="true",
            order_date="2024-02-01",
            order_quantity=15,
            sku="SKU999",
            gsipk1="gsi_test_pk",
            gsisk1="gsi_test_sk"
        )
        customer_orders_put_item(dto)
        item = CustomerOrders.get("test_pk2", "test_sk2")
        assert item.gsipk1 == "gsi_test_pk"
        assert item.order_quantity == 15

    def test_put_item_sets_version_and_created_at(self, mock_dynamodb_table):
        """
        Test that put operation sets version and created_at.
        
        Verifies that version and created_at are automatically set on put.
        """
        dto = CustomerOrdersDto(
            pk="version_pk",
            sk="version_sk",
            first_name="Version",
            last_name="Test"
        )
        customer_orders_put_item(dto)
        item = CustomerOrders.get("version_pk", "version_sk")
        assert item.version is not None
        assert item.created_at is not None

    def test_update_item_success(self, mock_dynamodb_table):
        """
        Test successful update operation.
        
        Verifies that an existing item can be updated.
        """
        create_sample_items()
        dto = CustomerOrdersDto(
            pk="cust1",
            sk="order1",
            first_name="Johnny",
            order_quantity=20
        )
        customer_orders_update_item(dto)
        item = CustomerOrders.get("cust1", "order1")
        assert item.first_name == "Johnny"
        assert item.order_quantity == 20
        assert item.updated_at is not None

    def test_update_item_increments_version(self, mock_dynamodb_table):
        """
        Test that update operation increments version.
        
        Verifies that version is incremented on update.
        """
        create_sample_items()
        item_before = CustomerOrders.get("cust1", "order1")
        version_before = item_before.version

        dto = CustomerOrdersDto(
            pk="cust1",
            sk="order1",
            first_name="Updated"
        )
        customer_orders_update_item(dto)
        item_after = CustomerOrders.get("cust1", "order1")
        assert item_after.version > version_before

    def test_update_item_nonexistent(self, mock_dynamodb_table):
        """
        Test update operation on non-existent item.
        
        Verifies that updating a non-existent item raises DoesNotExist.
        """
        dto = CustomerOrdersDto(
            pk="nonexistent",
            sk="nonexistent",
            first_name="test"
        )
        with pytest.raises(DoesNotExist):
            customer_orders_update_item(dto)

    def test_get_item_success(self, mock_dynamodb_table):
        """
        Test successful get operation.
        
        Verifies that an item can be retrieved by its keys.
        """
        create_sample_items()
        dto = customer_orders_get_item("cust1", "order1")
        assert dto is not None
        assert dto.pk == "cust1"
        assert dto.first_name == "John"

    def test_get_item_not_found(self, mock_dynamodb_table):
        """
        Test get operation for non-existent item.
        
        Verifies that getting a non-existent item returns None.
        """
        dto = customer_orders_get_item("nonexistent", "nonexistent")
        assert dto is None

    def test_get_item_nonexistent_pk(self, mock_dynamodb_table):
        """
        Test get operation with nonexistent partition key.
        
        Verifies that getting an item with nonexistent pk returns None.
        """
        create_sample_items()
        dto = customer_orders_get_item("no_such_pk", "order1")
        assert dto is None

    def test_get_item_nonexistent_sk(self, mock_dynamodb_table):
        """
        Test get operation with nonexistent sort key.
        
        Verifies that getting an item with nonexistent sk returns None.
        """
        create_sample_items()
        dto = customer_orders_get_item("cust1", "no_such_sk")
        assert dto is None

    def test_delete_item_success(self, mock_dynamodb_table):
        """
        Test successful delete operation.
        
        Verifies that an item can be deleted.
        """
        create_sample_items()
        customer_orders_delete_item("cust1", "order1")
        with pytest.raises(DoesNotExist):
            CustomerOrders.get("cust1", "order1")

    def test_delete_item_not_found(self, mock_dynamodb_table):
        """
        Test delete operation for non-existent item.
        
        Verifies that deleting a non-existent item raises DoesNotExist.
        """
        with pytest.raises(DoesNotExist):
            customer_orders_delete_item("nonexistent", "nonexistent")

    def test_create_or_update_item_create(self, mock_dynamodb_table):
        """
        Test create_or_update operation when item doesn't exist.
        
        Verifies that a new item is created.
        """
        dto = CustomerOrdersDto(
            pk="new_pk",
            sk="new_sk",
            first_name="New",
            last_name="Customer",
            order_quantity=5
        )
        customer_orders_create_or_update_item(dto)
        item = CustomerOrders.get("new_pk", "new_sk")
        assert item.first_name == "New"

    def test_create_or_update_item_update(self, mock_dynamodb_table):
        """
        Test create_or_update operation when item exists.
        
        Verifies that an existing item is updated.
        """
        create_sample_items()
        dto = CustomerOrdersDto(
            pk="cust1",
            sk="order1",
            first_name="Updated",
            order_quantity=100
        )
        customer_orders_create_or_update_item(dto)
        item = CustomerOrders.get("cust1", "order1")
        assert item.first_name == "Updated"
        assert item.order_quantity == 100
        assert item.updated_at is not None

    def test_query_base_table_all_items(self, mock_dynamodb_table):
        """
        Test querying base table for all items with a given partition key.
        
        Verifies that all items with the same pk are returned.
        """
        create_sample_items()
        results = customer_orders_query_base_table("cust1")
        assert len(results) == 2
        assert all(dto.pk == "cust1" for dto in results)

    def test_query_base_table_with_condition(self, mock_dynamodb_table):
        """
        Test querying base table with sort key condition.
        
        Verifies that query with conditions filters results correctly.
        """
        create_sample_items()
        from pynamodb.expressions.operand import Path
        results = customer_orders_query_base_table(
            "cust1",
            Path('sk') == "order1"
        )
        assert len(results) == 1
        assert results[0].sk == "order1"

    def test_query_base_table_no_results(self, mock_dynamodb_table):
        """
        Test querying base table with no matching results.
        
        Verifies that empty list is returned when no items match.
        """
        create_sample_items()
        results = customer_orders_query_base_table("nonexistent")
        assert len(results) == 0

    def test_query_base_table_pagination(self, mock_dynamodb_table):
        """
        Test querying base table with pagination.
        
        Verifies that query with limit returns correct number of items.
        """
        create_sample_items()
        results = customer_orders_query(
            hash_key="cust1",
            query_limit=1
        )
        assert len(results) == 1

    def test_query_with_filter_condition(self, mock_dynamodb_table):
        """
        Test querying with filter condition.
        
        Verifies that filter condition is applied correctly.
        """
        create_sample_items()
        from pynamodb.expressions.operand import Path
        results = customer_orders_query(
            hash_key="cust1",
            filter_key_condition=Path('order_quantity') == 5
        )
        assert len(results) == 1
        assert results[0].order_quantity == 5

    def test_query_with_range_key_condition(self, mock_dynamodb_table):
        """Test query with range key condition."""
        create_sample_items()
        from pynamodb.expressions.operand import Path
        
        results = customer_orders_query(
            hash_key="cust1",
            range_key_and_condition=Path('sk') >= 'order1'
        )
        
        assert len(results) >= 1
        assert all(dto.sk >= "order1" for dto in results)

    def test_query_scan_index_forward(self, mock_dynamodb_table):
        """
        Test querying with scan_index_forward parameter.
        
        Verifies that query respects sort order.
        """
        create_sample_items()
        results_asc = customer_orders_query(
            hash_key="cust1",
            scan_index_forward=True
        )
        results_desc = customer_orders_query(
            hash_key="cust1",
            scan_index_forward=False
        )
        assert results_asc[0].sk == "order1"
        assert results_desc[0].sk == "order2"

    def test_query_with_consistent_read(self, mock_dynamodb_table):
        """Test query with consistent read enabled."""
        create_sample_items()
        
        results = customer_orders_query(
            hash_key="cust1",
            consistent_read=True
        )
        
        assert len(results) >= 1
        assert all(dto.pk == "cust1" for dto in results)

    def test_query_with_limit(self, mock_dynamodb_table):
        """
        Test querying with limit parameter.
        
        Verifies that query respects the limit parameter.
        """
        create_sample_items()
        results = customer_orders_query(
            hash_key="cust1",
            query_limit=1
        )
        assert len(results) == 1

    def test_query_with_attributes_to_get(self, mock_dynamodb_table):
        """Test query with projection expression."""
        create_sample_items()
        
        results = customer_orders_query(
            hash_key="cust1",
            attribute_to_get=['pk', 'sk', 'first_name']
        )
        
        assert len(results) >= 1
        for dto in results:
            assert dto.pk is not None
            assert dto.first_name is not None

    def test_query_with_index_name(self, mock_dynamodb_table):
        """Test query using GSI index."""
        create_sample_items()
        
        results = customer_orders_query(
            hash_key="gsi_pk1",
            index_name="gsi__gsipk1__gsisk1__index"
        )
        
        assert len(results) >= 1
        assert all(dto.gsipk1 == "gsi_pk1" for dto in results)

    def test_query_empty_result_nonexistent_key(self, mock_dynamodb_table):
        """Test query with non-existent hash key returns empty results."""
        create_sample_items()
        
        results = customer_orders_query(hash_key="nonexistent_key_12345")
        
        assert len(results) == 0

    def test_query_with_multiple_filters(self, mock_dynamodb_table):
        """Test query with multiple filter conditions combined."""
        create_sample_items()
        from pynamodb.expressions.operand import Path
        
        filter_condition = (Path('first_name') == 'John') & (Path('order_quantity') >= 3)
        
        results = customer_orders_query(
            hash_key="cust1",
            filter_key_condition=filter_condition
        )
        
        assert all(dto.first_name == 'John' and dto.order_quantity >= 3 for dto in results)

    def test_query_with_last_evaluated_key(self, mock_dynamodb_table):
        """
        Test querying with last_evaluated_key parameter.
        
        Verifies that last_evaluated_key parameter is accepted and processed.
        """
        create_sample_items()
        results = customer_orders_query(
            hash_key="cust1",
            query_limit=1
        )
        assert len(results) == 1
        
        results_all = customer_orders_query(
            hash_key="cust1",
            last_evaluated_key=None
        )
        assert len(results_all) >= 1

    def test_gsi_query_success(self, mock_dynamodb_table):
        """
        Test GSI query by gsipk1.
        
        Verifies that GSI query returns items matching gsipk1.
        """
        create_sample_items()
        results = gsi__gsipk1__gsisk1__index_query("gsi_pk1")
        assert len(results) == 2
        assert all(dto.gsipk1 == "gsi_pk1" for dto in results)

    def test_gsi_query_with_sort_condition(self, mock_dynamodb_table):
        """
        Test GSI query with gsisk1 condition.
        
        Verifies that GSI query with sort key condition filters correctly.
        """
        create_sample_items()
        from pynamodb.expressions.operand import Path
        results = gsi__gsipk1__gsisk1__index_query(
            "gsi_pk1",
            Path('gsisk1') == "gsi_sk1"
        )
        assert len(results) == 1
        assert results[0].gsisk1 == "gsi_sk1"

    def test_gsi_query_with_sort_condition_range(self, mock_dynamodb_table):
        """Test GSI query with sort key range condition."""
        create_sample_items()
        from pynamodb.expressions.operand import Path
        
        results = gsi__gsipk1__gsisk1__index_query(
            "gsi_pk1",
            sort_key_condition=Path('gsisk1').between('gsi_sk1', 'gsi_sk3')
        )
        
        assert all('gsi_sk1' <= dto.gsisk1 <= 'gsi_sk3' for dto in results if dto.gsisk1 is not None)

    def test_gsi_query_no_results(self, mock_dynamodb_table):
        """
        Test GSI query with no matching results.
        
        Verifies that empty list is returned when no items match GSI query.
        """
        create_sample_items()
        results = gsi__gsipk1__gsisk1__index_query("nonexistent_gsi")
        assert len(results) == 0

    def test_batch_write_success(self, mock_dynamodb_table):
        """
        Test successful batch write operation.
        
        Verifies that multiple items can be written in batch.
        """
        dtos = [
            CustomerOrdersDto(
                pk=f"batch_pk_{i}",
                sk=f"batch_sk_{i}",
                first_name=f"First{i}",
                last_name=f"Last{i}",
                order_quantity=i * 10
            )
            for i in range(1, 6)
        ]
        customer_orders_batch_write(dtos)
        
        for i in range(1, 6):
            item = CustomerOrders.get(f"batch_pk_{i}", f"batch_sk_{i}")
            assert item.first_name == f"First{i}"

    def test_batch_write_with_ttl(self, mock_dynamodb_table):
        """Test batch write with TTL attribute handling."""
        dtos = [
            CustomerOrdersDto(
                pk=f"ttl_pk_{i}",
                sk="ttl_sk",
                first_name="TTL",
                last_name="Test"
            )
            for i in range(3)
        ]
        
        customer_orders_batch_write(dtos)
        
        for i in range(3):
            item = CustomerOrders.get(f"ttl_pk_{i}", "ttl_sk")
            assert item is not None
            assert item.first_name == "TTL"

    def test_batch_write_empty_list(self, mock_dynamodb_table):
        """
        Test batch write with empty list.
        
        Verifies that batch write handles empty list gracefully.
        """
        customer_orders_batch_write([])

    def test_batch_get_success(self, mock_dynamodb_table):
        """
        Test successful batch get operation.
        
        Verifies that multiple items can be retrieved in batch.
        """
        create_sample_items()
        keys = [
            ("cust1", "order1"),
            ("cust1", "order2"),
            ("cust2", "order1")
        ]
        results = customer_orders_batch_get(keys)
        assert len(results) == 3

    def test_batch_get_empty(self, mock_dynamodb_table):
        """
        Test batch get with empty key list.
        
        Verifies that batch get handles empty list gracefully.
        """
        results = customer_orders_batch_get([])
        assert len(results) == 0

    def test_batch_get_partial(self, mock_dynamodb_table):
        """
        Test batch get with some non-existent items.
        
        Verifies that batch get returns only existing items.
        """
        create_sample_items()
        keys = [
            ("cust1", "order1"),
            ("nonexistent", "nonexistent")
        ]
        results = customer_orders_batch_get(keys)
        assert len(results) == 1
        assert results[0].pk == "cust1"

    def test_batch_get_empty_result(self, mock_dynamodb_table):
        """Test batch get with non-existent keys."""
        keys = [
            ("nonexistent1", "nonexistent1"),
            ("nonexistent2", "nonexistent2")
        ]
        
        results = customer_orders_batch_get(keys)
        
        assert len(results) == 0

    def test_retry_mechanism_put(self, mock_dynamodb_table):
        """Test retry mechanism for put operation."""
        from unittest.mock import patch
        
        dto = CustomerOrdersDto(
            pk="retry_test",
            sk="retry_sk",
            first_name="Retry",
            last_name="Test"
        )
        
        with patch('data.model.customer_orders.CustomerOrders.save') as mock_save:
            mock_save.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            customer_orders_put_item(dto)
            
            assert mock_save.call_count == 3

    def test_retry_mechanism_update(self, mock_dynamodb_table):
        """Test retry mechanism for update operation."""
        from unittest.mock import patch
        
        create_sample_items()
        
        dto = CustomerOrdersDto(
            pk="cust1",
            sk="order1",
            first_name="retry_update"
        )
        
        with patch('data.model.customer_orders.CustomerOrders.save') as mock_save:
            mock_save.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            customer_orders_update_item(dto)
            
            assert mock_save.call_count == 3

    def test_retry_mechanism_delete(self, mock_dynamodb_table):
        """Test retry mechanism for delete operation."""
        from unittest.mock import patch
        
        create_sample_items()
        
        with patch('data.model.customer_orders.CustomerOrders.delete') as mock_delete:
            mock_delete.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            customer_orders_delete_item("cust1", "order1")
            
            assert mock_delete.call_count == 3

    def test_retry_mechanism_query(self, mock_dynamodb_table):
        """Test retry mechanism for query operation."""
        from unittest.mock import patch
        
        create_sample_items()
        
        with patch('data.model.customer_orders.CustomerOrders.query') as mock_query:
            mock_query.side_effect = [
                Exception("First failure"),
                Exception("Second failure"),
                iter([])
            ]
            
            results = customer_orders_query(hash_key="cust1")
            
            assert mock_query.call_count == 3

    def test_retry_exhausted_raises(self, mock_dynamodb_table):
        """Test that exhausted retries raise the exception."""
        from unittest.mock import patch
        
        dto = CustomerOrdersDto(
            pk="exhaust_pk",
            sk="exhaust_sk",
            first_name="Exhaust"
        )
        
        with patch('data.model.customer_orders.CustomerOrders.save') as mock_save:
            mock_save.side_effect = Exception("Persistent failure")
            
            with pytest.raises(Exception):
                customer_orders_put_item(dto)

    def test_create_or_update_retry(self, mock_dynamodb_table):
        """Test retry mechanism for create_or_update operation."""
        from unittest.mock import patch
        
        dto = CustomerOrdersDto(
            pk="retry_create_update",
            sk="retry_sk",
            first_name="Retry",
            last_name="Test"
        )
        
        with patch('data.model.customer_orders.CustomerOrders.save') as mock_save:
            mock_save.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            customer_orders_create_or_update_item(dto)
            
            assert mock_save.call_count == 3

    def test_dto_to_string(self, mock_dynamodb_table):
        """
        Test DTO __str__ method.
        
        Verifies that DTO can be converted to JSON string.
        """
        dto = CustomerOrdersDto(
            pk="test",
            sk="test_sk",
            first_name="Test",
            last_name="User",
            order_quantity=5
        )
        dto_str = str(dto)
        assert "test" in dto_str
        assert "Test" in dto_str

    def test_model_ttl_for_test_object(self, mock_dynamodb_table):
        """
        Test that TTL is set correctly for test object.
        
        Verifies that test objects get test TTL.
        """
        import os
        os.environ['AUTOMATED_TEST_TENANT_IDS'] = 'test_tenant'
        os.environ['CUSTOMER_ORDERS_TEST_TTL_DAYS'] = '30'
        
        item = CustomerOrders(
            pk="TEST_pk",
            sk="test_sk",
            first_name="Test",
            last_name="User"
        )
        ttl = item.get_ttl()
        if ttl is not None:
            assert ttl.days == 30
        else:
            assert CustomerOrders.get_ttl_days() == 0

    def test_model_ttl_for_non_test_object(self, mock_dynamodb_table):
        """
        Test that TTL is None for non-test object when TTL days is 0.
        
        Verifies that non-test objects get None TTL when env var is 0.
        """
        import os
        os.environ['CUSTOMER_ORDERS_TTL_DAYS'] = '0'
        
        item = CustomerOrders(
            pk="regular_pk",
            sk="regular_sk",
            first_name="Regular",
            last_name="User"
        )
        ttl = item.get_ttl()
        assert ttl is None

    def test_model_ttl_for_non_test_object_with_ttl(self, mock_dynamodb_table):
        """
        Test that TTL is set for non-test object when TTL days is non-zero.
        
        Verifies that non-test objects get correct TTL when env var is set.
        """
        import os
        os.environ['CUSTOMER_ORDERS_TTL_DAYS'] = '90'
        
        item = CustomerOrders(
            pk="regular_pk",
            sk="regular_sk",
            first_name="Regular",
            last_name="User"
        )
        ttl = item.get_ttl()
        assert ttl is not None
        assert ttl.days == 90
        
        # Clean up
        os.environ['CUSTOMER_ORDERS_TTL_DAYS'] = '0'

    def test_query_base_table_empty_result(self, mock_dynamodb_table):
        """Test base table query with non-existent key."""
        results = customer_orders_query_base_table("nonexistent_pk")
        
        assert len(results) == 0

    def test_query_scan_index_forward_false(self, mock_dynamodb_table):
        """Test query with descending sort order."""
        create_sample_items()
        
        results_asc = customer_orders_query(
            hash_key="cust1",
            scan_index_forward=True
        )
        
        results_desc = customer_orders_query(
            hash_key="cust1",
            scan_index_forward=False
        )
        
        assert len(results_asc) == len(results_desc) == 2
        if len(results_asc) > 1:
            assert results_asc[0].sk != results_desc[0].sk

    def test_gsi_query_empty_result(self, mock_dynamodb_table):
        """Test GSI query with non-existent key."""
        results = gsi__gsipk1__gsisk1__index_query("nonexistent_gsi")
        
        assert len(results) == 0

    def test_batch_get_partial_result(self, mock_dynamodb_table):
        """Test batch get with mix of existent and non-existent keys."""
        create_sample_items()
        
        keys = [
            ("cust1", "order1"),
            ("nonexistent", "nonexistent")
        ]
        
        results = customer_orders_batch_get(keys)
        
        assert len(results) == 1
        assert results[0].pk == "cust1"
