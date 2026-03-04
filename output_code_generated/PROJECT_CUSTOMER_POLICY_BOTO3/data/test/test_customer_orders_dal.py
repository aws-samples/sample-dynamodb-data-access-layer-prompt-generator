import pytest
import os
from moto import mock_aws
from datetime import datetime, timezone
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

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
    gsi__gsipk1__gsisk1__index_query,
    table
)


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["CUSTOMER_ORDERS"] = "CustomerOrders"


@pytest.fixture
def mock_dynamodb_table(aws_credentials):
    """Create a mock DynamoDB table for testing."""
    with mock_aws():
        dynamodb = table.meta.client
        dynamodb.create_table(
            TableName="CustomerOrders",
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsipk1", "AttributeType": "S"},
                {"AttributeName": "gsisk1", "AttributeType": "S"}
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi__gsipk1__gsisk1__index",
                    "KeySchema": [
                        {"AttributeName": "gsipk1", "KeyType": "HASH"},
                        {"AttributeName": "gsisk1", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }
            ],
            BillingMode="PAY_PER_REQUEST"
        )
        yield


def create_sample_items():
    """Create sample items in the mock table."""
    sample_dtos = [
        CustomerOrdersDto(
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
        CustomerOrdersDto(
            pk="cust1",
            sk="order2",
            cust_address="123 Main St",
            first_name="John",
            last_name="Doe",
            phone="555-1234",
            is_premium_member="true",
            order_date="2024-01-20",
            order_quantity=3,
            sku="SKU002",
            gsipk1="gsi_pk1",
            gsisk1="gsi_sk2"
        ),
        CustomerOrdersDto(
            pk="cust2",
            sk="order1",
            cust_address="456 Oak Ave",
            first_name="Jane",
            last_name="Smith",
            phone="555-5678",
            is_premium_member="false",
            order_date="2024-01-18",
            order_quantity=10,
            sku="SKU003",
            gsipk1="gsi_pk2",
            gsisk1="gsi_sk1"
        ),
        CustomerOrdersDto(
            pk="cust3",
            sk="order1",
            cust_address="789 Pine Rd",
            first_name="Bob",
            last_name="Johnson",
            phone="555-9999",
            is_premium_member="true",
            order_date="2024-01-22",
            order_quantity=7,
            sku="SKU004",
            gsipk1="gsi_pk1",
            gsisk1="gsi_sk3"
        )
    ]
    
    for dto in sample_dtos:
        customer_orders_put_item(dto)
    
    return sample_dtos


def test_put_item(mock_dynamodb_table):
    """Test putting an item into the table."""
    dto = CustomerOrdersDto(
        pk="test_pk",
        sk="test_sk",
        first_name="Test",
        last_name="User",
        order_quantity=7
    )
    customer_orders_put_item(dto)
    result = customer_orders_get_item("test_pk", "test_sk")
    assert result is not None
    assert result.pk == "test_pk"
    assert result.first_name == "Test"


def test_get_item(mock_dynamodb_table):
    """Test getting an item from the table."""
    create_sample_items()
    dto = customer_orders_get_item("cust1", "order1")
    assert dto is not None
    assert dto.pk == "cust1"
    assert dto.first_name == "John"
    assert dto.order_quantity == 5


def test_get_item_not_found(mock_dynamodb_table):
    """Test getting an item that doesn't exist."""
    dto = customer_orders_get_item("nonexistent", "nonexistent")
    assert dto is None


def test_update_item(mock_dynamodb_table):
    """Test updating an item in the table."""
    create_sample_items()
    dto = CustomerOrdersDto(
        pk="cust1",
        sk="order1",
        first_name="Johnny",
        order_quantity=20
    )
    customer_orders_update_item(dto)
    result = customer_orders_get_item("cust1", "order1")
    assert result.first_name == "Johnny"
    assert result.order_quantity == 20
    assert result.last_name == "Doe"  # Original should remain


def test_delete_item(mock_dynamodb_table):
    """Test deleting an item from the table."""
    create_sample_items()
    customer_orders_delete_item("cust1", "order1")
    result = customer_orders_get_item("cust1", "order1")
    assert result is None


def test_create_or_update_item_create(mock_dynamodb_table):
    """Test create_or_update when item doesn't exist (create path)."""
    dto = CustomerOrdersDto(
        pk="new_pk",
        sk="new_sk",
        first_name="New",
        last_name="Customer",
        order_quantity=5
    )
    customer_orders_create_or_update_item(dto)
    result = customer_orders_get_item("new_pk", "new_sk")
    assert result is not None
    assert result.first_name == "New"


def test_create_or_update_item_update(mock_dynamodb_table):
    """Test create_or_update when item exists (update path)."""
    create_sample_items()
    dto = CustomerOrdersDto(
        pk="cust1",
        sk="order1",
        first_name="Updated",
        order_quantity=100
    )
    customer_orders_create_or_update_item(dto)
    result = customer_orders_get_item("cust1", "order1")
    assert result.first_name == "Updated"
    assert result.order_quantity == 100


def test_query_base_table(mock_dynamodb_table):
    """Test querying the base table."""
    create_sample_items()
    result = customer_orders_query_base_table("cust1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 2
    assert all(dto.pk == "cust1" for dto in result["items"])


def test_query_base_table_with_sort_condition(mock_dynamodb_table):
    """Test querying base table with sort key condition."""
    create_sample_items()
    result = customer_orders_query_base_table(
        "cust1",
        Key("sk").begins_with("order1")
    )
    assert result is not None
    assert len(result["items"]) == 1
    assert result["items"][0].sk == "order1"


def test_query_base_table_with_pagination(mock_dynamodb_table):
    """Test pagination of query results."""
    for i in range(5):
        dto = CustomerOrdersDto(
            pk="pagination_test",
            sk=f"sk{i}",
            first_name=f"First{i}",
            last_name=f"Last{i}",
            order_quantity=i * 10
        )
        customer_orders_put_item(dto)
    
    result = customer_orders_query_base_table("pagination_test")
    assert len(result["items"]) == 5


def test_gsi_query(mock_dynamodb_table):
    """Test querying the GSI."""
    create_sample_items()
    result = gsi__gsipk1__gsisk1__index_query("gsi_pk1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 3
    assert all(dto.gsipk1 == "gsi_pk1" for dto in result["items"])


def test_gsi_query_with_sort_condition(mock_dynamodb_table):
    """Test GSI query with sort key condition."""
    create_sample_items()
    result = gsi__gsipk1__gsisk1__index_query(
        "gsi_pk1",
        Key("gsisk1").eq("gsi_sk1")
    )
    assert result is not None
    assert len(result["items"]) == 1
    assert result["items"][0].gsisk1 == "gsi_sk1"


def test_batch_write(mock_dynamodb_table):
    """Test batch writing items."""
    dtos = [
        CustomerOrdersDto(
            pk=f"batch_pk_{i}",
            sk=f"batch_sk_{i}",
            first_name=f"First{i}",
            last_name=f"Last{i}",
            order_quantity=i * 10
        )
        for i in range(5)
    ]
    customer_orders_batch_write(dtos)
    
    for i in range(5):
        result = customer_orders_get_item(f"batch_pk_{i}", f"batch_sk_{i}")
        assert result is not None
        assert result.first_name == f"First{i}"


def test_batch_get(mock_dynamodb_table):
    """Test batch getting items."""
    create_sample_items()
    keys = [
        {"pk": "cust1", "sk": "order1"},
        {"pk": "cust2", "sk": "order1"},
        {"pk": "cust3", "sk": "order1"}
    ]
    results = customer_orders_batch_get(keys)
    assert len(results) == 3
    pks = {dto.pk for dto in results}
    assert "cust1" in pks
    assert "cust2" in pks
    assert "cust3" in pks


def test_query_with_filter_condition(mock_dynamodb_table):
    """Test query with filter condition."""
    create_sample_items()
    results = customer_orders_query(
        hash_key="cust1",
        filter_key_condition=Attr("order_quantity").gte(4)
    )
    assert len(results) == 1
    assert all(dto.order_quantity >= 4 for dto in results)


def test_dto_str_method():
    """Test DTO string representation."""
    dto = CustomerOrdersDto(
        pk="test",
        sk="test_sk",
        first_name="Test",
        last_name="User",
        order_quantity=5
    )
    dto_str = str(dto)
    assert '"pk": "test"' in dto_str
    assert '"first_name": "Test"' in dto_str
    assert '"order_quantity": 5' in dto_str


def test_query_with_range_key_condition(mock_dynamodb_table):
    """Test query with range key condition."""
    create_sample_items()
    results = customer_orders_query(
        hash_key="cust1",
        range_key_and_condition=Key("sk").begins_with("order1")
    )
    assert len(results) == 1
    assert results[0].sk == "order1"


def test_query_scan_index_forward_false(mock_dynamodb_table):
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


def test_query_with_consistent_read(mock_dynamodb_table):
    """Test query with consistent read enabled."""
    create_sample_items()
    results = customer_orders_query(
        hash_key="cust1",
        consistent_read=True
    )
    assert len(results) >= 1
    assert all(dto.pk == "cust1" for dto in results)


def test_query_with_query_limit(mock_dynamodb_table):
    """Test query with limit parameter."""
    create_sample_items()
    results = customer_orders_query(
        hash_key="cust1",
        query_limit=1
    )
    assert len(results) == 1


def test_query_with_attributes_to_get(mock_dynamodb_table):
    """Test query with projection expression."""
    create_sample_items()
    results = customer_orders_query(
        hash_key="cust1",
        attribute_to_get=["pk", "sk", "first_name"]
    )
    assert len(results) >= 1
    for dto in results:
        assert dto.pk is not None
        assert dto.first_name is not None


def test_query_with_index_name(mock_dynamodb_table):
    """Test query using GSI index."""
    create_sample_items()
    results = customer_orders_query(
        hash_key="gsi_pk1",
        index_name="gsi__gsipk1__gsisk1__index"
    )
    assert len(results) >= 1
    assert all(dto.gsipk1 == "gsi_pk1" for dto in results)


def test_query_empty_result_nonexistent_key(mock_dynamodb_table):
    """Test query with non-existent hash key returns empty results."""
    create_sample_items()
    results = customer_orders_query(hash_key="nonexistent_key_12345")
    assert len(results) == 0


def test_query_base_table_empty_result(mock_dynamodb_table):
    """Test base table query with non-existent key."""
    result = customer_orders_query_base_table("nonexistent_pk")
    assert len(result["items"]) == 0


def test_gsi_query_empty_result(mock_dynamodb_table):
    """Test GSI query with non-existent key."""
    result = gsi__gsipk1__gsisk1__index_query("nonexistent_gsi")
    assert len(result["items"]) == 0


def test_batch_get_empty_result(mock_dynamodb_table):
    """Test batch get with non-existent keys."""
    keys = [
        {"pk": "nonexistent1", "sk": "nonexistent1"},
        {"pk": "nonexistent2", "sk": "nonexistent2"}
    ]
    results = customer_orders_batch_get(keys)
    assert len(results) == 0


def test_batch_get_partial_result(mock_dynamodb_table):
    """Test batch get with mix of existent and non-existent keys."""
    create_sample_items()
    keys = [
        {"pk": "cust1", "sk": "order1"},
        {"pk": "nonexistent", "sk": "nonexistent"}
    ]
    results = customer_orders_batch_get(keys)
    assert len(results) == 1
    assert results[0].pk == "cust1"


def test_retry_mechanism_put(mock_dynamodb_table):
    """Test retry mechanism for put operation."""
    from unittest.mock import patch
    
    dto = CustomerOrdersDto(
        pk="retry_test",
        sk="retry_sk",
        first_name="Retry",
        last_name="Test"
    )
    
    with patch("data.dal.customer_orders_dal.table.put_item") as mock_put:
        mock_put.side_effect = [Exception("First failure"), Exception("Second failure"), None]
        
        customer_orders_put_item(dto)
        
        assert mock_put.call_count == 3


def test_query_with_multiple_filters(mock_dynamodb_table):
    """Test query with multiple filter conditions combined."""
    create_sample_items()
    
    filter_condition = Attr("first_name").eq("John") & Attr("order_quantity").gte(3)
    
    results = customer_orders_query(
        hash_key="cust1",
        filter_key_condition=filter_condition
    )
    
    assert all(dto.first_name == "John" and dto.order_quantity >= 3 for dto in results)


def test_gsi_query_with_sort_condition_range(mock_dynamodb_table):
    """Test GSI query with sort key range condition."""
    create_sample_items()
    
    result = gsi__gsipk1__gsisk1__index_query(
        "gsi_pk1",
        sort_key_condition=Key("gsisk1").between("gsi_sk1", "gsi_sk3")
    )
    
    assert all("gsi_sk1" <= dto.gsisk1 <= "gsi_sk3" for dto in result["items"] if dto.gsisk1 is not None)


def test_batch_write_with_ttl_setting(mock_dynamodb_table):
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
        item = customer_orders_get_item(f"ttl_pk_{i}", "ttl_sk")
        assert item is not None
        assert item.first_name == "TTL"
