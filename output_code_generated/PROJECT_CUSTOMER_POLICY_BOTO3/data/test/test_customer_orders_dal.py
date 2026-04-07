import pytest
import os
from moto import mock_aws
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch, MagicMock
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

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
    get_ttl_days,
    get_test_ttl_days,
    is_test_object,
    get_ttl,
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
    """Create a mock DynamoDB table with GSI for testing."""
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
            pk="cust1", sk="order1", cust_address="123 Main St",
            first_name="John", last_name="Doe", phone="555-1234",
            is_premium_member="true", order_date="2024-01-15",
            order_quantity=5, sku="SKU001",
            gsipk1="gsi_pk1", gsisk1="gsi_sk1"
        ),
        CustomerOrdersDto(
            pk="cust1", sk="order2", cust_address="123 Main St",
            first_name="John", last_name="Doe", phone="555-5678",
            is_premium_member="true", order_date="2024-01-20",
            order_quantity=3, sku="SKU002",
            gsipk1="gsi_pk1", gsisk1="gsi_sk2"
        ),
        CustomerOrdersDto(
            pk="cust2", sk="order1", cust_address="456 Oak Ave",
            first_name="Jane", last_name="Smith", phone="555-9999",
            is_premium_member="false", order_date="2024-01-18",
            order_quantity=10, sku="SKU003",
            gsipk1="gsi_pk2", gsisk1="gsi_sk1"
        ),
        CustomerOrdersDto(
            pk="cust3", sk="order1", cust_address="789 Pine Rd",
            first_name="Bob", last_name="Johnson", phone="555-0000",
            is_premium_member="true", order_date="2024-01-22",
            order_quantity=7, sku="SKU004",
            gsipk1="gsi_pk1", gsisk1="gsi_sk3"
        )
    ]
    for dto in sample_dtos:
        customer_orders_put_item(dto)
    return sample_dtos


# ---- Put Item Tests ----

def test_put_item(mock_dynamodb_table):
    """Test putting an item into the table."""
    dto = CustomerOrdersDto(
        pk="test_pk", sk="test_sk", first_name="Test",
        last_name="User", order_quantity=7
    )
    customer_orders_put_item(dto)
    result = customer_orders_get_item("test_pk", "test_sk")
    assert result is not None
    assert result.pk == "test_pk"
    assert result.first_name == "Test"


def test_put_item_sets_version(mock_dynamodb_table):
    """Test that put_item sets version to 1."""
    dto = CustomerOrdersDto(pk="ver_pk", sk="ver_sk", first_name="Version")
    customer_orders_put_item(dto)
    raw = table.get_item(Key={"pk": "ver_pk", "sk": "ver_sk"})["Item"]
    assert raw["version"] == 1


def test_put_item_sets_ttl(mock_dynamodb_table):
    """Test that put_item sets time_to_live attribute."""
    dto = CustomerOrdersDto(pk="ttl_pk", sk="ttl_sk", first_name="TTL")
    customer_orders_put_item(dto)
    raw = table.get_item(Key={"pk": "ttl_pk", "sk": "ttl_sk"})["Item"]
    assert "time_to_live" in raw
    assert raw["time_to_live"] > 0


def test_put_item_sets_created_at(mock_dynamodb_table):
    """Test that put_item sets created_at timestamp."""
    dto = CustomerOrdersDto(pk="ts_pk", sk="ts_sk", first_name="Timestamp")
    customer_orders_put_item(dto)
    raw = table.get_item(Key={"pk": "ts_pk", "sk": "ts_sk"})["Item"]
    assert "created_at" in raw
    assert raw["created_at"].endswith("Z")


# ---- Get Item Tests ----

def test_get_item(mock_dynamodb_table):
    """Test getting an item from the table."""
    create_sample_items()
    dto = customer_orders_get_item("cust1", "order1")
    assert dto is not None
    assert dto.pk == "cust1"
    assert dto.first_name == "John"
    assert dto.order_quantity == 5


def test_get_item_not_found(mock_dynamodb_table):
    """Test getting an item that doesn't exist returns None."""
    dto = customer_orders_get_item("nonexistent", "nonexistent")
    assert dto is None


def test_get_item_nonexistent_pk(mock_dynamodb_table):
    """Test getting item with non-existent partition key."""
    create_sample_items()
    dto = customer_orders_get_item("no_such_pk_999", "order1")
    assert dto is None


def test_get_item_nonexistent_sk(mock_dynamodb_table):
    """Test getting item with valid pk but non-existent sort key."""
    create_sample_items()
    dto = customer_orders_get_item("cust1", "no_such_sk_999")
    assert dto is None


# ---- Update Item Tests ----

def test_update_item(mock_dynamodb_table):
    """Test updating an item in the table with optimistic locking."""
    create_sample_items()
    dto = CustomerOrdersDto(
        pk="cust1", sk="order1", first_name="Johnny", order_quantity=20
    )
    customer_orders_update_item(dto)
    result = customer_orders_get_item("cust1", "order1")
    assert result.first_name == "Johnny"
    assert result.order_quantity == 20
    assert result.last_name == "Doe"  # Original should remain


def test_update_item_increments_version(mock_dynamodb_table):
    """Test that update increments the version attribute."""
    create_sample_items()
    raw_before = table.get_item(Key={"pk": "cust1", "sk": "order1"})["Item"]
    version_before = raw_before["version"]
    dto = CustomerOrdersDto(pk="cust1", sk="order1", first_name="Updated")
    customer_orders_update_item(dto)
    raw_after = table.get_item(Key={"pk": "cust1", "sk": "order1"})["Item"]
    assert raw_after["version"] == version_before + 1


def test_update_item_sets_updated_at(mock_dynamodb_table):
    """Test that update sets updated_at timestamp."""
    create_sample_items()
    dto = CustomerOrdersDto(pk="cust1", sk="order1", first_name="TS")
    customer_orders_update_item(dto)
    raw = table.get_item(Key={"pk": "cust1", "sk": "order1"})["Item"]
    assert "updated_at" in raw
    assert raw["updated_at"].endswith("Z")


def test_update_nonexistent_item_raises(mock_dynamodb_table):
    """Test that updating a non-existent item raises an exception."""
    dto = CustomerOrdersDto(pk="no_pk", sk="no_sk", first_name="Ghost")
    with pytest.raises(Exception):
        customer_orders_update_item(dto)


# ---- Delete Item Tests ----

def test_delete_item(mock_dynamodb_table):
    """Test deleting an item from the table."""
    create_sample_items()
    customer_orders_delete_item("cust1", "order1")
    result = customer_orders_get_item("cust1", "order1")
    assert result is None


def test_delete_nonexistent_item(mock_dynamodb_table):
    """Test deleting a non-existent item does not raise."""
    customer_orders_delete_item("nonexistent", "nonexistent")


# ---- Create or Update Tests ----

def test_create_or_update_item_create(mock_dynamodb_table):
    """Test create_or_update when item doesn't exist (create path)."""
    dto = CustomerOrdersDto(
        pk="new_pk", sk="new_sk", first_name="New",
        last_name="Customer", order_quantity=5
    )
    customer_orders_create_or_update_item(dto)
    result = customer_orders_get_item("new_pk", "new_sk")
    assert result is not None
    assert result.first_name == "New"


def test_create_or_update_item_update(mock_dynamodb_table):
    """Test create_or_update when item exists (update path)."""
    create_sample_items()
    dto = CustomerOrdersDto(
        pk="cust1", sk="order1", first_name="Updated", order_quantity=100
    )
    customer_orders_create_or_update_item(dto)
    result = customer_orders_get_item("cust1", "order1")
    assert result.first_name == "Updated"
    assert result.order_quantity == 100


# ---- Base Table Query Tests ----

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
        "cust1", Key("sk").begins_with("order1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].sk == "order1"


def test_query_base_table_with_filter_condition(mock_dynamodb_table):
    """Test base table query with filter condition."""
    create_sample_items()
    result = customer_orders_query_base_table(
        "cust1", filter_key_condition=Attr("order_quantity").gte(4)
    )
    assert all(dto.order_quantity >= 4 for dto in result["items"])


def test_query_base_table_with_pagination(mock_dynamodb_table):
    """Test pagination of query results."""
    for i in range(5):
        dto = CustomerOrdersDto(
            pk="pagination_test", sk=f"sk{i}",
            first_name=f"First{i}", last_name=f"Last{i}",
            order_quantity=i * 10
        )
        customer_orders_put_item(dto)
    result = customer_orders_query_base_table("pagination_test")
    assert len(result["items"]) == 5


def test_query_base_table_empty_result(mock_dynamodb_table):
    """Test base table query with non-existent key."""
    result = customer_orders_query_base_table("nonexistent_pk")
    assert len(result["items"]) == 0


# ---- Generic Query Tests ----

def test_query_with_filter_condition(mock_dynamodb_table):
    """Test query with filter condition."""
    create_sample_items()
    results = customer_orders_query(
        hash_key="cust1",
        filter_key_condition=Attr("order_quantity").gte(4)
    )
    assert len(results) == 1
    assert all(dto.order_quantity >= 4 for dto in results)


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
    results_asc = customer_orders_query(hash_key="cust1", scan_index_forward=True)
    results_desc = customer_orders_query(hash_key="cust1", scan_index_forward=False)
    assert len(results_asc) == len(results_desc) == 2
    if len(results_asc) > 1:
        assert results_asc[0].sk != results_desc[0].sk


def test_query_with_consistent_read(mock_dynamodb_table):
    """Test query with consistent read enabled."""
    create_sample_items()
    results = customer_orders_query(hash_key="cust1", consistent_read=True)
    assert len(results) >= 1
    assert all(dto.pk == "cust1" for dto in results)


def test_query_with_query_limit(mock_dynamodb_table):
    """Test query with limit parameter."""
    create_sample_items()
    results = customer_orders_query(hash_key="cust1", query_limit=1)
    assert len(results) == 1


def test_query_with_attributes_to_get(mock_dynamodb_table):
    """Test query with projection expression."""
    create_sample_items()
    results = customer_orders_query(
        hash_key="cust1", attribute_to_get=["pk", "sk", "first_name"]
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


def test_query_with_multiple_filters(mock_dynamodb_table):
    """Test query with multiple filter conditions combined."""
    create_sample_items()
    filter_condition = Attr("first_name").eq("John") & Attr("order_quantity").gte(3)
    results = customer_orders_query(
        hash_key="cust1", filter_key_condition=filter_condition
    )
    assert all(dto.first_name == "John" and dto.order_quantity >= 3 for dto in results)


def test_query_with_last_evaluated_key(mock_dynamodb_table):
    """Test query pagination using last_evaluated_key."""
    for i in range(10):
        dto = CustomerOrdersDto(
            pk="pagination_key", sk=f"sk{i:02d}",
            first_name=f"First{i}", order_quantity=100 * i
        )
        customer_orders_put_item(dto)
    result = customer_orders_query_base_table("pagination_key")
    assert len(result["items"]) == 10


# ---- GSI Query Tests ----

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
        "gsi_pk1", Key("gsisk1").eq("gsi_sk1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].gsisk1 == "gsi_sk1"


def test_gsi_query_with_filter_condition(mock_dynamodb_table):
    """Test GSI query with filter condition."""
    create_sample_items()
    result = gsi__gsipk1__gsisk1__index_query(
        "gsi_pk1", filter_key_condition=Attr("order_quantity").lte(5)
    )
    assert all(dto.order_quantity <= 5 for dto in result["items"])


def test_gsi_query_with_sort_condition_range(mock_dynamodb_table):
    """Test GSI query with sort key range condition."""
    create_sample_items()
    result = gsi__gsipk1__gsisk1__index_query(
        "gsi_pk1",
        sort_key_condition=Key("gsisk1").between("gsi_sk1", "gsi_sk3")
    )
    assert all(
        "gsi_sk1" <= dto.gsisk1 <= "gsi_sk3"
        for dto in result["items"] if dto.gsisk1 is not None
    )


def test_gsi_query_empty_result(mock_dynamodb_table):
    """Test GSI query with non-existent key."""
    result = gsi__gsipk1__gsisk1__index_query("nonexistent_gsi")
    assert len(result["items"]) == 0


# ---- Batch Write Tests ----

def test_batch_write(mock_dynamodb_table):
    """Test batch writing items."""
    dtos = [
        CustomerOrdersDto(
            pk=f"batch_pk_{i}", sk=f"batch_sk_{i}",
            first_name=f"First{i}", last_name=f"Last{i}",
            order_quantity=i * 10
        )
        for i in range(5)
    ]
    customer_orders_batch_write(dtos)
    for i in range(5):
        result = customer_orders_get_item(f"batch_pk_{i}", f"batch_sk_{i}")
        assert result is not None
        assert result.first_name == f"First{i}"


def test_batch_write_with_ttl(mock_dynamodb_table):
    """Test batch write sets TTL attribute."""
    dtos = [
        CustomerOrdersDto(
            pk=f"ttl_pk_{i}", sk="ttl_sk",
            first_name="TTL", last_name="Test"
        )
        for i in range(3)
    ]
    customer_orders_batch_write(dtos)
    for i in range(3):
        item = customer_orders_get_item(f"ttl_pk_{i}", "ttl_sk")
        assert item is not None
        assert item.first_name == "TTL"


# ---- Batch Get Tests ----

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


# ---- Retry Mechanism Tests ----

def test_retry_mechanism_put(mock_dynamodb_table):
    """Test tenacity retry mechanism for put operation."""
    dto = CustomerOrdersDto(
        pk="retry_test", sk="retry_sk",
        first_name="Retry", last_name="Test"
    )
    with patch("data.dal.customer_orders_dal.table.put_item") as mock_put:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "PutItem"
        )
        mock_put.side_effect = [error, error, None]
        customer_orders_put_item(dto)
        assert mock_put.call_count == 3


def test_retry_mechanism_get(mock_dynamodb_table):
    """Test tenacity retry mechanism for get operation."""
    with patch("data.dal.customer_orders_dal.table.get_item") as mock_get:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "GetItem"
        )
        mock_get.side_effect = [error, error, {"Item": {"pk": "x", "sk": "y"}}]
        result = customer_orders_get_item("x", "y")
        assert mock_get.call_count == 3
        assert result is not None


def test_retry_mechanism_delete(mock_dynamodb_table):
    """Test tenacity retry mechanism for delete operation."""
    create_sample_items()
    with patch("data.dal.customer_orders_dal.table.delete_item") as mock_del:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "DeleteItem"
        )
        mock_del.side_effect = [error, error, None]
        customer_orders_delete_item("cust1", "order1")
        assert mock_del.call_count == 3


def test_retry_mechanism_query(mock_dynamodb_table):
    """Test tenacity retry mechanism for query operation."""
    create_sample_items()
    with patch("data.dal.customer_orders_dal.table.query") as mock_query:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "Query"
        )
        mock_query.side_effect = [
            error, error,
            {"Items": [], "Count": 0, "ScannedCount": 0}
        ]
        results = customer_orders_query(hash_key="cust1")
        assert mock_query.call_count == 3


def test_retry_exhausted_raises(mock_dynamodb_table):
    """Test that exhausting retries raises the exception."""
    dto = CustomerOrdersDto(pk="fail_pk", sk="fail_sk", first_name="Fail")
    with patch("data.dal.customer_orders_dal.table.put_item") as mock_put:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "PutItem"
        )
        mock_put.side_effect = error
        with pytest.raises(ClientError):
            customer_orders_put_item(dto)


# ---- DTO Tests ----

def test_dto_str_method():
    """Test DTO string representation."""
    dto = CustomerOrdersDto(
        pk="test", sk="test_sk", first_name="Test",
        last_name="User", order_quantity=5
    )
    dto_str = str(dto)
    assert '"pk": "test"' in dto_str
    assert '"first_name": "Test"' in dto_str
    assert '"order_quantity": 5' in dto_str


# ---- TTL Helper Tests ----

def test_get_ttl_days_default():
    """Test get_ttl_days returns default 30."""
    os.environ.pop("CUSTOMER_ORDERS_TTL_DAYS", None)
    os.environ["CUSTOMER_ORDERS_TTL_DAYS"] = "30"
    assert get_ttl_days() == 30


def test_get_ttl_days_custom():
    """Test get_ttl_days returns custom value."""
    os.environ["CUSTOMER_ORDERS_TTL_DAYS"] = "60"
    assert get_ttl_days() == 60
    os.environ["CUSTOMER_ORDERS_TTL_DAYS"] = "30"


def test_get_test_ttl_days_default():
    """Test get_test_ttl_days returns default 30."""
    os.environ["CUSTOMER_ORDERS_TEST_TTL_DAYS"] = "30"
    assert get_test_ttl_days() == 30


def test_is_test_object_true():
    """Test is_test_object returns True for TEST keys."""
    assert is_test_object("TEST_pk_123") is True
    assert is_test_object("my_test_key") is True


def test_is_test_object_false():
    """Test is_test_object returns False for non-TEST keys."""
    assert is_test_object("production_pk") is False
    assert is_test_object("") is False
    assert is_test_object(None) is False


def test_get_ttl_test_object():
    """Test get_ttl uses test TTL for TEST partition keys."""
    os.environ["CUSTOMER_ORDERS_TEST_TTL_DAYS"] = "7"
    ttl = get_ttl("TEST_pk")
    assert ttl is not None
    assert ttl > 0
    os.environ["CUSTOMER_ORDERS_TEST_TTL_DAYS"] = "30"


def test_get_ttl_normal_object():
    """Test get_ttl uses normal TTL for non-TEST partition keys."""
    os.environ["CUSTOMER_ORDERS_TTL_DAYS"] = "90"
    ttl = get_ttl("normal_pk")
    assert ttl is not None
    assert ttl > 0
    os.environ["CUSTOMER_ORDERS_TTL_DAYS"] = "30"
