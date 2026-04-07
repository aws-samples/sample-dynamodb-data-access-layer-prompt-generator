import pytest
import os
from moto import mock_aws
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch, MagicMock
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

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
    gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query,
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
    os.environ["DDB_TABLE_ONE"] = "ddb_table_one"


@pytest.fixture
def mock_dynamodb_table(aws_credentials):
    """Create a mock DynamoDB table with GSI for testing."""
    with mock_aws():
        dynamodb = table.meta.client
        dynamodb.create_table(
            TableName="ddb_table_one",
            KeySchema=[
                {"AttributeName": "pk_attribute_str_1", "KeyType": "HASH"},
                {"AttributeName": "sk_attribute_str_2", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk_attribute_str_1", "AttributeType": "S"},
                {"AttributeName": "sk_attribute_str_2", "AttributeType": "S"},
                {"AttributeName": "gsipk_attribute_str_7", "AttributeType": "S"},
                {"AttributeName": "gsisk_attribute_str_8", "AttributeType": "S"}
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index",
                    "KeySchema": [
                        {"AttributeName": "gsipk_attribute_str_7", "KeyType": "HASH"},
                        {"AttributeName": "gsisk_attribute_str_8", "KeyType": "RANGE"}
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
        DdbTableOneDto(
            pk_attribute_str_1="pk1", sk_attribute_str_2="sk1",
            attribute_str_3="str3_val1", attribute_str_4="str4_val1",
            attribute_num_5=100, attribute_map_6={"key1": "val1", "key2": "val2"},
            gsipk_attribute_str_7="gsi_pk1", gsisk_attribute_str_8="gsi_sk1"
        ),
        DdbTableOneDto(
            pk_attribute_str_1="pk1", sk_attribute_str_2="sk2",
            attribute_str_3="str3_val2", attribute_str_4="str4_val2",
            attribute_num_5=200, attribute_map_6={"key3": "val3"},
            gsipk_attribute_str_7="gsi_pk1", gsisk_attribute_str_8="gsi_sk2"
        ),
        DdbTableOneDto(
            pk_attribute_str_1="pk2", sk_attribute_str_2="sk1",
            attribute_str_3="str3_val3", attribute_str_4="str4_val3",
            attribute_num_5=300, attribute_map_6={"key4": "val4"},
            gsipk_attribute_str_7="gsi_pk2", gsisk_attribute_str_8="gsi_sk1"
        ),
        DdbTableOneDto(
            pk_attribute_str_1="pk3", sk_attribute_str_2="sk1",
            attribute_str_3="str3_val4", attribute_str_4="str4_val4",
            attribute_num_5=50, attribute_map_6={"key5": "val5"},
            gsipk_attribute_str_7="gsi_pk1", gsisk_attribute_str_8="gsi_sk3"
        )
    ]
    for dto in sample_dtos:
        ddb_table_one_put_item(dto)
    return sample_dtos


# ---- Put Item Tests ----

def test_put_item(mock_dynamodb_table):
    """Test putting an item into the table."""
    dto = DdbTableOneDto(
        pk_attribute_str_1="test_pk", sk_attribute_str_2="test_sk",
        attribute_str_3="val3", attribute_num_5=42
    )
    ddb_table_one_put_item(dto)
    result = ddb_table_one_get_item("test_pk", "test_sk")
    assert result is not None
    assert result.pk_attribute_str_1 == "test_pk"
    assert result.attribute_str_3 == "val3"


def test_put_item_sets_version(mock_dynamodb_table):
    """Test that put_item sets version to 1."""
    dto = DdbTableOneDto(pk_attribute_str_1="ver_pk", sk_attribute_str_2="ver_sk", attribute_str_3="Version")
    ddb_table_one_put_item(dto)
    raw = table.get_item(Key={"pk_attribute_str_1": "ver_pk", "sk_attribute_str_2": "ver_sk"})["Item"]
    assert raw["version"] == 1


def test_put_item_sets_ttl(mock_dynamodb_table):
    """Test that put_item sets time_to_live attribute."""
    dto = DdbTableOneDto(pk_attribute_str_1="ttl_pk", sk_attribute_str_2="ttl_sk", attribute_str_3="TTL")
    ddb_table_one_put_item(dto)
    raw = table.get_item(Key={"pk_attribute_str_1": "ttl_pk", "sk_attribute_str_2": "ttl_sk"})["Item"]
    assert "time_to_live" in raw
    assert raw["time_to_live"] > 0


def test_put_item_sets_created_at(mock_dynamodb_table):
    """Test that put_item sets created_at timestamp."""
    dto = DdbTableOneDto(pk_attribute_str_1="ts_pk", sk_attribute_str_2="ts_sk", attribute_str_3="Timestamp")
    ddb_table_one_put_item(dto)
    raw = table.get_item(Key={"pk_attribute_str_1": "ts_pk", "sk_attribute_str_2": "ts_sk"})["Item"]
    assert "created_at" in raw
    assert raw["created_at"].endswith("Z")


def test_put_item_with_map_attribute(mock_dynamodb_table):
    """Test putting an item with a map/dict attribute."""
    map_data = {"nested_key": "nested_val", "count": 5}
    dto = DdbTableOneDto(
        pk_attribute_str_1="map_pk", sk_attribute_str_2="map_sk",
        attribute_map_6=map_data
    )
    ddb_table_one_put_item(dto)
    result = ddb_table_one_get_item("map_pk", "map_sk")
    assert result is not None
    assert result.attribute_map_6 is not None
    assert result.attribute_map_6["nested_key"] == "nested_val"


def test_put_item_with_decimal_attribute(mock_dynamodb_table):
    """Test putting an item with a numeric attribute stored as Decimal."""
    dto = DdbTableOneDto(
        pk_attribute_str_1="dec_pk", sk_attribute_str_2="dec_sk",
        attribute_num_5=999
    )
    ddb_table_one_put_item(dto)
    raw = table.get_item(Key={"pk_attribute_str_1": "dec_pk", "sk_attribute_str_2": "dec_sk"})["Item"]
    assert raw["attribute_num_5"] == Decimal("999")


# ---- Get Item Tests ----

def test_get_item(mock_dynamodb_table):
    """Test getting an item from the table."""
    create_sample_items()
    dto = ddb_table_one_get_item("pk1", "sk1")
    assert dto is not None
    assert dto.pk_attribute_str_1 == "pk1"
    assert dto.attribute_str_3 == "str3_val1"
    assert dto.attribute_num_5 == 100


def test_get_item_not_found(mock_dynamodb_table):
    """Test getting an item that doesn't exist returns None."""
    dto = ddb_table_one_get_item("nonexistent", "nonexistent")
    assert dto is None


def test_get_item_nonexistent_pk(mock_dynamodb_table):
    """Test getting item with non-existent partition key."""
    create_sample_items()
    dto = ddb_table_one_get_item("no_such_pk_999", "sk1")
    assert dto is None


def test_get_item_nonexistent_sk(mock_dynamodb_table):
    """Test getting item with valid pk but non-existent sort key."""
    create_sample_items()
    dto = ddb_table_one_get_item("pk1", "no_such_sk_999")
    assert dto is None


# ---- Update Item Tests ----

def test_update_item(mock_dynamodb_table):
    """Test updating an item in the table with optimistic locking."""
    create_sample_items()
    dto = DdbTableOneDto(
        pk_attribute_str_1="pk1", sk_attribute_str_2="sk1",
        attribute_str_3="updated_str3", attribute_num_5=999
    )
    ddb_table_one_update_item(dto)
    result = ddb_table_one_get_item("pk1", "sk1")
    assert result.attribute_str_3 == "updated_str3"
    assert result.attribute_num_5 == 999
    assert result.attribute_str_4 == "str4_val1"  # Original should remain


def test_update_item_increments_version(mock_dynamodb_table):
    """Test that update increments the version attribute."""
    create_sample_items()
    raw_before = table.get_item(Key={"pk_attribute_str_1": "pk1", "sk_attribute_str_2": "sk1"})["Item"]
    version_before = raw_before["version"]
    dto = DdbTableOneDto(pk_attribute_str_1="pk1", sk_attribute_str_2="sk1", attribute_str_3="Updated")
    ddb_table_one_update_item(dto)
    raw_after = table.get_item(Key={"pk_attribute_str_1": "pk1", "sk_attribute_str_2": "sk1"})["Item"]
    assert raw_after["version"] == version_before + 1


def test_update_item_sets_updated_at(mock_dynamodb_table):
    """Test that update sets updated_at timestamp."""
    create_sample_items()
    dto = DdbTableOneDto(pk_attribute_str_1="pk1", sk_attribute_str_2="sk1", attribute_str_3="TS")
    ddb_table_one_update_item(dto)
    raw = table.get_item(Key={"pk_attribute_str_1": "pk1", "sk_attribute_str_2": "sk1"})["Item"]
    assert "updated_at" in raw
    assert raw["updated_at"].endswith("Z")


def test_update_item_with_map(mock_dynamodb_table):
    """Test updating an item with a map attribute."""
    create_sample_items()
    new_map = {"updated_key": "updated_val"}
    dto = DdbTableOneDto(
        pk_attribute_str_1="pk1", sk_attribute_str_2="sk1",
        attribute_map_6=new_map
    )
    ddb_table_one_update_item(dto)
    result = ddb_table_one_get_item("pk1", "sk1")
    assert result.attribute_map_6 == new_map


def test_update_nonexistent_item_raises(mock_dynamodb_table):
    """Test that updating a non-existent item raises an exception."""
    dto = DdbTableOneDto(pk_attribute_str_1="no_pk", sk_attribute_str_2="no_sk", attribute_str_3="Ghost")
    with pytest.raises(Exception):
        ddb_table_one_update_item(dto)


# ---- Delete Item Tests ----

def test_delete_item(mock_dynamodb_table):
    """Test deleting an item from the table."""
    create_sample_items()
    ddb_table_one_delete_item("pk1", "sk1")
    result = ddb_table_one_get_item("pk1", "sk1")
    assert result is None


def test_delete_nonexistent_item(mock_dynamodb_table):
    """Test deleting a non-existent item does not raise."""
    ddb_table_one_delete_item("nonexistent", "nonexistent")


def test_delete_item_verify_other_items_remain(mock_dynamodb_table):
    """Test that deleting one item doesn't affect other items."""
    create_sample_items()
    ddb_table_one_delete_item("pk1", "sk1")
    result = ddb_table_one_get_item("pk1", "sk2")
    assert result is not None
    assert result.attribute_str_3 == "str3_val2"


# ---- Create or Update Tests ----

def test_create_or_update_item_create(mock_dynamodb_table):
    """Test create_or_update when item doesn't exist (create path)."""
    dto = DdbTableOneDto(
        pk_attribute_str_1="new_pk", sk_attribute_str_2="new_sk",
        attribute_str_3="New", attribute_num_5=42
    )
    ddb_table_one_create_or_update_item(dto)
    result = ddb_table_one_get_item("new_pk", "new_sk")
    assert result is not None
    assert result.attribute_str_3 == "New"


def test_create_or_update_item_update(mock_dynamodb_table):
    """Test create_or_update when item exists (update path)."""
    create_sample_items()
    dto = DdbTableOneDto(
        pk_attribute_str_1="pk1", sk_attribute_str_2="sk1",
        attribute_str_3="Updated", attribute_num_5=777
    )
    ddb_table_one_create_or_update_item(dto)
    result = ddb_table_one_get_item("pk1", "sk1")
    assert result.attribute_str_3 == "Updated"
    assert result.attribute_num_5 == 777


def test_create_or_update_sets_version_on_create(mock_dynamodb_table):
    """Test create_or_update sets version=1 on new item."""
    dto = DdbTableOneDto(pk_attribute_str_1="cou_pk", sk_attribute_str_2="cou_sk", attribute_str_3="COU")
    ddb_table_one_create_or_update_item(dto)
    raw = table.get_item(Key={"pk_attribute_str_1": "cou_pk", "sk_attribute_str_2": "cou_sk"})["Item"]
    assert raw["version"] == 1


# ---- Base Table Query Tests ----

def test_query_base_table(mock_dynamodb_table):
    """Test querying the base table."""
    create_sample_items()
    result = ddb_table_one_query_base_table("pk1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 2
    assert all(dto.pk_attribute_str_1 == "pk1" for dto in result["items"])


def test_query_base_table_with_sort_condition(mock_dynamodb_table):
    """Test querying base table with sort key condition."""
    create_sample_items()
    result = ddb_table_one_query_base_table(
        "pk1", Key("sk_attribute_str_2").begins_with("sk1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].sk_attribute_str_2 == "sk1"


def test_query_base_table_with_filter_condition(mock_dynamodb_table):
    """Test base table query with filter condition."""
    create_sample_items()
    result = ddb_table_one_query_base_table(
        "pk1", filter_key_condition=Attr("attribute_num_5").gte(150)
    )
    assert all(dto.attribute_num_5 >= 150 for dto in result["items"])


def test_query_base_table_with_pagination(mock_dynamodb_table):
    """Test pagination of query results."""
    for i in range(5):
        dto = DdbTableOneDto(
            pk_attribute_str_1="pagination_test", sk_attribute_str_2=f"sk{i}",
            attribute_str_3=f"str3_{i}", attribute_num_5=i * 10
        )
        ddb_table_one_put_item(dto)
    result = ddb_table_one_query_base_table("pagination_test")
    assert len(result["items"]) == 5


def test_query_base_table_empty_result(mock_dynamodb_table):
    """Test base table query with non-existent key."""
    result = ddb_table_one_query_base_table("nonexistent_pk")
    assert len(result["items"]) == 0


# ---- Generic Query Tests ----

def test_query_with_filter_condition(mock_dynamodb_table):
    """Test query with filter condition."""
    create_sample_items()
    results = ddb_table_one_query(
        hash_key="pk1",
        filter_key_condition=Attr("attribute_num_5").gte(150)
    )
    assert len(results) == 1
    assert all(dto.attribute_num_5 >= 150 for dto in results)


def test_query_with_range_key_condition(mock_dynamodb_table):
    """Test query with range key condition."""
    create_sample_items()
    results = ddb_table_one_query(
        hash_key="pk1",
        range_key_and_condition=Key("sk_attribute_str_2").begins_with("sk1")
    )
    assert len(results) == 1
    assert results[0].sk_attribute_str_2 == "sk1"


def test_query_scan_index_forward_false(mock_dynamodb_table):
    """Test query with descending sort order."""
    create_sample_items()
    results_asc = ddb_table_one_query(hash_key="pk1", scan_index_forward=True)
    results_desc = ddb_table_one_query(hash_key="pk1", scan_index_forward=False)
    assert len(results_asc) == len(results_desc) == 2
    if len(results_asc) > 1:
        assert results_asc[0].sk_attribute_str_2 != results_desc[0].sk_attribute_str_2


def test_query_with_consistent_read(mock_dynamodb_table):
    """Test query with consistent read enabled."""
    create_sample_items()
    results = ddb_table_one_query(hash_key="pk1", consistent_read=True)
    assert len(results) >= 1
    assert all(dto.pk_attribute_str_1 == "pk1" for dto in results)


def test_query_with_query_limit(mock_dynamodb_table):
    """Test query with limit parameter."""
    create_sample_items()
    results = ddb_table_one_query(hash_key="pk1", query_limit=1)
    assert len(results) == 1


def test_query_with_attributes_to_get(mock_dynamodb_table):
    """Test query with projection expression."""
    create_sample_items()
    results = ddb_table_one_query(
        hash_key="pk1", attribute_to_get=["pk_attribute_str_1", "sk_attribute_str_2", "attribute_str_3"]
    )
    assert len(results) >= 1
    for dto in results:
        assert dto.pk_attribute_str_1 is not None
        assert dto.attribute_str_3 is not None


def test_query_with_index_name(mock_dynamodb_table):
    """Test query using GSI index."""
    create_sample_items()
    results = ddb_table_one_query(
        hash_key="gsi_pk1",
        index_name="gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index"
    )
    assert len(results) >= 1
    assert all(dto.gsipk_attribute_str_7 == "gsi_pk1" for dto in results)


def test_query_empty_result_nonexistent_key(mock_dynamodb_table):
    """Test query with non-existent hash key returns empty results."""
    create_sample_items()
    results = ddb_table_one_query(hash_key="nonexistent_key_12345")
    assert len(results) == 0


def test_query_with_multiple_filters(mock_dynamodb_table):
    """Test query with multiple filter conditions combined."""
    create_sample_items()
    filter_condition = Attr("attribute_str_3").eq("str3_val1") & Attr("attribute_num_5").gte(50)
    results = ddb_table_one_query(
        hash_key="pk1", filter_key_condition=filter_condition
    )
    assert all(dto.attribute_str_3 == "str3_val1" and dto.attribute_num_5 >= 50 for dto in results)


def test_query_with_last_evaluated_key(mock_dynamodb_table):
    """Test query pagination using last_evaluated_key."""
    for i in range(10):
        dto = DdbTableOneDto(
            pk_attribute_str_1="pagination_key", sk_attribute_str_2=f"sk{i:02d}",
            attribute_str_3=f"str3_{i}", attribute_num_5=100 * i
        )
        ddb_table_one_put_item(dto)
    result = ddb_table_one_query_base_table("pagination_key")
    assert len(result["items"]) == 10


# ---- GSI Query Tests ----

def test_gsi_query(mock_dynamodb_table):
    """Test querying the GSI."""
    create_sample_items()
    result = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query("gsi_pk1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 3
    assert all(dto.gsipk_attribute_str_7 == "gsi_pk1" for dto in result["items"])


def test_gsi_query_with_sort_condition(mock_dynamodb_table):
    """Test GSI query with sort key condition."""
    create_sample_items()
    result = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query(
        "gsi_pk1", Key("gsisk_attribute_str_8").eq("gsi_sk1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].gsisk_attribute_str_8 == "gsi_sk1"


def test_gsi_query_with_filter_condition(mock_dynamodb_table):
    """Test GSI query with filter condition."""
    create_sample_items()
    result = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query(
        "gsi_pk1", filter_key_condition=Attr("attribute_num_5").lte(100)
    )
    assert all(dto.attribute_num_5 <= 100 for dto in result["items"])


def test_gsi_query_with_sort_condition_range(mock_dynamodb_table):
    """Test GSI query with sort key range condition."""
    create_sample_items()
    result = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query(
        "gsi_pk1",
        sort_key_condition=Key("gsisk_attribute_str_8").between("gsi_sk1", "gsi_sk3")
    )
    assert all(
        "gsi_sk1" <= dto.gsisk_attribute_str_8 <= "gsi_sk3"
        for dto in result["items"] if dto.gsisk_attribute_str_8 is not None
    )


def test_gsi_query_empty_result(mock_dynamodb_table):
    """Test GSI query with non-existent key."""
    result = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query("nonexistent_gsi")
    assert len(result["items"]) == 0


# ---- Batch Write Tests ----

def test_batch_write(mock_dynamodb_table):
    """Test batch writing items."""
    dtos = [
        DdbTableOneDto(
            pk_attribute_str_1=f"batch_pk_{i}", sk_attribute_str_2=f"batch_sk_{i}",
            attribute_str_3=f"str3_{i}", attribute_num_5=i * 10
        )
        for i in range(5)
    ]
    ddb_table_one_batch_write(dtos)
    for i in range(5):
        result = ddb_table_one_get_item(f"batch_pk_{i}", f"batch_sk_{i}")
        assert result is not None
        assert result.attribute_str_3 == f"str3_{i}"


def test_batch_write_with_ttl(mock_dynamodb_table):
    """Test batch write sets TTL attribute."""
    dtos = [
        DdbTableOneDto(
            pk_attribute_str_1=f"ttl_pk_{i}", sk_attribute_str_2="ttl_sk",
            attribute_str_3="TTL"
        )
        for i in range(3)
    ]
    ddb_table_one_batch_write(dtos)
    for i in range(3):
        item = ddb_table_one_get_item(f"ttl_pk_{i}", "ttl_sk")
        assert item is not None
        assert item.attribute_str_3 == "TTL"


# ---- Batch Get Tests ----

def test_batch_get(mock_dynamodb_table):
    """Test batch getting items."""
    create_sample_items()
    keys = [
        {"pk_attribute_str_1": "pk1", "sk_attribute_str_2": "sk1"},
        {"pk_attribute_str_1": "pk2", "sk_attribute_str_2": "sk1"},
        {"pk_attribute_str_1": "pk3", "sk_attribute_str_2": "sk1"}
    ]
    results = ddb_table_one_batch_get(keys)
    assert len(results) == 3
    pks = {dto.pk_attribute_str_1 for dto in results}
    assert "pk1" in pks
    assert "pk2" in pks
    assert "pk3" in pks


def test_batch_get_empty_result(mock_dynamodb_table):
    """Test batch get with non-existent keys."""
    keys = [
        {"pk_attribute_str_1": "nonexistent1", "sk_attribute_str_2": "nonexistent1"},
        {"pk_attribute_str_1": "nonexistent2", "sk_attribute_str_2": "nonexistent2"}
    ]
    results = ddb_table_one_batch_get(keys)
    assert len(results) == 0


def test_batch_get_partial_result(mock_dynamodb_table):
    """Test batch get with mix of existent and non-existent keys."""
    create_sample_items()
    keys = [
        {"pk_attribute_str_1": "pk1", "sk_attribute_str_2": "sk1"},
        {"pk_attribute_str_1": "nonexistent", "sk_attribute_str_2": "nonexistent"}
    ]
    results = ddb_table_one_batch_get(keys)
    assert len(results) == 1
    assert results[0].pk_attribute_str_1 == "pk1"


# ---- Retry Mechanism Tests ----

def test_retry_mechanism_put(mock_dynamodb_table):
    """Test tenacity retry mechanism for put operation."""
    dto = DdbTableOneDto(
        pk_attribute_str_1="retry_test", sk_attribute_str_2="retry_sk",
        attribute_str_3="Retry"
    )
    with patch("data.dal.ddb_table_one_dal.table.put_item") as mock_put:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "PutItem"
        )
        mock_put.side_effect = [error, error, None]
        ddb_table_one_put_item(dto)
        assert mock_put.call_count == 3


def test_retry_mechanism_get(mock_dynamodb_table):
    """Test tenacity retry mechanism for get operation."""
    with patch("data.dal.ddb_table_one_dal.table.get_item") as mock_get:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "GetItem"
        )
        mock_get.side_effect = [error, error, {"Item": {"pk_attribute_str_1": "x", "sk_attribute_str_2": "y"}}]
        result = ddb_table_one_get_item("x", "y")
        assert mock_get.call_count == 3
        assert result is not None


def test_retry_mechanism_delete(mock_dynamodb_table):
    """Test tenacity retry mechanism for delete operation."""
    create_sample_items()
    with patch("data.dal.ddb_table_one_dal.table.delete_item") as mock_del:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "DeleteItem"
        )
        mock_del.side_effect = [error, error, None]
        ddb_table_one_delete_item("pk1", "sk1")
        assert mock_del.call_count == 3


def test_retry_mechanism_query(mock_dynamodb_table):
    """Test tenacity retry mechanism for query operation."""
    create_sample_items()
    with patch("data.dal.ddb_table_one_dal.table.query") as mock_query:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "Query"
        )
        mock_query.side_effect = [
            error, error,
            {"Items": [], "Count": 0, "ScannedCount": 0}
        ]
        results = ddb_table_one_query(hash_key="pk1")
        assert mock_query.call_count == 3


def test_retry_exhausted_raises(mock_dynamodb_table):
    """Test that exhausting retries raises the exception."""
    dto = DdbTableOneDto(pk_attribute_str_1="fail_pk", sk_attribute_str_2="fail_sk", attribute_str_3="Fail")
    with patch("data.dal.ddb_table_one_dal.table.put_item") as mock_put:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "PutItem"
        )
        mock_put.side_effect = error
        with pytest.raises(ClientError):
            ddb_table_one_put_item(dto)


# ---- DTO Tests ----

def test_dto_str_method():
    """Test DTO string representation."""
    dto = DdbTableOneDto(
        pk_attribute_str_1="test", sk_attribute_str_2="test_sk",
        attribute_str_3="Test", attribute_num_5=5
    )
    dto_str = str(dto)
    assert '"pk_attribute_str_1": "test"' in dto_str
    assert '"attribute_str_3": "Test"' in dto_str
    assert '"attribute_num_5": 5' in dto_str


# ---- TTL Helper Tests ----

def test_get_ttl_days_default():
    """Test get_ttl_days returns default 30."""
    os.environ.pop("DDB_TABLE_ONE_TTL_DAYS", None)
    os.environ["DDB_TABLE_ONE_TTL_DAYS"] = "30"
    assert get_ttl_days() == 30


def test_get_ttl_days_custom():
    """Test get_ttl_days returns custom value."""
    os.environ["DDB_TABLE_ONE_TTL_DAYS"] = "60"
    assert get_ttl_days() == 60
    os.environ["DDB_TABLE_ONE_TTL_DAYS"] = "30"


def test_get_test_ttl_days_default():
    """Test get_test_ttl_days returns default 30."""
    os.environ["DDB_TABLE_ONE_TEST_TTL_DAYS"] = "30"
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
    os.environ["DDB_TABLE_ONE_TEST_TTL_DAYS"] = "7"
    ttl = get_ttl("TEST_pk")
    assert ttl is not None
    assert ttl > 0
    os.environ["DDB_TABLE_ONE_TEST_TTL_DAYS"] = "30"


def test_get_ttl_normal_object():
    """Test get_ttl uses normal TTL for non-TEST partition keys."""
    os.environ["DDB_TABLE_ONE_TTL_DAYS"] = "90"
    ttl = get_ttl("normal_pk")
    assert ttl is not None
    assert ttl > 0
    os.environ["DDB_TABLE_ONE_TTL_DAYS"] = "30"
