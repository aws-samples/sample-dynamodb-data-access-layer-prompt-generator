import pytest
import os
from moto import mock_aws
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch, MagicMock
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from data.dto.ddb_table_two_dto import DdbTableTwoDto
from data.dal.ddb_table_two_dal import (
    ddb_table_two_put_item,
    ddb_table_two_update_item,
    ddb_table_two_get_item,
    ddb_table_two_delete_item,
    ddb_table_two_create_or_update_item,
    ddb_table_two_query,
    ddb_table_two_query_base_table,
    ddb_table_two_batch_write,
    ddb_table_two_batch_get,
    gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index_query,
    gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index_query,
    gsi__gsi3pk_attr_str_11__index_query,
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
    os.environ["DDB_TABLE_TWO"] = "ddb_table_two"


@pytest.fixture
def mock_dynamodb_table(aws_credentials):
    """Create a mock DynamoDB table with 3 GSIs for testing."""
    with mock_aws():
        dynamodb = table.meta.client
        dynamodb.create_table(
            TableName="ddb_table_two",
            KeySchema=[
                {"AttributeName": "pk_attr_str_1", "KeyType": "HASH"},
                {"AttributeName": "sk_attr_str_2", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk_attr_str_1", "AttributeType": "S"},
                {"AttributeName": "sk_attr_str_2", "AttributeType": "S"},
                {"AttributeName": "gsi1pk_attr_str_7", "AttributeType": "S"},
                {"AttributeName": "gsi1sk_attr_str_8", "AttributeType": "S"},
                {"AttributeName": "gsi2pk_attr_str_9", "AttributeType": "S"},
                {"AttributeName": "gsi2sk_attr_str_10", "AttributeType": "S"},
                {"AttributeName": "gsi3pk_attr_str_11", "AttributeType": "S"}
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index",
                    "KeySchema": [
                        {"AttributeName": "gsi1pk_attr_str_7", "KeyType": "HASH"},
                        {"AttributeName": "gsi1sk_attr_str_8", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                },
                {
                    "IndexName": "gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index",
                    "KeySchema": [
                        {"AttributeName": "gsi2pk_attr_str_9", "KeyType": "HASH"},
                        {"AttributeName": "gsi2sk_attr_str_10", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                },
                {
                    "IndexName": "gsi__gsi3pk_attr_str_11__index",
                    "KeySchema": [
                        {"AttributeName": "gsi3pk_attr_str_11", "KeyType": "HASH"}
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
        DdbTableTwoDto(
            pk_attr_str_1="pk1", sk_attr_str_2="sk1",
            attr_str_3="str3_val1", attr_str_4="str4_val1",
            attr_str_5="str5_val1", attr_str_6="str6_val1",
            gsi1pk_attr_str_7="gsi1_pk1", gsi1sk_attr_str_8="gsi1_sk1",
            gsi2pk_attr_str_9="gsi2_pk1", gsi2sk_attr_str_10="gsi2_sk1",
            gsi3pk_attr_str_11="gsi3_pk1"
        ),
        DdbTableTwoDto(
            pk_attr_str_1="pk1", sk_attr_str_2="sk2",
            attr_str_3="str3_val2", attr_str_4="str4_val2",
            attr_str_5="str5_val2", attr_str_6="str6_val2",
            gsi1pk_attr_str_7="gsi1_pk1", gsi1sk_attr_str_8="gsi1_sk2",
            gsi2pk_attr_str_9="gsi2_pk1", gsi2sk_attr_str_10="gsi2_sk2",
            gsi3pk_attr_str_11="gsi3_pk1"
        ),
        DdbTableTwoDto(
            pk_attr_str_1="pk2", sk_attr_str_2="sk1",
            attr_str_3="str3_val3", attr_str_4="str4_val3",
            attr_str_5="str5_val3", attr_str_6="str6_val3",
            gsi1pk_attr_str_7="gsi1_pk2", gsi1sk_attr_str_8="gsi1_sk1",
            gsi2pk_attr_str_9="gsi2_pk2", gsi2sk_attr_str_10="gsi2_sk1",
            gsi3pk_attr_str_11="gsi3_pk2"
        ),
        DdbTableTwoDto(
            pk_attr_str_1="pk3", sk_attr_str_2="sk1",
            attr_str_3="str3_val4", attr_str_4="str4_val4",
            attr_str_5="str5_val4", attr_str_6="str6_val4",
            gsi1pk_attr_str_7="gsi1_pk1", gsi1sk_attr_str_8="gsi1_sk3",
            gsi2pk_attr_str_9="gsi2_pk1", gsi2sk_attr_str_10="gsi2_sk3",
            gsi3pk_attr_str_11="gsi3_pk1"
        )
    ]
    for dto in sample_dtos:
        ddb_table_two_put_item(dto)
    return sample_dtos


# ---- Put Item Tests ----

def test_put_item(mock_dynamodb_table):
    """Test putting an item into the table."""
    dto = DdbTableTwoDto(
        pk_attr_str_1="test_pk", sk_attr_str_2="test_sk",
        attr_str_3="val3", attr_str_4="val4"
    )
    ddb_table_two_put_item(dto)
    result = ddb_table_two_get_item("test_pk", "test_sk")
    assert result is not None
    assert result.pk_attr_str_1 == "test_pk"
    assert result.attr_str_3 == "val3"


def test_put_item_sets_version(mock_dynamodb_table):
    """Test that put_item sets version to 1."""
    dto = DdbTableTwoDto(pk_attr_str_1="ver_pk", sk_attr_str_2="ver_sk", attr_str_3="Version")
    ddb_table_two_put_item(dto)
    raw = table.get_item(Key={"pk_attr_str_1": "ver_pk", "sk_attr_str_2": "ver_sk"})["Item"]
    assert raw["version"] == 1


def test_put_item_sets_ttl(mock_dynamodb_table):
    """Test that put_item sets time_to_live attribute."""
    dto = DdbTableTwoDto(pk_attr_str_1="ttl_pk", sk_attr_str_2="ttl_sk", attr_str_3="TTL")
    ddb_table_two_put_item(dto)
    raw = table.get_item(Key={"pk_attr_str_1": "ttl_pk", "sk_attr_str_2": "ttl_sk"})["Item"]
    assert "time_to_live" in raw
    assert raw["time_to_live"] > 0


def test_put_item_does_not_set_created_at(mock_dynamodb_table):
    """Test that put_item does NOT set created_at (table doesn't have it)."""
    dto = DdbTableTwoDto(pk_attr_str_1="no_ca_pk", sk_attr_str_2="no_ca_sk", attr_str_3="NoCreatedAt")
    ddb_table_two_put_item(dto)
    raw = table.get_item(Key={"pk_attr_str_1": "no_ca_pk", "sk_attr_str_2": "no_ca_sk"})["Item"]
    assert "created_at" not in raw


def test_put_item_with_all_gsi_keys(mock_dynamodb_table):
    """Test putting an item with all GSI keys populated."""
    dto = DdbTableTwoDto(
        pk_attr_str_1="gsi_pk", sk_attr_str_2="gsi_sk",
        attr_str_3="val3",
        gsi1pk_attr_str_7="g1pk", gsi1sk_attr_str_8="g1sk",
        gsi2pk_attr_str_9="g2pk", gsi2sk_attr_str_10="g2sk",
        gsi3pk_attr_str_11="g3pk"
    )
    ddb_table_two_put_item(dto)
    result = ddb_table_two_get_item("gsi_pk", "gsi_sk")
    assert result is not None
    assert result.gsi1pk_attr_str_7 == "g1pk"
    assert result.gsi2pk_attr_str_9 == "g2pk"
    assert result.gsi3pk_attr_str_11 == "g3pk"


# ---- Get Item Tests ----

def test_get_item(mock_dynamodb_table):
    """Test getting an item from the table."""
    create_sample_items()
    dto = ddb_table_two_get_item("pk1", "sk1")
    assert dto is not None
    assert dto.pk_attr_str_1 == "pk1"
    assert dto.attr_str_3 == "str3_val1"


def test_get_item_not_found(mock_dynamodb_table):
    """Test getting an item that doesn't exist returns None."""
    dto = ddb_table_two_get_item("nonexistent", "nonexistent")
    assert dto is None


def test_get_item_nonexistent_pk(mock_dynamodb_table):
    """Test getting item with non-existent partition key."""
    create_sample_items()
    dto = ddb_table_two_get_item("no_such_pk_999", "sk1")
    assert dto is None


def test_get_item_nonexistent_sk(mock_dynamodb_table):
    """Test getting item with valid pk but non-existent sort key."""
    create_sample_items()
    dto = ddb_table_two_get_item("pk1", "no_such_sk_999")
    assert dto is None


# ---- Update Item Tests ----

def test_update_item(mock_dynamodb_table):
    """Test updating an item in the table with optimistic locking."""
    create_sample_items()
    dto = DdbTableTwoDto(
        pk_attr_str_1="pk1", sk_attr_str_2="sk1",
        attr_str_3="updated_str3", attr_str_4="updated_str4"
    )
    ddb_table_two_update_item(dto)
    result = ddb_table_two_get_item("pk1", "sk1")
    assert result.attr_str_3 == "updated_str3"
    assert result.attr_str_4 == "updated_str4"
    assert result.attr_str_5 == "str5_val1"  # Original should remain


def test_update_item_increments_version(mock_dynamodb_table):
    """Test that update increments the version attribute."""
    create_sample_items()
    raw_before = table.get_item(Key={"pk_attr_str_1": "pk1", "sk_attr_str_2": "sk1"})["Item"]
    version_before = raw_before["version"]
    dto = DdbTableTwoDto(pk_attr_str_1="pk1", sk_attr_str_2="sk1", attr_str_3="Updated")
    ddb_table_two_update_item(dto)
    raw_after = table.get_item(Key={"pk_attr_str_1": "pk1", "sk_attr_str_2": "sk1"})["Item"]
    assert raw_after["version"] == version_before + 1


def test_update_item_does_not_set_updated_at(mock_dynamodb_table):
    """Test that update does NOT set updated_at (table doesn't have it)."""
    create_sample_items()
    dto = DdbTableTwoDto(pk_attr_str_1="pk1", sk_attr_str_2="sk1", attr_str_3="NoUpdatedAt")
    ddb_table_two_update_item(dto)
    raw = table.get_item(Key={"pk_attr_str_1": "pk1", "sk_attr_str_2": "sk1"})["Item"]
    assert "updated_at" not in raw


def test_update_nonexistent_item_raises(mock_dynamodb_table):
    """Test that updating a non-existent item raises an exception."""
    dto = DdbTableTwoDto(pk_attr_str_1="no_pk", sk_attr_str_2="no_sk", attr_str_3="Ghost")
    with pytest.raises(Exception):
        ddb_table_two_update_item(dto)


def test_update_item_gsi_keys(mock_dynamodb_table):
    """Test updating GSI key attributes."""
    create_sample_items()
    dto = DdbTableTwoDto(
        pk_attr_str_1="pk1", sk_attr_str_2="sk1",
        gsi1pk_attr_str_7="new_gsi1_pk", gsi3pk_attr_str_11="new_gsi3_pk"
    )
    ddb_table_two_update_item(dto)
    result = ddb_table_two_get_item("pk1", "sk1")
    assert result.gsi1pk_attr_str_7 == "new_gsi1_pk"
    assert result.gsi3pk_attr_str_11 == "new_gsi3_pk"


# ---- Delete Item Tests ----

def test_delete_item(mock_dynamodb_table):
    """Test deleting an item from the table."""
    create_sample_items()
    ddb_table_two_delete_item("pk1", "sk1")
    result = ddb_table_two_get_item("pk1", "sk1")
    assert result is None


def test_delete_nonexistent_item(mock_dynamodb_table):
    """Test deleting a non-existent item does not raise."""
    ddb_table_two_delete_item("nonexistent", "nonexistent")


def test_delete_item_verify_other_items_remain(mock_dynamodb_table):
    """Test that deleting one item doesn't affect other items."""
    create_sample_items()
    ddb_table_two_delete_item("pk1", "sk1")
    result = ddb_table_two_get_item("pk1", "sk2")
    assert result is not None
    assert result.attr_str_3 == "str3_val2"


# ---- Create or Update Tests ----

def test_create_or_update_item_create(mock_dynamodb_table):
    """Test create_or_update when item doesn't exist (create path)."""
    dto = DdbTableTwoDto(
        pk_attr_str_1="new_pk", sk_attr_str_2="new_sk",
        attr_str_3="New", attr_str_4="Item"
    )
    ddb_table_two_create_or_update_item(dto)
    result = ddb_table_two_get_item("new_pk", "new_sk")
    assert result is not None
    assert result.attr_str_3 == "New"


def test_create_or_update_item_update(mock_dynamodb_table):
    """Test create_or_update when item exists (update path)."""
    create_sample_items()
    dto = DdbTableTwoDto(
        pk_attr_str_1="pk1", sk_attr_str_2="sk1",
        attr_str_3="Updated", attr_str_4="Values"
    )
    ddb_table_two_create_or_update_item(dto)
    result = ddb_table_two_get_item("pk1", "sk1")
    assert result.attr_str_3 == "Updated"
    assert result.attr_str_4 == "Values"


def test_create_or_update_does_not_set_created_at(mock_dynamodb_table):
    """Test create_or_update does NOT set created_at."""
    dto = DdbTableTwoDto(pk_attr_str_1="cou_pk", sk_attr_str_2="cou_sk", attr_str_3="COU")
    ddb_table_two_create_or_update_item(dto)
    raw = table.get_item(Key={"pk_attr_str_1": "cou_pk", "sk_attr_str_2": "cou_sk"})["Item"]
    assert "created_at" not in raw


def test_create_or_update_sets_version_on_create(mock_dynamodb_table):
    """Test create_or_update sets version=1 on new item."""
    dto = DdbTableTwoDto(pk_attr_str_1="cou_v_pk", sk_attr_str_2="cou_v_sk", attr_str_3="COU_V")
    ddb_table_two_create_or_update_item(dto)
    raw = table.get_item(Key={"pk_attr_str_1": "cou_v_pk", "sk_attr_str_2": "cou_v_sk"})["Item"]
    assert raw["version"] == 1


# ---- Base Table Query Tests ----

def test_query_base_table(mock_dynamodb_table):
    """Test querying the base table."""
    create_sample_items()
    result = ddb_table_two_query_base_table("pk1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 2
    assert all(dto.pk_attr_str_1 == "pk1" for dto in result["items"])


def test_query_base_table_with_sort_condition(mock_dynamodb_table):
    """Test querying base table with sort key condition."""
    create_sample_items()
    result = ddb_table_two_query_base_table(
        "pk1", Key("sk_attr_str_2").begins_with("sk1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].sk_attr_str_2 == "sk1"


def test_query_base_table_with_filter_condition(mock_dynamodb_table):
    """Test base table query with filter condition."""
    create_sample_items()
    result = ddb_table_two_query_base_table(
        "pk1", filter_key_condition=Attr("attr_str_3").eq("str3_val1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].attr_str_3 == "str3_val1"


def test_query_base_table_with_pagination(mock_dynamodb_table):
    """Test pagination of query results."""
    for i in range(5):
        dto = DdbTableTwoDto(
            pk_attr_str_1="pagination_test", sk_attr_str_2=f"sk{i}",
            attr_str_3=f"str3_{i}"
        )
        ddb_table_two_put_item(dto)
    result = ddb_table_two_query_base_table("pagination_test")
    assert len(result["items"]) == 5


def test_query_base_table_empty_result(mock_dynamodb_table):
    """Test base table query with non-existent key."""
    result = ddb_table_two_query_base_table("nonexistent_pk")
    assert len(result["items"]) == 0


# ---- Generic Query Tests ----

def test_query_with_filter_condition(mock_dynamodb_table):
    """Test query with filter condition."""
    create_sample_items()
    results = ddb_table_two_query(
        hash_key="pk1",
        filter_key_condition=Attr("attr_str_3").eq("str3_val1")
    )
    assert len(results) == 1
    assert results[0].attr_str_3 == "str3_val1"


def test_query_with_range_key_condition(mock_dynamodb_table):
    """Test query with range key condition."""
    create_sample_items()
    results = ddb_table_two_query(
        hash_key="pk1",
        range_key_and_condition=Key("sk_attr_str_2").begins_with("sk1")
    )
    assert len(results) == 1
    assert results[0].sk_attr_str_2 == "sk1"


def test_query_scan_index_forward_false(mock_dynamodb_table):
    """Test query with descending sort order."""
    create_sample_items()
    results_asc = ddb_table_two_query(hash_key="pk1", scan_index_forward=True)
    results_desc = ddb_table_two_query(hash_key="pk1", scan_index_forward=False)
    assert len(results_asc) == len(results_desc) == 2
    if len(results_asc) > 1:
        assert results_asc[0].sk_attr_str_2 != results_desc[0].sk_attr_str_2


def test_query_with_consistent_read(mock_dynamodb_table):
    """Test query with consistent read enabled."""
    create_sample_items()
    results = ddb_table_two_query(hash_key="pk1", consistent_read=True)
    assert len(results) >= 1
    assert all(dto.pk_attr_str_1 == "pk1" for dto in results)


def test_query_with_query_limit(mock_dynamodb_table):
    """Test query with limit parameter."""
    create_sample_items()
    results = ddb_table_two_query(hash_key="pk1", query_limit=1)
    assert len(results) == 1


def test_query_with_attributes_to_get(mock_dynamodb_table):
    """Test query with projection expression."""
    create_sample_items()
    results = ddb_table_two_query(
        hash_key="pk1", attribute_to_get=["pk_attr_str_1", "sk_attr_str_2", "attr_str_3"]
    )
    assert len(results) >= 1
    for dto in results:
        assert dto.pk_attr_str_1 is not None
        assert dto.attr_str_3 is not None


def test_query_with_gsi1_index_name(mock_dynamodb_table):
    """Test query using GSI1 index."""
    create_sample_items()
    results = ddb_table_two_query(
        hash_key="gsi1_pk1",
        index_name="gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index"
    )
    assert len(results) >= 1
    assert all(dto.gsi1pk_attr_str_7 == "gsi1_pk1" for dto in results)


def test_query_with_gsi2_index_name(mock_dynamodb_table):
    """Test query using GSI2 index."""
    create_sample_items()
    results = ddb_table_two_query(
        hash_key="gsi2_pk1",
        index_name="gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index"
    )
    assert len(results) >= 1
    assert all(dto.gsi2pk_attr_str_9 == "gsi2_pk1" for dto in results)


def test_query_with_gsi3_index_name(mock_dynamodb_table):
    """Test query using GSI3 index (hash key only)."""
    create_sample_items()
    results = ddb_table_two_query(
        hash_key="gsi3_pk1",
        index_name="gsi__gsi3pk_attr_str_11__index"
    )
    assert len(results) >= 1
    assert all(dto.gsi3pk_attr_str_11 == "gsi3_pk1" for dto in results)


def test_query_empty_result_nonexistent_key(mock_dynamodb_table):
    """Test query with non-existent hash key returns empty results."""
    create_sample_items()
    results = ddb_table_two_query(hash_key="nonexistent_key_12345")
    assert len(results) == 0


def test_query_with_multiple_filters(mock_dynamodb_table):
    """Test query with multiple filter conditions combined."""
    create_sample_items()
    filter_condition = Attr("attr_str_3").eq("str3_val1") & Attr("attr_str_4").eq("str4_val1")
    results = ddb_table_two_query(
        hash_key="pk1", filter_key_condition=filter_condition
    )
    assert all(dto.attr_str_3 == "str3_val1" and dto.attr_str_4 == "str4_val1" for dto in results)


def test_query_with_last_evaluated_key(mock_dynamodb_table):
    """Test query pagination using last_evaluated_key."""
    for i in range(10):
        dto = DdbTableTwoDto(
            pk_attr_str_1="pagination_key", sk_attr_str_2=f"sk{i:02d}",
            attr_str_3=f"str3_{i}"
        )
        ddb_table_two_put_item(dto)
    result = ddb_table_two_query_base_table("pagination_key")
    assert len(result["items"]) == 10


# ---- GSI1 Query Tests ----

def test_gsi1_query(mock_dynamodb_table):
    """Test querying GSI1."""
    create_sample_items()
    result = gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index_query("gsi1_pk1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 3
    assert all(dto.gsi1pk_attr_str_7 == "gsi1_pk1" for dto in result["items"])


def test_gsi1_query_with_sort_condition(mock_dynamodb_table):
    """Test GSI1 query with sort key condition."""
    create_sample_items()
    result = gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index_query(
        "gsi1_pk1", Key("gsi1sk_attr_str_8").eq("gsi1_sk1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].gsi1sk_attr_str_8 == "gsi1_sk1"


def test_gsi1_query_with_filter_condition(mock_dynamodb_table):
    """Test GSI1 query with filter condition."""
    create_sample_items()
    result = gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index_query(
        "gsi1_pk1", filter_key_condition=Attr("attr_str_3").eq("str3_val1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].attr_str_3 == "str3_val1"


def test_gsi1_query_with_sort_condition_range(mock_dynamodb_table):
    """Test GSI1 query with sort key range condition."""
    create_sample_items()
    result = gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index_query(
        "gsi1_pk1",
        sort_key_condition=Key("gsi1sk_attr_str_8").between("gsi1_sk1", "gsi1_sk3")
    )
    assert all(
        "gsi1_sk1" <= dto.gsi1sk_attr_str_8 <= "gsi1_sk3"
        for dto in result["items"] if dto.gsi1sk_attr_str_8 is not None
    )


def test_gsi1_query_empty_result(mock_dynamodb_table):
    """Test GSI1 query with non-existent key."""
    result = gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index_query("nonexistent_gsi1")
    assert len(result["items"]) == 0


# ---- GSI2 Query Tests ----

def test_gsi2_query(mock_dynamodb_table):
    """Test querying GSI2."""
    create_sample_items()
    result = gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index_query("gsi2_pk1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 3
    assert all(dto.gsi2pk_attr_str_9 == "gsi2_pk1" for dto in result["items"])


def test_gsi2_query_with_sort_condition(mock_dynamodb_table):
    """Test GSI2 query with sort key condition."""
    create_sample_items()
    result = gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index_query(
        "gsi2_pk1", Key("gsi2sk_attr_str_10").eq("gsi2_sk1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].gsi2sk_attr_str_10 == "gsi2_sk1"


def test_gsi2_query_with_filter_condition(mock_dynamodb_table):
    """Test GSI2 query with filter condition."""
    create_sample_items()
    result = gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index_query(
        "gsi2_pk1", filter_key_condition=Attr("attr_str_3").eq("str3_val2")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].attr_str_3 == "str3_val2"


def test_gsi2_query_with_sort_condition_range(mock_dynamodb_table):
    """Test GSI2 query with sort key range condition."""
    create_sample_items()
    result = gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index_query(
        "gsi2_pk1",
        sort_key_condition=Key("gsi2sk_attr_str_10").between("gsi2_sk1", "gsi2_sk3")
    )
    assert all(
        "gsi2_sk1" <= dto.gsi2sk_attr_str_10 <= "gsi2_sk3"
        for dto in result["items"] if dto.gsi2sk_attr_str_10 is not None
    )


def test_gsi2_query_empty_result(mock_dynamodb_table):
    """Test GSI2 query with non-existent key."""
    result = gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index_query("nonexistent_gsi2")
    assert len(result["items"]) == 0


# ---- GSI3 Query Tests (hash key only, no range key) ----

def test_gsi3_query(mock_dynamodb_table):
    """Test querying GSI3 (hash key only)."""
    create_sample_items()
    result = gsi__gsi3pk_attr_str_11__index_query("gsi3_pk1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 3
    assert all(dto.gsi3pk_attr_str_11 == "gsi3_pk1" for dto in result["items"])


def test_gsi3_query_with_filter_condition(mock_dynamodb_table):
    """Test GSI3 query with filter condition."""
    create_sample_items()
    result = gsi__gsi3pk_attr_str_11__index_query(
        "gsi3_pk1", filter_key_condition=Attr("attr_str_3").eq("str3_val1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].attr_str_3 == "str3_val1"


def test_gsi3_query_empty_result(mock_dynamodb_table):
    """Test GSI3 query with non-existent key."""
    result = gsi__gsi3pk_attr_str_11__index_query("nonexistent_gsi3")
    assert len(result["items"]) == 0


def test_gsi3_query_single_item(mock_dynamodb_table):
    """Test GSI3 query returning single item."""
    create_sample_items()
    result = gsi__gsi3pk_attr_str_11__index_query("gsi3_pk2")
    assert len(result["items"]) == 1
    assert result["items"][0].gsi3pk_attr_str_11 == "gsi3_pk2"


# ---- Batch Write Tests ----

def test_batch_write(mock_dynamodb_table):
    """Test batch writing items."""
    dtos = [
        DdbTableTwoDto(
            pk_attr_str_1=f"batch_pk_{i}", sk_attr_str_2=f"batch_sk_{i}",
            attr_str_3=f"str3_{i}", attr_str_4=f"str4_{i}"
        )
        for i in range(5)
    ]
    ddb_table_two_batch_write(dtos)
    for i in range(5):
        result = ddb_table_two_get_item(f"batch_pk_{i}", f"batch_sk_{i}")
        assert result is not None
        assert result.attr_str_3 == f"str3_{i}"


def test_batch_write_with_ttl(mock_dynamodb_table):
    """Test batch write sets TTL attribute."""
    dtos = [
        DdbTableTwoDto(
            pk_attr_str_1=f"ttl_pk_{i}", sk_attr_str_2="ttl_sk",
            attr_str_3="TTL"
        )
        for i in range(3)
    ]
    ddb_table_two_batch_write(dtos)
    for i in range(3):
        item = ddb_table_two_get_item(f"ttl_pk_{i}", "ttl_sk")
        assert item is not None
        assert item.attr_str_3 == "TTL"


def test_batch_write_does_not_set_created_at(mock_dynamodb_table):
    """Test batch write does NOT set created_at or updated_at."""
    dtos = [
        DdbTableTwoDto(
            pk_attr_str_1="bw_no_ca", sk_attr_str_2="bw_sk",
            attr_str_3="NoCa"
        )
    ]
    ddb_table_two_batch_write(dtos)
    raw = table.get_item(Key={"pk_attr_str_1": "bw_no_ca", "sk_attr_str_2": "bw_sk"})["Item"]
    assert "created_at" not in raw
    assert "updated_at" not in raw


# ---- Batch Get Tests ----

def test_batch_get(mock_dynamodb_table):
    """Test batch getting items."""
    create_sample_items()
    keys = [
        {"pk_attr_str_1": "pk1", "sk_attr_str_2": "sk1"},
        {"pk_attr_str_1": "pk2", "sk_attr_str_2": "sk1"},
        {"pk_attr_str_1": "pk3", "sk_attr_str_2": "sk1"}
    ]
    results = ddb_table_two_batch_get(keys)
    assert len(results) == 3
    pks = {dto.pk_attr_str_1 for dto in results}
    assert "pk1" in pks
    assert "pk2" in pks
    assert "pk3" in pks


def test_batch_get_empty_result(mock_dynamodb_table):
    """Test batch get with non-existent keys."""
    keys = [
        {"pk_attr_str_1": "nonexistent1", "sk_attr_str_2": "nonexistent1"},
        {"pk_attr_str_1": "nonexistent2", "sk_attr_str_2": "nonexistent2"}
    ]
    results = ddb_table_two_batch_get(keys)
    assert len(results) == 0


def test_batch_get_partial_result(mock_dynamodb_table):
    """Test batch get with mix of existent and non-existent keys."""
    create_sample_items()
    keys = [
        {"pk_attr_str_1": "pk1", "sk_attr_str_2": "sk1"},
        {"pk_attr_str_1": "nonexistent", "sk_attr_str_2": "nonexistent"}
    ]
    results = ddb_table_two_batch_get(keys)
    assert len(results) == 1
    assert results[0].pk_attr_str_1 == "pk1"


# ---- Retry Mechanism Tests ----

def test_retry_mechanism_put(mock_dynamodb_table):
    """Test tenacity retry mechanism for put operation."""
    dto = DdbTableTwoDto(
        pk_attr_str_1="retry_test", sk_attr_str_2="retry_sk",
        attr_str_3="Retry"
    )
    with patch("data.dal.ddb_table_two_dal.table.put_item") as mock_put:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "PutItem"
        )
        mock_put.side_effect = [error, error, None]
        ddb_table_two_put_item(dto)
        assert mock_put.call_count == 3


def test_retry_mechanism_get(mock_dynamodb_table):
    """Test tenacity retry mechanism for get operation."""
    with patch("data.dal.ddb_table_two_dal.table.get_item") as mock_get:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "GetItem"
        )
        mock_get.side_effect = [error, error, {"Item": {"pk_attr_str_1": "x", "sk_attr_str_2": "y"}}]
        result = ddb_table_two_get_item("x", "y")
        assert mock_get.call_count == 3
        assert result is not None


def test_retry_mechanism_delete(mock_dynamodb_table):
    """Test tenacity retry mechanism for delete operation."""
    create_sample_items()
    with patch("data.dal.ddb_table_two_dal.table.delete_item") as mock_del:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "DeleteItem"
        )
        mock_del.side_effect = [error, error, None]
        ddb_table_two_delete_item("pk1", "sk1")
        assert mock_del.call_count == 3


def test_retry_mechanism_query(mock_dynamodb_table):
    """Test tenacity retry mechanism for query operation."""
    create_sample_items()
    with patch("data.dal.ddb_table_two_dal.table.query") as mock_query:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "Query"
        )
        mock_query.side_effect = [
            error, error,
            {"Items": [], "Count": 0, "ScannedCount": 0}
        ]
        results = ddb_table_two_query(hash_key="pk1")
        assert mock_query.call_count == 3


def test_retry_exhausted_raises(mock_dynamodb_table):
    """Test that exhausting retries raises the exception."""
    dto = DdbTableTwoDto(pk_attr_str_1="fail_pk", sk_attr_str_2="fail_sk", attr_str_3="Fail")
    with patch("data.dal.ddb_table_two_dal.table.put_item") as mock_put:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "PutItem"
        )
        mock_put.side_effect = error
        with pytest.raises(ClientError):
            ddb_table_two_put_item(dto)


# ---- DTO Tests ----

def test_dto_str_method():
    """Test DTO string representation."""
    dto = DdbTableTwoDto(
        pk_attr_str_1="test", sk_attr_str_2="test_sk",
        attr_str_3="Test", attr_str_4="Value"
    )
    dto_str = str(dto)
    assert '"pk_attr_str_1": "test"' in dto_str
    assert '"attr_str_3": "Test"' in dto_str
    assert '"attr_str_4": "Value"' in dto_str


# ---- TTL Helper Tests ----

def test_get_ttl_days_default():
    """Test get_ttl_days returns default 30."""
    os.environ.pop("DDB_TABLE_TWO_TTL_DAYS", None)
    os.environ["DDB_TABLE_TWO_TTL_DAYS"] = "30"
    assert get_ttl_days() == 30


def test_get_ttl_days_custom():
    """Test get_ttl_days returns custom value."""
    os.environ["DDB_TABLE_TWO_TTL_DAYS"] = "60"
    assert get_ttl_days() == 60
    os.environ["DDB_TABLE_TWO_TTL_DAYS"] = "30"


def test_get_test_ttl_days_default():
    """Test get_test_ttl_days returns default 30."""
    os.environ["DDB_TABLE_TWO_TEST_TTL_DAYS"] = "30"
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
    os.environ["DDB_TABLE_TWO_TEST_TTL_DAYS"] = "7"
    ttl = get_ttl("TEST_pk")
    assert ttl is not None
    assert ttl > 0
    os.environ["DDB_TABLE_TWO_TEST_TTL_DAYS"] = "30"


def test_get_ttl_normal_object():
    """Test get_ttl uses normal TTL for non-TEST partition keys."""
    os.environ["DDB_TABLE_TWO_TTL_DAYS"] = "90"
    ttl = get_ttl("normal_pk")
    assert ttl is not None
    assert ttl > 0
    os.environ["DDB_TABLE_TWO_TTL_DAYS"] = "30"
