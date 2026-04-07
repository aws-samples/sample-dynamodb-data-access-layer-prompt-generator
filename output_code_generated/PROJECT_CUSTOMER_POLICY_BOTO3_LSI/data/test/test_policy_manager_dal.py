import pytest
import os
from moto import mock_aws
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch, MagicMock
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from data.dto.policy_manager_dto import PolicyManagerDto
from data.dal.policy_manager_dal import (
    policy_manager_put_item,
    policy_manager_update_item,
    policy_manager_get_item,
    policy_manager_delete_item,
    policy_manager_create_or_update_item,
    policy_manager_query,
    policy_manager_query_base_table,
    policy_manager_batch_write,
    policy_manager_batch_get,
    gsi__gsipk1__gsisk1__index_query,
    gsi__gsipk2__gsisk2__index_query,
    lsi__policy_version__index_query,
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
    os.environ["POLICY_MANAGER"] = "PolicyManager"


@pytest.fixture
def mock_dynamodb_table(aws_credentials):
    """Create a mock DynamoDB table with GSIs and LSI for testing."""
    with mock_aws():
        dynamodb = table.meta.client
        dynamodb.create_table(
            TableName="PolicyManager",
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsipk1", "AttributeType": "S"},
                {"AttributeName": "gsisk1", "AttributeType": "S"},
                {"AttributeName": "gsipk2", "AttributeType": "S"},
                {"AttributeName": "gsisk2", "AttributeType": "S"},
                {"AttributeName": "policy_version", "AttributeType": "S"}
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi__gsipk1__gsisk1__index",
                    "KeySchema": [
                        {"AttributeName": "gsipk1", "KeyType": "HASH"},
                        {"AttributeName": "gsisk1", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                },
                {
                    "IndexName": "gsi__gsipk2__gsisk2__index",
                    "KeySchema": [
                        {"AttributeName": "gsipk2", "KeyType": "HASH"},
                        {"AttributeName": "gsisk2", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }
            ],
            LocalSecondaryIndexes=[
                {
                    "IndexName": "lsi__policy_version__index",
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "policy_version", "KeyType": "RANGE"}
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
        PolicyManagerDto(
            pk="policy1", sk="version1", policy_id="POL001",
            policy_name="Home Insurance", risk_id="RISK001",
            address="123 Main St", premium=1000, sum_insured=500000,
            policy_version="v1", start_date="2024-01-01",
            end_date="2025-01-01", limit=100000, deductible="1000",
            amount=50000, gsipk1="gsi1_pk1", gsisk1="gsi1_sk1",
            gsipk2="gsi2_pk1", gsisk2="gsi2_sk1"
        ),
        PolicyManagerDto(
            pk="policy1", sk="version2", policy_id="POL001",
            policy_name="Home Insurance", risk_id="RISK001",
            address="123 Main St", premium=1200, sum_insured=600000,
            policy_version="v2", start_date="2024-06-01",
            end_date="2025-06-01", limit=120000, deductible="1000",
            amount=60000, gsipk1="gsi1_pk1", gsisk1="gsi1_sk2",
            gsipk2="gsi2_pk1", gsisk2="gsi2_sk2"
        ),
        PolicyManagerDto(
            pk="policy2", sk="version1", policy_id="POL002",
            policy_name="Auto Insurance", risk_id="RISK002",
            address="456 Oak Ave", premium=800, sum_insured=50000,
            policy_version="v1", start_date="2024-02-01",
            end_date="2025-02-01", limit=25000, deductible="500",
            amount=10000, gsipk1="gsi1_pk2", gsisk1="gsi1_sk1",
            gsipk2="gsi2_pk2", gsisk2="gsi2_sk1"
        ),
        PolicyManagerDto(
            pk="policy3", sk="version1", policy_id="POL003",
            policy_name="Life Insurance", risk_id="RISK003",
            address="789 Pine Rd", premium=1500, sum_insured=1000000,
            policy_version="v1", start_date="2024-03-01",
            end_date="2025-03-01", limit=200000, deductible="2000",
            amount=100000, gsipk1="gsi1_pk1", gsisk1="gsi1_sk3",
            gsipk2="gsi2_pk1", gsisk2="gsi2_sk3"
        )
    ]
    for dto in sample_dtos:
        policy_manager_put_item(dto)
    return sample_dtos


# ---- Put Item Tests ----

def test_put_item(mock_dynamodb_table):
    """Test putting an item into the table."""
    dto = PolicyManagerDto(
        pk="test_pk", sk="test_sk", policy_id="POL999",
        policy_name="Test Policy", risk_id="RISK999",
        address="789 Test St", premium=1500, sum_insured=750000,
        policy_version="v1", start_date="2024-03-01"
    )
    policy_manager_put_item(dto)
    result = policy_manager_get_item("test_pk", "test_sk")
    assert result is not None
    assert result.pk == "test_pk"
    assert result.policy_name == "Test Policy"


def test_put_item_sets_version(mock_dynamodb_table):
    """Test that put_item sets version to 1."""
    dto = PolicyManagerDto(pk="ver_pk", sk="ver_sk", policy_id="POL_V",
                           policy_name="Version", risk_id="R1",
                           address="Addr", premium=100, sum_insured=1000,
                           policy_version="v1", start_date="2024-01-01")
    policy_manager_put_item(dto)
    raw = table.get_item(Key={"pk": "ver_pk", "sk": "ver_sk"})["Item"]
    assert raw["version"] == 1


def test_put_item_sets_ttl(mock_dynamodb_table):
    """Test that put_item sets time_to_live attribute."""
    dto = PolicyManagerDto(pk="ttl_pk", sk="ttl_sk", policy_id="POL_T",
                           policy_name="TTL", risk_id="R1",
                           address="Addr", premium=100, sum_insured=1000,
                           policy_version="v1", start_date="2024-01-01")
    policy_manager_put_item(dto)
    raw = table.get_item(Key={"pk": "ttl_pk", "sk": "ttl_sk"})["Item"]
    assert "time_to_live" in raw


def test_put_item_sets_created_at(mock_dynamodb_table):
    """Test that put_item sets created_at timestamp."""
    dto = PolicyManagerDto(pk="ts_pk", sk="ts_sk", policy_id="POL_TS",
                           policy_name="TS", risk_id="R1",
                           address="Addr", premium=100, sum_insured=1000,
                           policy_version="v1", start_date="2024-01-01")
    policy_manager_put_item(dto)
    raw = table.get_item(Key={"pk": "ts_pk", "sk": "ts_sk"})["Item"]
    assert "created_at" in raw
    assert raw["created_at"].endswith("Z")


# ---- Get Item Tests ----

def test_get_item(mock_dynamodb_table):
    """Test getting an item from the table."""
    create_sample_items()
    dto = policy_manager_get_item("policy1", "version1")
    assert dto is not None
    assert dto.pk == "policy1"
    assert dto.policy_name == "Home Insurance"
    assert dto.premium == 1000


def test_get_item_not_found(mock_dynamodb_table):
    """Test getting an item that doesn't exist."""
    dto = policy_manager_get_item("nonexistent", "nonexistent")
    assert dto is None


def test_get_item_nonexistent_pk(mock_dynamodb_table):
    """Test getting item with non-existent partition key."""
    create_sample_items()
    dto = policy_manager_get_item("no_such_pk_999", "version1")
    assert dto is None


def test_get_item_nonexistent_sk(mock_dynamodb_table):
    """Test getting item with valid pk but non-existent sort key."""
    create_sample_items()
    dto = policy_manager_get_item("policy1", "no_such_sk_999")
    assert dto is None


# ---- Update Item Tests ----

def test_update_item(mock_dynamodb_table):
    """Test updating an item in the table."""
    create_sample_items()
    dto = PolicyManagerDto(
        pk="policy1", sk="version1",
        policy_name="Updated Home Insurance", premium=1100
    )
    policy_manager_update_item(dto)
    result = policy_manager_get_item("policy1", "version1")
    assert result.policy_name == "Updated Home Insurance"
    assert result.premium == 1100
    assert result.policy_id == "POL001"


def test_update_item_increments_version(mock_dynamodb_table):
    """Test that update increments the version attribute."""
    create_sample_items()
    raw_before = table.get_item(Key={"pk": "policy1", "sk": "version1"})["Item"]
    version_before = raw_before["version"]
    dto = PolicyManagerDto(pk="policy1", sk="version1", policy_name="Updated")
    policy_manager_update_item(dto)
    raw_after = table.get_item(Key={"pk": "policy1", "sk": "version1"})["Item"]
    assert raw_after["version"] == version_before + 1


def test_update_item_sets_updated_at(mock_dynamodb_table):
    """Test that update sets updated_at timestamp."""
    create_sample_items()
    dto = PolicyManagerDto(pk="policy1", sk="version1", policy_name="TS")
    policy_manager_update_item(dto)
    raw = table.get_item(Key={"pk": "policy1", "sk": "version1"})["Item"]
    assert "updated_at" in raw
    assert raw["updated_at"].endswith("Z")


def test_update_nonexistent_item_raises(mock_dynamodb_table):
    """Test that updating a non-existent item raises an exception."""
    dto = PolicyManagerDto(pk="no_pk", sk="no_sk", policy_name="Ghost")
    with pytest.raises(Exception):
        policy_manager_update_item(dto)


# ---- Delete Item Tests ----

def test_delete_item(mock_dynamodb_table):
    """Test deleting an item from the table."""
    create_sample_items()
    policy_manager_delete_item("policy1", "version1")
    result = policy_manager_get_item("policy1", "version1")
    assert result is None


def test_delete_nonexistent_item(mock_dynamodb_table):
    """Test deleting a non-existent item does not raise."""
    policy_manager_delete_item("nonexistent", "nonexistent")


# ---- Create or Update Tests ----

def test_create_or_update_item_create(mock_dynamodb_table):
    """Test create_or_update when item doesn't exist (create path)."""
    dto = PolicyManagerDto(
        pk="new_pk", sk="new_sk", policy_id="POL777",
        policy_name="New Policy", risk_id="RISK777",
        address="New Address", premium=900, sum_insured=450000,
        policy_version="v1", start_date="2024-05-01"
    )
    policy_manager_create_or_update_item(dto)
    result = policy_manager_get_item("new_pk", "new_sk")
    assert result is not None
    assert result.policy_name == "New Policy"


def test_create_or_update_item_update(mock_dynamodb_table):
    """Test create_or_update when item exists (update path)."""
    create_sample_items()
    dto = PolicyManagerDto(
        pk="policy1", sk="version1",
        policy_name="Updated Policy", premium=1300
    )
    policy_manager_create_or_update_item(dto)
    result = policy_manager_get_item("policy1", "version1")
    assert result.policy_name == "Updated Policy"
    assert result.premium == 1300


# ---- Base Table Query Tests ----

def test_query_base_table(mock_dynamodb_table):
    """Test querying the base table."""
    create_sample_items()
    result = policy_manager_query_base_table("policy1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 2
    assert all(dto.pk == "policy1" for dto in result["items"])


def test_query_base_table_with_sort_condition(mock_dynamodb_table):
    """Test querying base table with sort key condition."""
    create_sample_items()
    result = policy_manager_query_base_table(
        "policy1", Key("sk").begins_with("version1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].sk == "version1"


def test_query_base_table_with_filter_condition(mock_dynamodb_table):
    """Test base table query with filter condition."""
    create_sample_items()
    result = policy_manager_query_base_table(
        "policy1", filter_key_condition=Attr("premium").gte(1100)
    )
    assert all(dto.premium >= 1100 for dto in result["items"])


def test_query_base_table_with_pagination(mock_dynamodb_table):
    """Test pagination of query results."""
    for i in range(5):
        dto = PolicyManagerDto(
            pk="pagination_test", sk=f"sk{i}",
            policy_id=f"POL{i}", policy_name=f"Policy{i}",
            risk_id=f"RISK{i}", address=f"Address{i}",
            premium=1000 * i, sum_insured=500000 * i,
            policy_version="v1", start_date="2024-01-01"
        )
        policy_manager_put_item(dto)
    result = policy_manager_query_base_table("pagination_test")
    assert len(result["items"]) == 5


def test_query_base_table_empty_result(mock_dynamodb_table):
    """Test base table query with non-existent key."""
    result = policy_manager_query_base_table("nonexistent_pk")
    assert len(result["items"]) == 0


# ---- Generic Query Tests ----

def test_query_with_filter_condition(mock_dynamodb_table):
    """Test query with filter condition."""
    create_sample_items()
    results = policy_manager_query(
        hash_key="policy1",
        filter_key_condition=Attr("premium").gte(1100)
    )
    assert len(results) == 1
    assert all(dto.premium >= 1100 for dto in results)


def test_query_with_range_key_condition(mock_dynamodb_table):
    """Test query with range key condition."""
    create_sample_items()
    results = policy_manager_query(
        hash_key="policy1",
        range_key_and_condition=Key("sk").begins_with("version1")
    )
    assert len(results) == 1
    assert results[0].sk == "version1"


def test_query_scan_index_forward_false(mock_dynamodb_table):
    """Test query with descending sort order."""
    create_sample_items()
    results_asc = policy_manager_query(hash_key="policy1", scan_index_forward=True)
    results_desc = policy_manager_query(hash_key="policy1", scan_index_forward=False)
    assert len(results_asc) == len(results_desc) == 2
    if len(results_asc) > 1:
        assert results_asc[0].sk != results_desc[0].sk


def test_query_with_consistent_read(mock_dynamodb_table):
    """Test query with consistent read enabled."""
    create_sample_items()
    results = policy_manager_query(hash_key="policy1", consistent_read=True)
    assert len(results) >= 1
    assert all(dto.pk == "policy1" for dto in results)


def test_query_with_query_limit(mock_dynamodb_table):
    """Test query with limit parameter."""
    create_sample_items()
    results = policy_manager_query(hash_key="policy1", query_limit=1)
    assert len(results) == 1


def test_query_with_attributes_to_get(mock_dynamodb_table):
    """Test query with projection expression."""
    create_sample_items()
    results = policy_manager_query(
        hash_key="policy1", attribute_to_get=["pk", "sk", "policy_name"]
    )
    assert len(results) >= 1
    for dto in results:
        assert dto.pk is not None
        assert dto.policy_name is not None


def test_query_with_index_name(mock_dynamodb_table):
    """Test query using GSI index."""
    create_sample_items()
    results = policy_manager_query(
        hash_key="gsi1_pk1",
        index_name="gsi__gsipk1__gsisk1__index"
    )
    assert len(results) >= 1
    assert all(dto.gsipk1 == "gsi1_pk1" for dto in results)


def test_query_empty_result_nonexistent_key(mock_dynamodb_table):
    """Test query with non-existent hash key returns empty results."""
    create_sample_items()
    results = policy_manager_query(hash_key="nonexistent_key_12345")
    assert len(results) == 0


def test_query_with_multiple_filters(mock_dynamodb_table):
    """Test query with multiple filter conditions combined."""
    create_sample_items()
    filter_condition = Attr("policy_name").eq("Home Insurance") & Attr("premium").gte(1000)
    results = policy_manager_query(
        hash_key="policy1", filter_key_condition=filter_condition
    )
    assert all(dto.policy_name == "Home Insurance" and dto.premium >= 1000 for dto in results)


def test_query_with_last_evaluated_key(mock_dynamodb_table):
    """Test query pagination using last_evaluated_key."""
    for i in range(10):
        dto = PolicyManagerDto(
            pk="pagination_key", sk=f"sk{i:02d}",
            policy_id=f"POL{i}", policy_name=f"Policy{i}",
            risk_id=f"RISK{i}", address=f"Addr{i}",
            premium=100 * i, sum_insured=1000 * i,
            policy_version="v1", start_date="2024-01-01"
        )
        policy_manager_put_item(dto)
    result = policy_manager_query_base_table("pagination_key")
    assert len(result["items"]) == 10


# ---- GSI1 Query Tests ----

def test_gsi1_query(mock_dynamodb_table):
    """Test querying GSI1."""
    create_sample_items()
    result = gsi__gsipk1__gsisk1__index_query("gsi1_pk1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 3
    assert all(dto.gsipk1 == "gsi1_pk1" for dto in result["items"])


def test_gsi1_query_with_sort_condition(mock_dynamodb_table):
    """Test GSI1 query with sort key condition."""
    create_sample_items()
    result = gsi__gsipk1__gsisk1__index_query(
        "gsi1_pk1", Key("gsisk1").eq("gsi1_sk1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].gsisk1 == "gsi1_sk1"


def test_gsi1_query_with_filter_condition(mock_dynamodb_table):
    """Test GSI1 query with filter condition."""
    create_sample_items()
    result = gsi__gsipk1__gsisk1__index_query(
        "gsi1_pk1", filter_key_condition=Attr("premium").gte(1200)
    )
    assert all(dto.premium >= 1200 for dto in result["items"])


def test_gsi1_query_with_sort_condition_range(mock_dynamodb_table):
    """Test GSI1 query with sort key range condition."""
    create_sample_items()
    result = gsi__gsipk1__gsisk1__index_query(
        "gsi1_pk1",
        sort_key_condition=Key("gsisk1").between("gsi1_sk1", "gsi1_sk3")
    )
    assert all(
        "gsi1_sk1" <= dto.gsisk1 <= "gsi1_sk3"
        for dto in result["items"] if dto.gsisk1 is not None
    )


def test_gsi1_query_empty_result(mock_dynamodb_table):
    """Test GSI1 query with non-existent key."""
    result = gsi__gsipk1__gsisk1__index_query("nonexistent_gsi")
    assert len(result["items"]) == 0


# ---- GSI2 Query Tests ----

def test_gsi2_query(mock_dynamodb_table):
    """Test querying GSI2."""
    create_sample_items()
    result = gsi__gsipk2__gsisk2__index_query("gsi2_pk1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 3
    assert all(dto.gsipk2 == "gsi2_pk1" for dto in result["items"])


def test_gsi2_query_with_sort_condition(mock_dynamodb_table):
    """Test GSI2 query with sort key condition."""
    create_sample_items()
    result = gsi__gsipk2__gsisk2__index_query(
        "gsi2_pk1", Key("gsisk2").eq("gsi2_sk1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].gsisk2 == "gsi2_sk1"


def test_gsi2_query_with_sort_condition_range(mock_dynamodb_table):
    """Test GSI2 query with sort key range condition."""
    create_sample_items()
    result = gsi__gsipk2__gsisk2__index_query(
        "gsi2_pk1",
        sort_key_condition=Key("gsisk2").between("gsi2_sk1", "gsi2_sk3")
    )
    assert all(
        "gsi2_sk1" <= dto.gsisk2 <= "gsi2_sk3"
        for dto in result["items"] if dto.gsisk2 is not None
    )


def test_gsi2_query_empty_result(mock_dynamodb_table):
    """Test GSI2 query with non-existent key."""
    result = gsi__gsipk2__gsisk2__index_query("nonexistent_gsi")
    assert len(result["items"]) == 0


# ---- LSI Query Tests ----

def test_lsi_query(mock_dynamodb_table):
    """Test querying the LSI by policy_version."""
    create_sample_items()
    result = lsi__policy_version__index_query("policy1")
    assert result is not None
    assert "items" in result
    assert len(result["items"]) == 2
    assert all(dto.pk == "policy1" for dto in result["items"])


def test_lsi_query_with_sort_condition(mock_dynamodb_table):
    """Test LSI query with policy_version sort key condition."""
    create_sample_items()
    result = lsi__policy_version__index_query(
        "policy1", sort_key_condition=Key("policy_version").eq("v1")
    )
    assert len(result["items"]) == 1
    assert result["items"][0].policy_version == "v1"


def test_lsi_query_with_filter_condition(mock_dynamodb_table):
    """Test LSI query with filter condition."""
    create_sample_items()
    result = lsi__policy_version__index_query(
        "policy1", filter_key_condition=Attr("premium").gte(1100)
    )
    assert all(dto.premium >= 1100 for dto in result["items"])


def test_lsi_query_with_sort_range(mock_dynamodb_table):
    """Test LSI query with policy_version range condition."""
    create_sample_items()
    result = lsi__policy_version__index_query(
        "policy1",
        sort_key_condition=Key("policy_version").between("v1", "v2")
    )
    assert len(result["items"]) == 2


def test_lsi_query_empty_result(mock_dynamodb_table):
    """Test LSI query with non-existent key."""
    result = lsi__policy_version__index_query("nonexistent_pk")
    assert len(result["items"]) == 0


def test_lsi_query_nonexistent_version(mock_dynamodb_table):
    """Test LSI query with non-existent policy_version value."""
    create_sample_items()
    result = lsi__policy_version__index_query(
        "policy1", sort_key_condition=Key("policy_version").eq("v99")
    )
    assert len(result["items"]) == 0


# ---- Batch Write Tests ----

def test_batch_write(mock_dynamodb_table):
    """Test batch writing items."""
    dtos = [
        PolicyManagerDto(
            pk=f"batch_pk_{i}", sk=f"batch_sk_{i}",
            policy_id=f"POL{i}", policy_name=f"Policy{i}",
            risk_id=f"RISK{i}", address=f"Address{i}",
            premium=1000 * i, sum_insured=500000 * i,
            policy_version="v1", start_date="2024-01-01"
        )
        for i in range(5)
    ]
    policy_manager_batch_write(dtos)
    for i in range(5):
        result = policy_manager_get_item(f"batch_pk_{i}", f"batch_sk_{i}")
        assert result is not None
        assert result.policy_name == f"Policy{i}"


def test_batch_write_with_ttl(mock_dynamodb_table):
    """Test batch write sets TTL attribute."""
    dtos = [
        PolicyManagerDto(
            pk=f"ttl_pk_{i}", sk="ttl_sk",
            policy_id="POL001", policy_name="TTL Test",
            risk_id="RISK001", address="TTL Address",
            premium=1000, sum_insured=500000,
            policy_version="v1", start_date="2024-01-01"
        )
        for i in range(3)
    ]
    policy_manager_batch_write(dtos)
    for i in range(3):
        item = policy_manager_get_item(f"ttl_pk_{i}", "ttl_sk")
        assert item is not None
        assert item.policy_name == "TTL Test"


# ---- Batch Get Tests ----

def test_batch_get(mock_dynamodb_table):
    """Test batch getting items."""
    create_sample_items()
    keys = [
        {"pk": "policy1", "sk": "version1"},
        {"pk": "policy2", "sk": "version1"},
        {"pk": "policy3", "sk": "version1"}
    ]
    results = policy_manager_batch_get(keys)
    assert len(results) == 3
    pks = {dto.pk for dto in results}
    assert "policy1" in pks
    assert "policy2" in pks
    assert "policy3" in pks


def test_batch_get_empty_result(mock_dynamodb_table):
    """Test batch get with non-existent keys."""
    keys = [
        {"pk": "nonexistent1", "sk": "nonexistent1"},
        {"pk": "nonexistent2", "sk": "nonexistent2"}
    ]
    results = policy_manager_batch_get(keys)
    assert len(results) == 0


def test_batch_get_partial_result(mock_dynamodb_table):
    """Test batch get with mix of existent and non-existent keys."""
    create_sample_items()
    keys = [
        {"pk": "policy1", "sk": "version1"},
        {"pk": "nonexistent", "sk": "nonexistent"}
    ]
    results = policy_manager_batch_get(keys)
    assert len(results) == 1
    assert results[0].pk == "policy1"


# ---- Retry Mechanism Tests ----

def test_retry_mechanism_put(mock_dynamodb_table):
    """Test tenacity retry mechanism for put operation."""
    dto = PolicyManagerDto(
        pk="retry_test", sk="retry_sk", policy_id="POL999",
        policy_name="Retry Test", risk_id="RISK999",
        address="Retry Address", premium=1000, sum_insured=500000,
        policy_version="v1", start_date="2024-01-01"
    )
    with patch("data.dal.policy_manager_dal.table.put_item") as mock_put:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "PutItem"
        )
        mock_put.side_effect = [error, error, None]
        policy_manager_put_item(dto)
        assert mock_put.call_count == 3


def test_retry_mechanism_get(mock_dynamodb_table):
    """Test tenacity retry mechanism for get operation."""
    with patch("data.dal.policy_manager_dal.table.get_item") as mock_get:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "GetItem"
        )
        mock_get.side_effect = [error, error, {"Item": {"pk": "x", "sk": "y"}}]
        result = policy_manager_get_item("x", "y")
        assert mock_get.call_count == 3
        assert result is not None


def test_retry_mechanism_delete(mock_dynamodb_table):
    """Test tenacity retry mechanism for delete operation."""
    create_sample_items()
    with patch("data.dal.policy_manager_dal.table.delete_item") as mock_del:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "DeleteItem"
        )
        mock_del.side_effect = [error, error, None]
        policy_manager_delete_item("policy1", "version1")
        assert mock_del.call_count == 3


def test_retry_mechanism_query(mock_dynamodb_table):
    """Test tenacity retry mechanism for query operation."""
    create_sample_items()
    with patch("data.dal.policy_manager_dal.table.query") as mock_query:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "Query"
        )
        mock_query.side_effect = [
            error, error,
            {"Items": [], "Count": 0, "ScannedCount": 0}
        ]
        results = policy_manager_query(hash_key="policy1")
        assert mock_query.call_count == 3


def test_retry_exhausted_raises(mock_dynamodb_table):
    """Test that exhausting retries raises the exception."""
    dto = PolicyManagerDto(pk="fail_pk", sk="fail_sk", policy_id="FAIL",
                           policy_name="Fail", risk_id="R1",
                           address="Addr", premium=100, sum_insured=1000,
                           policy_version="v1", start_date="2024-01-01")
    with patch("data.dal.policy_manager_dal.table.put_item") as mock_put:
        error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Exceeded"}},
            "PutItem"
        )
        mock_put.side_effect = error
        with pytest.raises(ClientError):
            policy_manager_put_item(dto)


# ---- DTO Tests ----

def test_dto_str_method():
    """Test DTO string representation."""
    dto = PolicyManagerDto(
        pk="test", sk="test_sk", policy_id="POL001",
        policy_name="Test", risk_id="RISK001",
        address="Test Address", premium=1000, sum_insured=500000,
        policy_version="v1", start_date="2024-01-01"
    )
    dto_str = str(dto)
    assert '"pk": "test"' in dto_str
    assert '"policy_name": "Test"' in dto_str
    assert '"premium": 1000' in dto_str


# ---- TTL Helper Tests ----

def test_get_ttl_days_default():
    """Test get_ttl_days returns default 30."""
    os.environ["POLICY_MANAGER_TTL_DAYS"] = "30"
    assert get_ttl_days() == 30


def test_get_ttl_days_custom():
    """Test get_ttl_days returns custom value."""
    os.environ["POLICY_MANAGER_TTL_DAYS"] = "60"
    assert get_ttl_days() == 60
    os.environ["POLICY_MANAGER_TTL_DAYS"] = "30"


def test_get_test_ttl_days_default():
    """Test get_test_ttl_days returns default 30."""
    os.environ["POLICY_MANAGER_TEST_TTL_DAYS"] = "30"
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
    os.environ["POLICY_MANAGER_TEST_TTL_DAYS"] = "7"
    ttl = get_ttl("TEST_pk")
    assert ttl is not None
    assert ttl > 0
    os.environ["POLICY_MANAGER_TEST_TTL_DAYS"] = "30"


def test_get_ttl_normal_object():
    """Test get_ttl uses normal TTL for non-TEST partition keys."""
    os.environ["POLICY_MANAGER_TTL_DAYS"] = "90"
    ttl = get_ttl("normal_pk")
    assert ttl is not None
    assert ttl > 0
    os.environ["POLICY_MANAGER_TTL_DAYS"] = "30"
