import pytest
import os
from moto import mock_aws
from datetime import datetime, timezone
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

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
    table
)


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_REGION'] = 'us-east-1'
    os.environ['DDB_TABLE_ONE'] = 'ddb_table_one'


@pytest.fixture
def mock_dynamodb_table(aws_credentials):
    """Create a mock DynamoDB table for testing."""
    with mock_aws():
        dynamodb = table.meta.client
        dynamodb.create_table(
            TableName='ddb_table_one',
            KeySchema=[
                {'AttributeName': 'pk_attribute_str_1', 'KeyType': 'HASH'},
                {'AttributeName': 'sk_attribute_str_2', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'pk_attribute_str_1', 'AttributeType': 'S'},
                {'AttributeName': 'sk_attribute_str_2', 'AttributeType': 'S'},
                {'AttributeName': 'gsipk_attribute_str_7', 'AttributeType': 'S'},
                {'AttributeName': 'gsisk_attribute_str_8', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index',
                    'KeySchema': [
                        {'AttributeName': 'gsipk_attribute_str_7', 'KeyType': 'HASH'},
                        {'AttributeName': 'gsisk_attribute_str_8', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        yield


def create_sample_items():
    """Create sample items in the mock table."""
    sample_dtos = [
        DdbTableOneDto(
            pk_attribute_str_1="pk1",
            sk_attribute_str_2="sk1",
            attribute_str_3="attr3_val1",
            attribute_str_4="attr4_val1",
            attribute_num_5=100,
            gsipk_attribute_str_7="gsi_pk1",
            gsisk_attribute_str_8="gsi_sk1"
        ),
        DdbTableOneDto(
            pk_attribute_str_1="pk1",
            sk_attribute_str_2="sk2",
            attribute_str_3="attr3_val2",
            attribute_str_4="attr4_val2",
            attribute_num_5=200,
            gsipk_attribute_str_7="gsi_pk1",
            gsisk_attribute_str_8="gsi_sk2"
        ),
        DdbTableOneDto(
            pk_attribute_str_1="pk2",
            sk_attribute_str_2="sk1",
            attribute_str_3="attr3_val3",
            attribute_str_4="attr4_val3",
            attribute_num_5=300,
            gsipk_attribute_str_7="gsi_pk2",
            gsisk_attribute_str_8="gsi_sk1"
        ),
        DdbTableOneDto(
            pk_attribute_str_1="pk3",
            sk_attribute_str_2="sk1",
            attribute_str_3="attr3_val4",
            attribute_str_4="attr4_val4",
            attribute_num_5=50,
            gsipk_attribute_str_7="gsi_pk1",
            gsisk_attribute_str_8="gsi_sk3"
        )
    ]
    
    for dto in sample_dtos:
        ddb_table_one_put_item(dto)
    
    return sample_dtos


def test_put_item(mock_dynamodb_table):
    """Test putting an item into the table."""
    dto = DdbTableOneDto(
        pk_attribute_str_1="test_pk",
        sk_attribute_str_2="test_sk",
        attribute_str_3="test_attr3",
        attribute_str_4="test_attr4",
        attribute_num_5=500
    )
    ddb_table_one_put_item(dto)
    result = ddb_table_one_get_item("test_pk", "test_sk")
    assert result is not None
    assert result.pk_attribute_str_1 == "test_pk"
    assert result.attribute_str_3 == "test_attr3"


def test_get_item(mock_dynamodb_table):
    """Test getting an item from the table."""
    create_sample_items()
    dto = ddb_table_one_get_item("pk1", "sk1")
    assert dto is not None
    assert dto.pk_attribute_str_1 == "pk1"
    assert dto.attribute_str_3 == "attr3_val1"
    assert dto.attribute_num_5 == 100


def test_get_item_not_found(mock_dynamodb_table):
    """Test getting an item that doesn't exist."""
    dto = ddb_table_one_get_item("nonexistent", "nonexistent")
    assert dto is None


def test_update_item(mock_dynamodb_table):
    """Test updating an item in the table."""
    create_sample_items()
    dto = DdbTableOneDto(
        pk_attribute_str_1="pk1",
        sk_attribute_str_2="sk1",
        attribute_str_3="updated_attr3",
        attribute_num_5=999
    )
    ddb_table_one_update_item(dto)
    result = ddb_table_one_get_item("pk1", "sk1")
    assert result.attribute_str_3 == "updated_attr3"
    assert result.attribute_num_5 == 999
    assert result.attribute_str_4 == "attr4_val1"  # Original should remain


def test_delete_item(mock_dynamodb_table):
    """Test deleting an item from the table."""
    create_sample_items()
    ddb_table_one_delete_item("pk1", "sk1")
    result = ddb_table_one_get_item("pk1", "sk1")
    assert result is None


def test_create_or_update_item_create(mock_dynamodb_table):
    """Test create_or_update when item doesn't exist (create path)."""
    dto = DdbTableOneDto(
        pk_attribute_str_1="new_pk",
        sk_attribute_str_2="new_sk",
        attribute_str_3="new_attr3",
        attribute_str_4="new_attr4",
        attribute_num_5=777
    )
    ddb_table_one_create_or_update_item(dto)
    result = ddb_table_one_get_item("new_pk", "new_sk")
    assert result is not None
    assert result.attribute_str_3 == "new_attr3"


def test_create_or_update_item_update(mock_dynamodb_table):
    """Test create_or_update when item exists (update path)."""
    create_sample_items()
    dto = DdbTableOneDto(
        pk_attribute_str_1="pk1",
        sk_attribute_str_2="sk1",
        attribute_str_3="updated_value",
        attribute_num_5=8888
    )
    ddb_table_one_create_or_update_item(dto)
    result = ddb_table_one_get_item("pk1", "sk1")
    assert result.attribute_str_3 == "updated_value"
    assert result.attribute_num_5 == 8888


def test_query_base_table(mock_dynamodb_table):
    """Test querying the base table."""
    create_sample_items()
    result = ddb_table_one_query_base_table("pk1")
    assert result is not None
    assert 'items' in result
    assert len(result['items']) == 2
    assert all(dto.pk_attribute_str_1 == "pk1" for dto in result['items'])


def test_query_base_table_with_sort_condition(mock_dynamodb_table):
    """Test querying base table with sort key condition."""
    create_sample_items()
    result = ddb_table_one_query_base_table(
        "pk1",
        Key('sk_attribute_str_2').begins_with('sk1')
    )
    assert result is not None
    assert len(result['items']) == 1
    assert result['items'][0].sk_attribute_str_2 == "sk1"


def test_query_base_table_with_pagination(mock_dynamodb_table):
    """Test pagination of query results."""
    for i in range(5):
        dto = DdbTableOneDto(
            pk_attribute_str_1="pagination_test",
            sk_attribute_str_2=f"sk{i}",
            attribute_str_3=f"attr3_{i}",
            attribute_str_4=f"attr4_{i}",
            attribute_num_5=100 * i
        )
        ddb_table_one_put_item(dto)
    
    result = ddb_table_one_query_base_table("pagination_test")
    assert len(result['items']) == 5


def test_gsi_query(mock_dynamodb_table):
    """Test querying the GSI."""
    create_sample_items()
    result = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query("gsi_pk1")
    assert result is not None
    assert 'items' in result
    assert len(result['items']) == 3
    assert all(dto.gsipk_attribute_str_7 == "gsi_pk1" for dto in result['items'])


def test_gsi_query_with_sort_condition(mock_dynamodb_table):
    """Test GSI query with sort key condition."""
    create_sample_items()
    result = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query(
        "gsi_pk1",
        Key('gsisk_attribute_str_8').eq("gsi_sk1")
    )
    assert result is not None
    assert len(result['items']) == 1
    assert result['items'][0].gsisk_attribute_str_8 == "gsi_sk1"


def test_batch_write(mock_dynamodb_table):
    """Test batch writing items."""
    dtos = [
        DdbTableOneDto(
            pk_attribute_str_1=f"batch_pk_{i}",
            sk_attribute_str_2=f"batch_sk_{i}",
            attribute_str_3=f"attr3_{i}",
            attribute_str_4=f"attr4_{i}",
            attribute_num_5=100 * i
        )
        for i in range(5)
    ]
    ddb_table_one_batch_write(dtos)
    
    for i in range(5):
        result = ddb_table_one_get_item(f"batch_pk_{i}", f"batch_sk_{i}")
        assert result is not None
        assert result.attribute_str_3 == f"attr3_{i}"


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
    assert 'pk1' in pks
    assert 'pk2' in pks
    assert 'pk3' in pks


def test_query_with_filter_condition(mock_dynamodb_table):
    """Test query with filter condition."""
    create_sample_items()
    results = ddb_table_one_query(
        hash_key="pk1",
        filter_key_condition=Attr('attribute_num_5').gte(150)
    )
    assert len(results) == 1
    assert all(dto.attribute_num_5 >= 150 for dto in results)


def test_dto_str_method():
    """Test DTO string representation."""
    dto = DdbTableOneDto(
        pk_attribute_str_1="test",
        sk_attribute_str_2="test_sk",
        attribute_str_3="attr3",
        attribute_str_4="attr4",
        attribute_num_5=100
    )
    dto_str = str(dto)
    assert '"pk_attribute_str_1": "test"' in dto_str
    assert '"attribute_str_3": "attr3"' in dto_str
    assert '"attribute_num_5": 100' in dto_str


def test_query_with_range_key_condition(mock_dynamodb_table):
    """Test query with range key condition."""
    create_sample_items()
    results = ddb_table_one_query(
        hash_key="pk1",
        range_key_and_condition=Key('sk_attribute_str_2').begins_with('sk1')
    )
    assert len(results) == 1
    assert results[0].sk_attribute_str_2 == "sk1"


def test_query_scan_index_forward_false(mock_dynamodb_table):
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


def test_query_with_consistent_read(mock_dynamodb_table):
    """Test query with consistent read enabled."""
    create_sample_items()
    results = ddb_table_one_query(
        hash_key="pk1",
        consistent_read=True
    )
    assert len(results) >= 1
    assert all(dto.pk_attribute_str_1 == "pk1" for dto in results)


def test_query_with_query_limit(mock_dynamodb_table):
    """Test query with limit parameter."""
    create_sample_items()
    results = ddb_table_one_query(
        hash_key="pk1",
        query_limit=1
    )
    assert len(results) == 1


def test_query_with_attributes_to_get(mock_dynamodb_table):
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


def test_query_base_table_empty_result(mock_dynamodb_table):
    """Test base table query with non-existent key."""
    result = ddb_table_one_query_base_table("nonexistent_pk")
    assert result is not None
    assert 'items' in result
    assert len(result['items']) == 0


def test_gsi_query_empty_result(mock_dynamodb_table):
    """Test GSI query with non-existent key."""
    result = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query("nonexistent_gsi")
    assert result is not None
    assert 'items' in result
    assert len(result['items']) == 0


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


def test_retry_mechanism_put(mock_dynamodb_table):
    """Test retry mechanism for put operation."""
    from unittest.mock import patch
    from botocore.exceptions import ClientError
    
    dto = DdbTableOneDto(
        pk_attribute_str_1="retry_test",
        sk_attribute_str_2="retry_sk",
        attribute_str_3="retry_attr3",
        attribute_str_4="retry_attr4"
    )
    
    with patch('data.dal.ddb_table_one_dal.table.put_item') as mock_put:
        error = ClientError(
            {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Exceeded'}},
            'PutItem'
        )
        mock_put.side_effect = [error, error, None]
        ddb_table_one_put_item(dto)
        assert mock_put.call_count == 3


def test_retry_mechanism_update(mock_dynamodb_table):
    """Test retry mechanism for update operation."""
    from unittest.mock import patch
    from botocore.exceptions import ClientError
    
    create_sample_items()
    dto = DdbTableOneDto(
        pk_attribute_str_1="pk1",
        sk_attribute_str_2="sk1",
        attribute_str_3="retry_update"
    )
    
    with patch('data.dal.ddb_table_one_dal.table.update_item') as mock_update:
        error = ClientError(
            {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Exceeded'}},
            'UpdateItem'
        )
        mock_update.side_effect = [error, error, None]
        ddb_table_one_update_item(dto)
        assert mock_update.call_count == 3


def test_retry_mechanism_delete(mock_dynamodb_table):
    """Test retry mechanism for delete operation."""
    from unittest.mock import patch
    from botocore.exceptions import ClientError
    
    create_sample_items()
    
    with patch('data.dal.ddb_table_one_dal.table.delete_item') as mock_delete:
        error = ClientError(
            {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Exceeded'}},
            'DeleteItem'
        )
        mock_delete.side_effect = [error, error, None]
        ddb_table_one_delete_item("pk1", "sk1")
        assert mock_delete.call_count == 3


def test_retry_mechanism_query(mock_dynamodb_table):
    """Test retry mechanism for query operation."""
    from unittest.mock import patch
    from botocore.exceptions import ClientError
    
    create_sample_items()
    
    with patch('data.dal.ddb_table_one_dal.table.query') as mock_query:
        error = ClientError(
            {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Exceeded'}},
            'Query'
        )
        mock_query.side_effect = [
            error,
            error,
            {'Items': [], 'Count': 0, 'ScannedCount': 0}
        ]
        results = ddb_table_one_query(hash_key="pk1")
        assert mock_query.call_count == 3


def test_query_base_table_with_filter_condition(mock_dynamodb_table):
    """Test base table query with filter condition."""
    create_sample_items()
    result = ddb_table_one_query_base_table(
        "pk1",
        filter_key_condition=Attr('attribute_num_5').gte(150)
    )
    assert result is not None
    assert all(dto.attribute_num_5 >= 150 for dto in result['items'])


def test_gsi_query_with_filter_condition(mock_dynamodb_table):
    """Test GSI query with filter condition."""
    create_sample_items()
    result = gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index_query(
        "gsi_pk1",
        filter_key_condition=Attr('attribute_num_5').lte(100)
    )
    assert result is not None
    assert all(dto.attribute_num_5 <= 100 for dto in result['items'])


def test_batch_write_with_ttl(mock_dynamodb_table):
    """Test batch write with TTL attribute."""
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
        retrieved = ddb_table_one_get_item(f"ttl_pk_{i}", "ttl_sk")
        assert retrieved is not None


def test_create_or_update_retry(mock_dynamodb_table):
    """Test retry mechanism for create_or_update operation."""
    from unittest.mock import patch
    from botocore.exceptions import ClientError
    
    dto = DdbTableOneDto(
        pk_attribute_str_1="retry_create_update",
        sk_attribute_str_2="retry_sk",
        attribute_str_3="retry_attr3",
        attribute_str_4="retry_attr4"
    )
    
    with patch('data.dal.ddb_table_one_dal.table.put_item') as mock_put:
        error = ClientError(
            {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Exceeded'}},
            'PutItem'
        )
        mock_put.side_effect = [error, error, None]
        ddb_table_one_create_or_update_item(dto)
        assert mock_put.call_count == 3


def test_query_with_last_evaluated_key(mock_dynamodb_table):
    """Test query pagination using last_evaluated_key."""
    for i in range(10):
        dto = DdbTableOneDto(
            pk_attribute_str_1="pagination_key",
            sk_attribute_str_2=f"sk{i:02d}",
            attribute_str_3=f"attr3_{i}",
            attribute_str_4=f"attr4_{i}",
            attribute_num_5=100 * i
        )
        ddb_table_one_put_item(dto)
    
    result = ddb_table_one_query_base_table("pagination_key")
    assert len(result['items']) == 10
