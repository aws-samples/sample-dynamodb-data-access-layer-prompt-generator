import pytest
import os
from moto import mock_aws
from datetime import datetime, timezone
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

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
    os.environ['DDB_TABLE_TWO'] = 'ddb_table_two'


@pytest.fixture
def mock_dynamodb_table(aws_credentials):
    """Create a mock DynamoDB table for testing."""
    with mock_aws():
        dynamodb = table.meta.client
        dynamodb.create_table(
            TableName='ddb_table_two',
            KeySchema=[
                {'AttributeName': 'pk_attr_str_1', 'KeyType': 'HASH'},
                {'AttributeName': 'sk_attr_str_2', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'pk_attr_str_1', 'AttributeType': 'S'},
                {'AttributeName': 'sk_attr_str_2', 'AttributeType': 'S'},
                {'AttributeName': 'gsi1pk_attr_str_7', 'AttributeType': 'S'},
                {'AttributeName': 'gsi1sk_attr_str_8', 'AttributeType': 'S'},
                {'AttributeName': 'gsi2pk_attr_str_9', 'AttributeType': 'S'},
                {'AttributeName': 'gsi2sk_attr_str_10', 'AttributeType': 'S'},
                {'AttributeName': 'gsi3pk_attr_str_11', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index',
                    'KeySchema': [
                        {'AttributeName': 'gsi1pk_attr_str_7', 'KeyType': 'HASH'},
                        {'AttributeName': 'gsi1sk_attr_str_8', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                },
                {
                    'IndexName': 'gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index',
                    'KeySchema': [
                        {'AttributeName': 'gsi2pk_attr_str_9', 'KeyType': 'HASH'},
                        {'AttributeName': 'gsi2sk_attr_str_10', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'KEYS_ONLY'}
                },
                {
                    'IndexName': 'gsi__gsi3pk_attr_str_11__index',
                    'KeySchema': [
                        {'AttributeName': 'gsi3pk_attr_str_11', 'KeyType': 'HASH'}
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
        DdbTableTwoDto(
            pk_attr_str_1="pk1",
            sk_attr_str_2="sk1",
            attr_str_3="attr3_val1",
            attr_str_4="attr4_val1",
            attr_str_5="attr5_val1",
            attr_str_6="attr6_val1",
            gsi1pk_attr_str_7="gsi1_pk1",
            gsi1sk_attr_str_8="gsi1_sk1",
            gsi2pk_attr_str_9="gsi2_pk1",
            gsi2sk_attr_str_10="gsi2_sk1",
            gsi3pk_attr_str_11="gsi3_pk1"
        ),
        DdbTableTwoDto(
            pk_attr_str_1="pk1",
            sk_attr_str_2="sk2",
            attr_str_3="attr3_val2",
            attr_str_4="attr4_val2",
            attr_str_5="attr5_val2",
            attr_str_6="attr6_val2",
            gsi1pk_attr_str_7="gsi1_pk1",
            gsi1sk_attr_str_8="gsi1_sk2",
            gsi2pk_attr_str_9="gsi2_pk1",
            gsi2sk_attr_str_10="gsi2_sk2",
            gsi3pk_attr_str_11="gsi3_pk1"
        ),
        DdbTableTwoDto(
            pk_attr_str_1="pk2",
            sk_attr_str_2="sk1",
            attr_str_3="attr3_val3",
            attr_str_4="attr4_val3",
            attr_str_5="attr5_val3",
            attr_str_6="attr6_val3",
            gsi1pk_attr_str_7="gsi1_pk2",
            gsi1sk_attr_str_8="gsi1_sk1",
            gsi2pk_attr_str_9="gsi2_pk2",
            gsi2sk_attr_str_10="gsi2_sk1",
            gsi3pk_attr_str_11="gsi3_pk2"
        )
    ]
    
    for dto in sample_dtos:
        ddb_table_two_put_item(dto)
    
    return sample_dtos


class TestDdbTableTwoDAL:
    """Test class for DdbTableTwo DAL methods."""

    def test_put_item_success(self, mock_dynamodb_table):
        """Test successful put operation."""
        dto = DdbTableTwoDto(
            pk_attr_str_1="test_pk",
            sk_attr_str_2="test_sk",
            attr_str_3="test_attr3",
            attr_str_4="test_attr4",
            attr_str_5="test_attr5",
            attr_str_6="test_attr6",
            gsi1pk_attr_str_7="test_gsi1_pk",
            gsi1sk_attr_str_8="test_gsi1_sk",
            gsi2pk_attr_str_9="test_gsi2_pk",
            gsi2sk_attr_str_10="test_gsi2_sk"
        )
        ddb_table_two_put_item(dto)
        result = ddb_table_two_get_item("test_pk", "test_sk")
        assert result is not None
        assert result.pk_attr_str_1 == "test_pk"

    def test_update_item_success(self, mock_dynamodb_table):
        """Test successful update operation."""
        create_sample_items()
        dto = DdbTableTwoDto(
            pk_attr_str_1="pk1",
            sk_attr_str_2="sk1",
            attr_str_3="updated_attr3",
            attr_str_5="updated_attr5"
        )
        ddb_table_two_update_item(dto)
        result = ddb_table_two_get_item("pk1", "sk1")
        assert result.attr_str_3 == "updated_attr3"
        assert result.attr_str_5 == "updated_attr5"

    def test_get_item_success(self, mock_dynamodb_table):
        """Test successful get operation."""
        create_sample_items()
        dto = ddb_table_two_get_item("pk1", "sk1")
        assert dto is not None
        assert dto.pk_attr_str_1 == "pk1"
        assert dto.attr_str_3 == "attr3_val1"

    def test_get_item_not_found(self, mock_dynamodb_table):
        """Test get operation for non-existent item."""
        dto = ddb_table_two_get_item("nonexistent", "nonexistent")
        assert dto is None

    def test_delete_item_success(self, mock_dynamodb_table):
        """Test successful delete operation."""
        create_sample_items()
        ddb_table_two_delete_item("pk1", "sk1")
        result = ddb_table_two_get_item("pk1", "sk1")
        assert result is None

    def test_create_or_update_item_create(self, mock_dynamodb_table):
        """Test create_or_update operation when item doesn't exist."""
        dto = DdbTableTwoDto(
            pk_attr_str_1="new_pk",
            sk_attr_str_2="new_sk",
            attr_str_3="new_attr3",
            attr_str_4="new_attr4",
            attr_str_5="new_attr5",
            attr_str_6="new_attr6",
            gsi1pk_attr_str_7="new_gsi1_pk",
            gsi1sk_attr_str_8="new_gsi1_sk",
            gsi2pk_attr_str_9="new_gsi2_pk",
            gsi2sk_attr_str_10="new_gsi2_sk"
        )
        ddb_table_two_create_or_update_item(dto)
        result = ddb_table_two_get_item("new_pk", "new_sk")
        assert result.attr_str_3 == "new_attr3"

    def test_create_or_update_item_update(self, mock_dynamodb_table):
        """Test create_or_update operation when item exists."""
        create_sample_items()
        dto = DdbTableTwoDto(
            pk_attr_str_1="pk1",
            sk_attr_str_2="sk1",
            attr_str_3="updated_value",
            attr_str_6="updated_attr6"
        )
        ddb_table_two_create_or_update_item(dto)
        result = ddb_table_two_get_item("pk1", "sk1")
        assert result.attr_str_3 == "updated_value"
        assert result.attr_str_6 == "updated_attr6"

    def test_query_base_table_all_items(self, mock_dynamodb_table):
        """Test querying base table for all items with a given partition key."""
        create_sample_items()
        results = ddb_table_two_query_base_table("pk1")
        assert len(results['items']) == 2
        assert all(dto.pk_attr_str_1 == "pk1" for dto in results['items'])

    def test_query_base_table_with_condition(self, mock_dynamodb_table):
        """Test querying base table with sort key condition."""
        create_sample_items()
        results = ddb_table_two_query_base_table(
            "pk1",
            Key('sk_attr_str_2').eq("sk1")
        )
        assert len(results['items']) == 1
        assert results['items'][0].sk_attr_str_2 == "sk1"

    def test_query_base_table_no_results(self, mock_dynamodb_table):
        """Test querying base table with no matching results."""
        create_sample_items()
        results = ddb_table_two_query_base_table("nonexistent")
        assert len(results['items']) == 0

    def test_batch_write_success(self, mock_dynamodb_table):
        """Test successful batch write operation."""
        dtos = [
            DdbTableTwoDto(
                pk_attr_str_1=f"batch_pk_{i}",
                sk_attr_str_2=f"batch_sk_{i}",
                attr_str_3=f"attr3_{i}",
                attr_str_4=f"attr4_{i}",
                attr_str_5=f"attr5_{i}",
                attr_str_6=f"attr6_{i}",
                gsi1pk_attr_str_7=f"gsi1_pk_{i}",
                gsi1sk_attr_str_8=f"gsi1_sk_{i}",
                gsi2pk_attr_str_9=f"gsi2_pk_{i}",
                gsi2sk_attr_str_10=f"gsi2_sk_{i}"
            )
            for i in range(1, 6)
        ]
        ddb_table_two_batch_write(dtos)
        
        for i in range(1, 6):
            result = ddb_table_two_get_item(f"batch_pk_{i}", f"batch_sk_{i}")
            assert result.attr_str_3 == f"attr3_{i}"

    def test_batch_get_success(self, mock_dynamodb_table):
        """Test successful batch get operation."""
        create_sample_items()
        keys = [
            {"pk_attr_str_1": "pk1", "sk_attr_str_2": "sk1"},
            {"pk_attr_str_1": "pk1", "sk_attr_str_2": "sk2"},
            {"pk_attr_str_1": "pk2", "sk_attr_str_2": "sk1"}
        ]
        results = ddb_table_two_batch_get(keys)
        assert len(results) == 3

    def test_gsi1_query_success(self, mock_dynamodb_table):
        """Test GSI1 query by gsi1pk_attr_str_7."""
        create_sample_items()
        results = gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index_query("gsi1_pk1")
        assert len(results['items']) == 2
        assert all(dto.gsi1pk_attr_str_7 == "gsi1_pk1" for dto in results['items'])

    def test_gsi1_query_with_sort_condition(self, mock_dynamodb_table):
        """Test GSI1 query with gsi1sk_attr_str_8 condition."""
        create_sample_items()
        results = gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index_query(
            "gsi1_pk1",
            Key('gsi1sk_attr_str_8').eq("gsi1_sk1")
        )
        assert len(results['items']) == 1
        assert results['items'][0].gsi1sk_attr_str_8 == "gsi1_sk1"

    def test_gsi1_query_no_results(self, mock_dynamodb_table):
        """Test GSI1 query with no matching results."""
        create_sample_items()
        results = gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index_query("nonexistent_gsi")
        assert len(results['items']) == 0

    def test_gsi2_query_success(self, mock_dynamodb_table):
        """Test GSI2 query by gsi2pk_attr_str_9."""
        create_sample_items()
        results = gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index_query("gsi2_pk1")
        assert len(results['items']) == 2
        assert all(dto.gsi2pk_attr_str_9 == "gsi2_pk1" for dto in results['items'])

    def test_gsi2_query_no_results(self, mock_dynamodb_table):
        """Test GSI2 query with no matching results."""
        create_sample_items()
        results = gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index_query("nonexistent_gsi")
        assert len(results['items']) == 0

    def test_gsi3_query_success(self, mock_dynamodb_table):
        """Test GSI3 query by gsi3pk_attr_str_11."""
        create_sample_items()
        results = gsi__gsi3pk_attr_str_11__index_query("gsi3_pk1")
        assert len(results['items']) == 2
        assert all(dto.gsi3pk_attr_str_11 == "gsi3_pk1" for dto in results['items'])

    def test_gsi3_query_no_results(self, mock_dynamodb_table):
        """Test GSI3 query with no matching results."""
        create_sample_items()
        results = gsi__gsi3pk_attr_str_11__index_query("nonexistent_gsi")
        assert len(results['items']) == 0

    def test_query_with_limit(self, mock_dynamodb_table):
        """Test querying with limit parameter."""
        create_sample_items()
        results = ddb_table_two_query(
            hash_key="pk1",
            query_limit=1
        )
        assert len(results) == 1

    def test_query_empty_result_nonexistent_key(self, mock_dynamodb_table):
        """Test query with non-existent hash key returns empty results."""
        create_sample_items()
        results = ddb_table_two_query(hash_key="nonexistent_key_12345")
        assert len(results) == 0

    def test_batch_get_empty_result(self, mock_dynamodb_table):
        """Test batch get with non-existent keys."""
        keys = [
            {"pk_attr_str_1": "nonexistent1", "sk_attr_str_2": "nonexistent1"},
            {"pk_attr_str_1": "nonexistent2", "sk_attr_str_2": "nonexistent2"}
        ]
        results = ddb_table_two_batch_get(keys)
        assert len(results) == 0

    def test_batch_get_partial_result(self, mock_dynamodb_table):
        """Test batch get with mix of existent and non-existent keys."""
        create_sample_items()
        keys = [
            {"pk_attr_str_1": "pk1", "sk_attr_str_2": "sk1"},
            {"pk_attr_str_1": "nonexistent", "sk_attr_str_2": "nonexistent"}
        ]
        results = ddb_table_two_batch_get(keys)
        assert len(results) == 1
        assert results[0].pk_attr_str_1 == "pk1"

    def test_retry_mechanism_put(self, mock_dynamodb_table):
        """Test retry mechanism for put operation."""
        from unittest.mock import patch
        from botocore.exceptions import ClientError
        
        dto = DdbTableTwoDto(
            pk_attr_str_1="retry_test",
            sk_attr_str_2="retry_sk",
            attr_str_3="retry_attr3",
            attr_str_4="retry_attr4",
            attr_str_5="retry_attr5",
            attr_str_6="retry_attr6",
            gsi1pk_attr_str_7="retry_gsi1_pk",
            gsi1sk_attr_str_8="retry_gsi1_sk",
            gsi2pk_attr_str_9="retry_gsi2_pk",
            gsi2sk_attr_str_10="retry_gsi2_sk"
        )
        
        with patch('data.dal.ddb_table_two_dal.table.put_item') as mock_put:
            error = ClientError(
                {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Exceeded'}},
                'PutItem'
            )
            mock_put.side_effect = [error, error, None]
            ddb_table_two_put_item(dto)
            assert mock_put.call_count == 3

    def test_retry_mechanism_update(self, mock_dynamodb_table):
        """Test retry mechanism for update operation."""
        from unittest.mock import patch
        from botocore.exceptions import ClientError
        
        create_sample_items()
        dto = DdbTableTwoDto(
            pk_attr_str_1="pk1",
            sk_attr_str_2="sk1",
            attr_str_3="retry_update"
        )
        
        with patch('data.dal.ddb_table_two_dal.table.update_item') as mock_update:
            error = ClientError(
                {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Exceeded'}},
                'UpdateItem'
            )
            mock_update.side_effect = [error, error, None]
            ddb_table_two_update_item(dto)
            assert mock_update.call_count == 3

    def test_retry_mechanism_delete(self, mock_dynamodb_table):
        """Test retry mechanism for delete operation."""
        from unittest.mock import patch
        from botocore.exceptions import ClientError
        
        create_sample_items()
        
        with patch('data.dal.ddb_table_two_dal.table.delete_item') as mock_delete:
            error = ClientError(
                {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Exceeded'}},
                'DeleteItem'
            )
            mock_delete.side_effect = [error, error, None]
            ddb_table_two_delete_item("pk1", "sk1")
            assert mock_delete.call_count == 3

    def test_retry_mechanism_query(self, mock_dynamodb_table):
        """Test retry mechanism for query operation."""
        from unittest.mock import patch
        from botocore.exceptions import ClientError
        
        create_sample_items()
        
        with patch('data.dal.ddb_table_two_dal.table.query') as mock_query:
            error = ClientError(
                {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Exceeded'}},
                'Query'
            )
            mock_query.side_effect = [
                error,
                error,
                {'Items': [], 'Count': 0, 'ScannedCount': 0}
            ]
            results = ddb_table_two_query(hash_key="pk1")
            assert mock_query.call_count == 3

    def test_query_base_table_with_filter_condition(self, mock_dynamodb_table):
        """Test base table query with filter condition."""
        create_sample_items()
        result = ddb_table_two_query_base_table(
            "pk1",
            filter_key_condition=Attr('attr_str_3').eq('attr3_val1')
        )
        assert result is not None
        assert all(dto.attr_str_3 == 'attr3_val1' for dto in result['items'])

    def test_gsi1_query_with_filter_condition(self, mock_dynamodb_table):
        """Test GSI1 query with filter condition."""
        create_sample_items()
        result = gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index_query(
            "gsi1_pk1",
            filter_key_condition=Attr('attr_str_3').eq('attr3_val1')
        )
        assert result is not None
        assert all(dto.attr_str_3 == 'attr3_val1' for dto in result['items'])

    def test_gsi2_query_with_sort_condition(self, mock_dynamodb_table):
        """Test GSI2 query with sort key condition."""
        create_sample_items()
        results = gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index_query(
            "gsi2_pk1",
            Key('gsi2sk_attr_str_10').eq("gsi2_sk1")
        )
        assert len(results['items']) == 1
        assert results['items'][0].gsi2sk_attr_str_10 == "gsi2_sk1"

    def test_gsi2_query_with_filter_condition(self, mock_dynamodb_table):
        """Test GSI2 query with filter condition."""
        create_sample_items()
        result = gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index_query(
            "gsi2_pk1",
            filter_key_condition=Attr('attr_str_3').eq('attr3_val2')
        )
        assert result is not None
        assert all(dto.attr_str_3 == 'attr3_val2' for dto in result['items'])

    def test_gsi3_query_with_filter_condition(self, mock_dynamodb_table):
        """Test GSI3 query with filter condition."""
        create_sample_items()
        result = gsi__gsi3pk_attr_str_11__index_query(
            "gsi3_pk1",
            filter_key_condition=Attr('attr_str_4').eq('attr4_val1')
        )
        assert result is not None
        assert all(dto.attr_str_4 == 'attr4_val1' for dto in result['items'])

    def test_batch_write_with_ttl(self, mock_dynamodb_table):
        """Test batch write with TTL attribute."""
        dtos = [
            DdbTableTwoDto(
                pk_attr_str_1=f"ttl_pk_{i}",
                sk_attr_str_2="ttl_sk",
                attr_str_3="ttl_attr3",
                attr_str_4="ttl_attr4",
                attr_str_5="ttl_attr5",
                attr_str_6="ttl_attr6",
                gsi1pk_attr_str_7="ttl_gsi1_pk",
                gsi1sk_attr_str_8="ttl_gsi1_sk",
                gsi2pk_attr_str_9="ttl_gsi2_pk",
                gsi2sk_attr_str_10="ttl_gsi2_sk"
            )
            for i in range(3)
        ]
        ddb_table_two_batch_write(dtos)
        
        for i in range(3):
            retrieved = ddb_table_two_get_item(f"ttl_pk_{i}", "ttl_sk")
            assert retrieved is not None

    def test_create_or_update_retry(self, mock_dynamodb_table):
        """Test retry mechanism for create_or_update operation."""
        from unittest.mock import patch
        from botocore.exceptions import ClientError
        
        dto = DdbTableTwoDto(
            pk_attr_str_1="retry_create_update",
            sk_attr_str_2="retry_sk",
            attr_str_3="retry_attr3",
            attr_str_4="retry_attr4",
            attr_str_5="retry_attr5",
            attr_str_6="retry_attr6",
            gsi1pk_attr_str_7="retry_gsi1_pk",
            gsi1sk_attr_str_8="retry_gsi1_sk",
            gsi2pk_attr_str_9="retry_gsi2_pk",
            gsi2sk_attr_str_10="retry_gsi2_sk"
        )
        
        with patch('data.dal.ddb_table_two_dal.table.put_item') as mock_put:
            error = ClientError(
                {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Exceeded'}},
                'PutItem'
            )
            mock_put.side_effect = [error, error, None]
            ddb_table_two_create_or_update_item(dto)
            assert mock_put.call_count == 3

    def test_query_with_last_evaluated_key(self, mock_dynamodb_table):
        """Test query pagination using last_evaluated_key."""
        for i in range(10):
            dto = DdbTableTwoDto(
                pk_attr_str_1="pagination_key",
                sk_attr_str_2=f"sk{i:02d}",
                attr_str_3=f"attr3_{i}",
                attr_str_4=f"attr4_{i}",
                attr_str_5=f"attr5_{i}",
                attr_str_6=f"attr6_{i}",
                gsi1pk_attr_str_7="pagination_gsi1_pk",
                gsi1sk_attr_str_8=f"gsi1_sk{i:02d}",
                gsi2pk_attr_str_9="pagination_gsi2_pk",
                gsi2sk_attr_str_10=f"gsi2_sk{i:02d}"
            )
            ddb_table_two_put_item(dto)
        
        result = ddb_table_two_query_base_table("pagination_key")
        assert len(result['items']) == 10

    def test_dto_str_method(self):
        """Test DTO string representation."""
        dto = DdbTableTwoDto(
            pk_attr_str_1="test",
            sk_attr_str_2="test_sk",
            attr_str_3="attr3",
            attr_str_4="attr4",
            attr_str_5="attr5",
            attr_str_6="attr6",
            gsi1pk_attr_str_7="gsi1_pk",
            gsi1sk_attr_str_8="gsi1_sk"
        )
        dto_str = str(dto)
        assert '"pk_attr_str_1": "test"' in dto_str
        assert '"attr_str_3": "attr3"' in dto_str
        assert '"gsi1pk_attr_str_7": "gsi1_pk"' in dto_str

    def test_query_with_range_key_condition(self, mock_dynamodb_table):
        """Test query with range key condition."""
        create_sample_items()
        results = ddb_table_two_query(
            hash_key="pk1",
            range_key_and_condition=Key('sk_attr_str_2').begins_with('sk1')
        )
        assert len(results) == 1
        assert results[0].sk_attr_str_2 == "sk1"

    def test_query_scan_index_forward_false(self, mock_dynamodb_table):
        """Test query with descending sort order."""
        create_sample_items()
        results_asc = ddb_table_two_query(
            hash_key="pk1",
            scan_index_forward=True
        )
        results_desc = ddb_table_two_query(
            hash_key="pk1",
            scan_index_forward=False
        )
        assert len(results_asc) == len(results_desc) == 2
        if len(results_asc) > 1:
            assert results_asc[0].sk_attr_str_2 != results_desc[0].sk_attr_str_2

    def test_query_with_consistent_read(self, mock_dynamodb_table):
        """Test query with consistent read enabled."""
        create_sample_items()
        results = ddb_table_two_query(
            hash_key="pk1",
            consistent_read=True
        )
        assert len(results) >= 1
        assert all(dto.pk_attr_str_1 == "pk1" for dto in results)

    def test_query_with_attributes_to_get(self, mock_dynamodb_table):
        """Test query with projection expression."""
        create_sample_items()
        results = ddb_table_two_query(
            hash_key="pk1",
            attribute_to_get=['pk_attr_str_1', 'sk_attr_str_2', 'attr_str_3']
        )
        assert len(results) >= 1
        for dto in results:
            assert dto.pk_attr_str_1 is not None
            assert dto.attr_str_3 is not None

    def test_query_with_index_name(self, mock_dynamodb_table):
        """Test query using GSI index."""
        create_sample_items()
        results = ddb_table_two_query(
            hash_key="gsi1_pk1",
            index_name="gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index"
        )
        assert len(results) >= 1
        assert all(dto.gsi1pk_attr_str_7 == "gsi1_pk1" for dto in results)

    def test_query_with_filter_condition(self, mock_dynamodb_table):
        """Test query with filter condition."""
        create_sample_items()
        results = ddb_table_two_query(
            hash_key="pk1",
            filter_key_condition=Attr('attr_str_3').eq('attr3_val1')
        )
        assert len(results) == 1
        assert all(dto.attr_str_3 == 'attr3_val1' for dto in results)
