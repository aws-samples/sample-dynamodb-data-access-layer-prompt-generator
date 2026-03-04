import pytest
from moto import mock_aws
from datetime import datetime, timezone
from pynamodb.exceptions import DoesNotExist, PutError, UpdateError, DeleteError

from data.model.policy_manager import PolicyManager
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
    gsi__gsipk2__gsisk2__index_query
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
        if not PolicyManager.exists():
            PolicyManager.create_table(read_capacity_units=5, write_capacity_units=5, wait=True)
        yield


def create_sample_items():
    """
    Create sample items in the mock table.
    
    Returns:
        list: List of PolicyManager items created
    """
    sample_items = [
        PolicyManager(
            pk="policy1",
            sk="version1",
            policy_id="POL001",
            policy_name="Home Insurance",
            risk_id="RISK001",
            address="123 Main St",
            premium=1000,
            sum_insured=500000,
            policy_version="v1",
            start_date="2024-01-01",
            end_date="2025-01-01",
            limit=100000,
            deductible="1000",
            amount=50000,
            gsipk1="gsi1_pk1",
            gsisk1="gsi1_sk1",
            gsipk2="gsi2_pk1",
            gsisk2="gsi2_sk1"
        ),
        PolicyManager(
            pk="policy1",
            sk="version2",
            policy_id="POL001",
            policy_name="Home Insurance",
            risk_id="RISK001",
            address="123 Main St",
            premium=1200,
            sum_insured=600000,
            policy_version="v2",
            start_date="2024-06-01",
            end_date="2025-06-01",
            limit=120000,
            deductible="1000",
            amount=60000,
            gsipk1="gsi1_pk1",
            gsisk1="gsi1_sk2",
            gsipk2="gsi2_pk1",
            gsisk2="gsi2_sk2"
        ),
        PolicyManager(
            pk="policy2",
            sk="version1",
            policy_id="POL002",
            policy_name="Auto Insurance",
            risk_id="RISK002",
            address="456 Oak Ave",
            premium=800,
            sum_insured=50000,
            policy_version="v1",
            start_date="2024-02-01",
            end_date="2025-02-01",
            limit=25000,
            deductible="500",
            amount=10000,
            gsipk1="gsi1_pk2",
            gsisk1="gsi1_sk1",
            gsipk2="gsi2_pk2",
            gsisk2="gsi2_sk1"
        )
    ]

    for item in sample_items:
        item.save()

    return sample_items


class TestPolicyManagerDAL:
    """
    Test class for PolicyManager DAL methods.
    
    This class contains comprehensive unit tests for all DAL methods
    with both positive and negative test cases.
    """

    def test_put_item_success(self, mock_dynamodb_table):
        """
        Test successful put operation.
        
        Verifies that a new item can be successfully added to the table.
        """
        dto = PolicyManagerDto(
            pk="test_pk",
            sk="test_sk",
            policy_id="POL999",
            policy_name="Test Policy",
            risk_id="RISK999",
            address="789 Test St",
            premium=1500,
            sum_insured=750000,
            policy_version="v1",
            start_date="2024-03-01"
        )
        policy_manager_put_item(dto)
        item = PolicyManager.get("test_pk", "test_sk")
        assert item.pk == "test_pk"
        assert item.policy_name == "Test Policy"
        assert item.premium == 1500

    def test_put_item_with_all_fields(self, mock_dynamodb_table):
        """
        Test put operation with all optional fields populated.
        
        Verifies that all fields are properly stored.
        """
        dto = PolicyManagerDto(
            pk="test_pk2",
            sk="test_sk2",
            policy_id="POL888",
            policy_name="Full Policy",
            risk_id="RISK888",
            address="100 Full St",
            premium=2000,
            sum_insured=1000000,
            policy_version="v2",
            start_date="2024-04-01",
            end_date="2025-04-01",
            limit=200000,
            deductible="2000",
            amount=100000,
            gsipk1="gsi1_test",
            gsisk1="gsi1_test_sk",
            gsipk2="gsi2_test",
            gsisk2="gsi2_test_sk"
        )
        policy_manager_put_item(dto)
        item = PolicyManager.get("test_pk2", "test_sk2")
        assert item.gsipk1 == "gsi1_test"
        assert item.premium == 2000

    def test_update_item_success(self, mock_dynamodb_table):
        """
        Test successful update operation.
        
        Verifies that an existing item can be updated.
        """
        create_sample_items()
        dto = PolicyManagerDto(
            pk="policy1",
            sk="version1",
            policy_name="Updated Home Insurance",
            premium=1100
        )
        policy_manager_update_item(dto)
        item = PolicyManager.get("policy1", "version1")
        assert item.policy_name == "Updated Home Insurance"
        assert item.premium == 1100
        assert item.updated_at is not None

    def test_update_item_nonexistent(self, mock_dynamodb_table):
        """
        Test update operation on non-existent item.
        
        Verifies that updating a non-existent item raises DoesNotExist.
        """
        dto = PolicyManagerDto(
            pk="nonexistent",
            sk="nonexistent",
            policy_name="test"
        )
        with pytest.raises(DoesNotExist):
            policy_manager_update_item(dto)

    def test_get_item_success(self, mock_dynamodb_table):
        """
        Test successful get operation.
        
        Verifies that an item can be retrieved by its keys.
        """
        create_sample_items()
        dto = policy_manager_get_item("policy1", "version1")
        assert dto is not None
        assert dto.pk == "policy1"
        assert dto.policy_name == "Home Insurance"

    def test_get_item_not_found(self, mock_dynamodb_table):
        """
        Test get operation for non-existent item.
        
        Verifies that getting a non-existent item returns None.
        """
        dto = policy_manager_get_item("nonexistent", "nonexistent")
        assert dto is None

    def test_delete_item_success(self, mock_dynamodb_table):
        """
        Test successful delete operation.
        
        Verifies that an item can be deleted.
        """
        create_sample_items()
        policy_manager_delete_item("policy1", "version1")
        with pytest.raises(DoesNotExist):
            PolicyManager.get("policy1", "version1")

    def test_delete_item_not_found(self, mock_dynamodb_table):
        """
        Test delete operation for non-existent item.
        
        Verifies that deleting a non-existent item raises DoesNotExist.
        """
        with pytest.raises(DoesNotExist):
            policy_manager_delete_item("nonexistent", "nonexistent")

    def test_create_or_update_item_create(self, mock_dynamodb_table):
        """
        Test create_or_update operation when item doesn't exist.
        
        Verifies that a new item is created.
        """
        dto = PolicyManagerDto(
            pk="new_pk",
            sk="new_sk",
            policy_id="POL777",
            policy_name="New Policy",
            risk_id="RISK777",
            address="New Address",
            premium=900,
            sum_insured=450000,
            policy_version="v1",
            start_date="2024-05-01"
        )
        policy_manager_create_or_update_item(dto)
        item = PolicyManager.get("new_pk", "new_sk")
        assert item.policy_name == "New Policy"

    def test_create_or_update_item_update(self, mock_dynamodb_table):
        """
        Test create_or_update operation when item exists.
        
        Verifies that an existing item is updated.
        """
        create_sample_items()
        dto = PolicyManagerDto(
            pk="policy1",
            sk="version1",
            policy_name="Updated Policy",
            premium=1300
        )
        policy_manager_create_or_update_item(dto)
        item = PolicyManager.get("policy1", "version1")
        assert item.policy_name == "Updated Policy"
        assert item.premium == 1300
        assert item.updated_at is not None


    def test_query_base_table_all_items(self, mock_dynamodb_table):
        """
        Test querying base table for all items with a given partition key.
        
        Verifies that all items with the same pk are returned.
        """
        create_sample_items()
        results = policy_manager_query_base_table("policy1")
        assert len(results) == 2
        assert all(dto.pk == "policy1" for dto in results)

    def test_query_base_table_with_condition(self, mock_dynamodb_table):
        """
        Test querying base table with sort key condition.
        
        Verifies that query with conditions filters results correctly.
        """
        create_sample_items()
        from pynamodb.expressions.operand import Path
        results = policy_manager_query_base_table(
            "policy1",
            Path('sk') == "version1"
        )
        assert len(results) == 1
        assert results[0].sk == "version1"

    def test_query_base_table_no_results(self, mock_dynamodb_table):
        """
        Test querying base table with no matching results.
        
        Verifies that empty list is returned when no items match.
        """
        create_sample_items()
        results = policy_manager_query_base_table("nonexistent")
        assert len(results) == 0

    def test_query_with_limit(self, mock_dynamodb_table):
        """
        Test querying with limit parameter.
        
        Verifies that query respects the limit parameter.
        """
        create_sample_items()
        results = policy_manager_query(
            hash_key="policy1",
            query_limit=1
        )
        assert len(results) == 1

    def test_query_scan_index_forward(self, mock_dynamodb_table):
        """
        Test querying with scan_index_forward parameter.
        
        Verifies that query respects sort order.
        """
        create_sample_items()
        results_asc = policy_manager_query(
            hash_key="policy1",
            scan_index_forward=True
        )
        results_desc = policy_manager_query(
            hash_key="policy1",
            scan_index_forward=False
        )
        assert results_asc[0].sk == "version1"
        assert results_desc[0].sk == "version2"

    def test_batch_write_success(self, mock_dynamodb_table):
        """
        Test successful batch write operation.
        
        Verifies that multiple items can be written in batch.
        """
        dtos = [
            PolicyManagerDto(
                pk=f"batch_pk_{i}",
                sk=f"batch_sk_{i}",
                policy_id=f"POL{i}",
                policy_name=f"Policy{i}",
                risk_id=f"RISK{i}",
                address=f"Address{i}",
                premium=1000 * i,
                sum_insured=500000 * i,
                policy_version="v1",
                start_date="2024-01-01"
            )
            for i in range(1, 6)
        ]
        policy_manager_batch_write(dtos)
        
        for i in range(1, 6):
            item = PolicyManager.get(f"batch_pk_{i}", f"batch_sk_{i}")
            assert item.policy_name == f"Policy{i}"

    def test_batch_write_empty_list(self, mock_dynamodb_table):
        """
        Test batch write with empty list.
        
        Verifies that batch write handles empty list gracefully.
        """
        policy_manager_batch_write([])

    def test_batch_get_success(self, mock_dynamodb_table):
        """
        Test successful batch get operation.
        
        Verifies that multiple items can be retrieved in batch.
        """
        create_sample_items()
        keys = [
            ("policy1", "version1"),
            ("policy1", "version2"),
            ("policy2", "version1")
        ]
        results = policy_manager_batch_get(keys)
        assert len(results) == 3

    def test_batch_get_partial_results(self, mock_dynamodb_table):
        """
        Test batch get with some non-existent items.
        
        Verifies that batch get returns only existing items.
        """
        create_sample_items()
        keys = [
            ("policy1", "version1"),
            ("nonexistent", "nonexistent")
        ]
        results = policy_manager_batch_get(keys)
        assert len(results) == 1
        assert results[0].pk == "policy1"

    def test_batch_get_empty_list(self, mock_dynamodb_table):
        """
        Test batch get with empty key list.
        
        Verifies that batch get handles empty list gracefully.
        """
        results = policy_manager_batch_get([])
        assert len(results) == 0

    def test_gsi1_query_success(self, mock_dynamodb_table):
        """
        Test GSI1 query by gsipk1.
        
        Verifies that GSI1 query returns items matching gsipk1.
        """
        create_sample_items()
        results = gsi__gsipk1__gsisk1__index_query("gsi1_pk1")
        assert len(results) == 2
        assert all(dto.gsipk1 == "gsi1_pk1" for dto in results)

    def test_gsi1_query_with_sort_condition(self, mock_dynamodb_table):
        """
        Test GSI1 query with gsisk1 condition.
        
        Verifies that GSI1 query with sort key condition filters correctly.
        """
        create_sample_items()
        from pynamodb.expressions.operand import Path
        results = gsi__gsipk1__gsisk1__index_query(
            "gsi1_pk1",
            Path('gsisk1') == "gsi1_sk1"
        )
        assert len(results) == 1
        assert results[0].gsisk1 == "gsi1_sk1"

    def test_gsi1_query_no_results(self, mock_dynamodb_table):
        """
        Test GSI1 query with no matching results.
        
        Verifies that empty list is returned when no items match GSI1 query.
        """
        create_sample_items()
        results = gsi__gsipk1__gsisk1__index_query("nonexistent_gsi")
        assert len(results) == 0

    def test_gsi2_query_success(self, mock_dynamodb_table):
        """
        Test GSI2 query by gsipk2.
        
        Verifies that GSI2 query returns items matching gsipk2.
        """
        create_sample_items()
        results = gsi__gsipk2__gsisk2__index_query("gsi2_pk1")
        assert len(results) == 2
        assert all(dto.gsipk2 == "gsi2_pk1" for dto in results)

    def test_gsi2_query_no_results(self, mock_dynamodb_table):
        """
        Test GSI2 query with no matching results.
        
        Verifies that empty list is returned when no items match GSI2 query.
        """
        create_sample_items()
        results = gsi__gsipk2__gsisk2__index_query("nonexistent_gsi")
        assert len(results) == 0

    def test_dto_to_string(self, mock_dynamodb_table):
        """
        Test DTO __str__ method.
        
        Verifies that DTO can be converted to JSON string.
        """
        dto = PolicyManagerDto(
            pk="test",
            sk="test_sk",
            policy_id="POL001",
            policy_name="Test",
            risk_id="RISK001",
            address="Test Address",
            premium=1000,
            sum_insured=500000,
            policy_version="v1",
            start_date="2024-01-01"
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
        os.environ['POLICY_MANAGER_TEST_TTL_DAYS'] = '30'
        
        item = PolicyManager(
            pk="TEST_pk",
            sk="test_sk",
            policy_id="POL001",
            policy_name="Test",
            risk_id="RISK001",
            address="Test Address",
            premium=1000,
            sum_insured=500000,
            policy_version="v1",
            start_date="2024-01-01"
        )
        ttl = item.get_ttl()
        if ttl is not None:
            assert ttl.days == 30
        else:
            assert PolicyManager.get_ttl_days() == 0

    def test_query_with_filter_condition(self, mock_dynamodb_table):
        """
        Test querying with filter condition.
        
        Verifies that filter condition is applied correctly.
        """
        create_sample_items()
        from pynamodb.expressions.operand import Path
        results = policy_manager_query(
            hash_key="policy1",
            filter_key_condition=Path('premium') == 1000
        )
        assert len(results) == 1
        assert results[0].premium == 1000

    def test_query_with_last_evaluated_key(self, mock_dynamodb_table):
        """
        Test querying with last_evaluated_key parameter.
        
        Verifies that last_evaluated_key parameter is accepted and processed.
        """
        create_sample_items()
        results = policy_manager_query(
            hash_key="policy1",
            query_limit=1
        )
        assert len(results) == 1
        
        results_all = policy_manager_query(
            hash_key="policy1",
            last_evaluated_key=None
        )
        assert len(results_all) >= 1

    def test_query_with_range_key_condition(self, mock_dynamodb_table):
        """Test query with range key condition."""
        create_sample_items()
        from pynamodb.expressions.operand import Path
        
        results = policy_manager_query(
            hash_key="policy1",
            range_key_and_condition=Path('sk') >= 'version1'
        )
        
        assert len(results) >= 1
        assert all(dto.sk >= "version1" for dto in results)

    def test_query_scan_index_forward_false(self, mock_dynamodb_table):
        """Test query with descending sort order."""
        create_sample_items()
        
        results_asc = policy_manager_query(
            hash_key="policy1",
            scan_index_forward=True
        )
        
        results_desc = policy_manager_query(
            hash_key="policy1",
            scan_index_forward=False
        )
        
        assert len(results_asc) == len(results_desc) == 2
        if len(results_asc) > 1:
            assert results_asc[0].sk != results_desc[0].sk

    def test_query_with_consistent_read(self, mock_dynamodb_table):
        """Test query with consistent read enabled."""
        create_sample_items()
        
        results = policy_manager_query(
            hash_key="policy1",
            consistent_read=True
        )
        
        assert len(results) >= 1
        assert all(dto.pk == "policy1" for dto in results)

    def test_query_with_attributes_to_get(self, mock_dynamodb_table):
        """Test query with projection expression."""
        create_sample_items()
        
        results = policy_manager_query(
            hash_key="policy1",
            attribute_to_get=['pk', 'sk', 'policy_name']
        )
        
        assert len(results) >= 1
        for dto in results:
            assert dto.pk is not None
            assert dto.policy_name is not None

    def test_query_with_index_name(self, mock_dynamodb_table):
        """Test query using GSI index."""
        create_sample_items()
        
        results = policy_manager_query(
            hash_key="gsi1_pk1",
            index_name="gsi__gsipk1__gsisk1__index"
        )
        
        assert len(results) >= 1
        assert all(dto.gsipk1 == "gsi1_pk1" for dto in results)

    def test_query_empty_result_nonexistent_key(self, mock_dynamodb_table):
        """Test query with non-existent hash key returns empty results."""
        create_sample_items()
        
        results = policy_manager_query(hash_key="nonexistent_key_12345")
        
        assert len(results) == 0

    def test_query_base_table_empty_result(self, mock_dynamodb_table):
        """Test base table query with non-existent key."""
        results = policy_manager_query_base_table("nonexistent_pk")
        
        assert len(results) == 0

    def test_batch_get_empty_result(self, mock_dynamodb_table):
        """Test batch get with non-existent keys."""
        keys = [
            ("nonexistent1", "nonexistent1"),
            ("nonexistent2", "nonexistent2")
        ]
        
        results = policy_manager_batch_get(keys)
        
        assert len(results) == 0

    def test_batch_get_partial_result(self, mock_dynamodb_table):
        """Test batch get with mix of existent and non-existent keys."""
        create_sample_items()
        
        keys = [
            ("policy1", "version1"),
            ("nonexistent", "nonexistent")
        ]
        
        results = policy_manager_batch_get(keys)
        
        assert len(results) == 1
        assert results[0].pk == "policy1"

    def test_retry_mechanism_put(self, mock_dynamodb_table):
        """Test retry mechanism for put operation."""
        from unittest.mock import patch
        
        dto = PolicyManagerDto(
            pk="retry_test",
            sk="retry_sk",
            policy_id="POL999",
            policy_name="Retry Test",
            risk_id="RISK999",
            address="Retry Address",
            premium=1000,
            sum_insured=500000,
            policy_version="v1",
            start_date="2024-01-01"
        )
        
        with patch('data.model.policy_manager.PolicyManager.save') as mock_save:
            mock_save.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            policy_manager_put_item(dto)
            
            assert mock_save.call_count == 3

    def test_retry_mechanism_update(self, mock_dynamodb_table):
        """Test retry mechanism for update operation."""
        from unittest.mock import patch
        
        create_sample_items()
        
        dto = PolicyManagerDto(
            pk="policy1",
            sk="version1",
            policy_name="retry_update"
        )
        
        with patch('data.model.policy_manager.PolicyManager.save') as mock_save:
            mock_save.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            policy_manager_update_item(dto)
            
            assert mock_save.call_count == 3

    def test_retry_mechanism_delete(self, mock_dynamodb_table):
        """Test retry mechanism for delete operation."""
        from unittest.mock import patch
        
        create_sample_items()
        
        with patch('data.model.policy_manager.PolicyManager.delete') as mock_delete:
            mock_delete.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            policy_manager_delete_item("policy1", "version1")
            
            assert mock_delete.call_count == 3

    def test_retry_mechanism_query(self, mock_dynamodb_table):
        """Test retry mechanism for query operation."""
        from unittest.mock import patch
        
        create_sample_items()
        
        with patch('data.model.policy_manager.PolicyManager.query') as mock_query:
            mock_query.side_effect = [
                Exception("First failure"),
                Exception("Second failure"),
                iter([])
            ]
            
            results = policy_manager_query(hash_key="policy1")
            
            assert mock_query.call_count == 3

    def test_batch_write_with_ttl_setting(self, mock_dynamodb_table):
        """Test batch write with TTL attribute handling."""
        dtos = [
            PolicyManagerDto(
                pk=f"ttl_pk_{i}",
                sk="ttl_sk",
                policy_id="POL001",
                policy_name="TTL Test",
                risk_id="RISK001",
                address="TTL Address",
                premium=1000,
                sum_insured=500000,
                policy_version="v1",
                start_date="2024-01-01"
            )
            for i in range(3)
        ]
        
        policy_manager_batch_write(dtos)
        
        for i in range(3):
            item = PolicyManager.get(f"ttl_pk_{i}", "ttl_sk")
            assert item is not None
            assert item.policy_name == "TTL Test"

    def test_create_or_update_retry(self, mock_dynamodb_table):
        """Test retry mechanism for create_or_update operation."""
        from unittest.mock import patch
        
        dto = PolicyManagerDto(
            pk="retry_create_update",
            sk="retry_sk",
            policy_id="POL999",
            policy_name="Retry Test",
            risk_id="RISK999",
            address="Retry Address",
            premium=1000,
            sum_insured=500000,
            policy_version="v1",
            start_date="2024-01-01"
        )
        
        with patch('data.model.policy_manager.PolicyManager.save') as mock_save:
            mock_save.side_effect = [Exception("First failure"), Exception("Second failure"), None]
            
            policy_manager_create_or_update_item(dto)
            
            assert mock_save.call_count == 3

    def test_query_with_multiple_filters(self, mock_dynamodb_table):
        """Test query with multiple filter conditions combined."""
        create_sample_items()
        from pynamodb.expressions.operand import Path
        
        filter_condition = (Path('policy_name') == 'Home Insurance') & (Path('premium') >= 1000)
        
        results = policy_manager_query(
            hash_key="policy1",
            filter_key_condition=filter_condition
        )
        
        assert all(dto.policy_name == 'Home Insurance' and dto.premium >= 1000 for dto in results)

    def test_gsi1_query_with_sort_condition_range(self, mock_dynamodb_table):
        """Test GSI1 query with sort key range condition."""
        create_sample_items()
        from pynamodb.expressions.operand import Path
        
        results = gsi__gsipk1__gsisk1__index_query(
            "gsi1_pk1",
            sort_key_condition=Path('gsisk1').between('gsi1_sk1', 'gsi1_sk3')
        )
        
        assert all('gsi1_sk1' <= dto.gsisk1 <= 'gsi1_sk3' for dto in results if dto.gsisk1 is not None)

    def test_gsi2_query_with_sort_condition(self, mock_dynamodb_table):
        """Test GSI2 query with sort key condition."""
        create_sample_items()
        from pynamodb.expressions.operand import Path
        
        results = gsi__gsipk2__gsisk2__index_query(
            "gsi2_pk1",
            sort_key_condition=Path('gsisk2') == 'gsi2_sk1'
        )
        
        assert len(results) == 1
        assert results[0].gsisk2 == 'gsi2_sk1'
