import os
from datetime import datetime, timedelta, timezone
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute, TTLAttribute, VersionAttribute
from pynamodb.indexes import GlobalSecondaryIndex, LocalSecondaryIndex, IncludeProjection, KeysOnlyProjection


class PolicyManager(Model):
    """
    PynamoDB model for PolicyManager DynamoDB table.
    
    This class represents the PolicyManager DynamoDB table and provides
    the schema definition including primary keys, attributes, GSIs, and LSIs.
    
    Constructor Arguments:
        pk (str): Primary partition/hash key
        sk (str): Sort/range key
        policy_id (str, optional): Policy ID
        policy_name (str, optional): Policy name
        risk_id (str, optional): Risk ID
        address (str, optional): Address
        premium (int, optional): Premium amount
        sum_insured (int, optional): Sum insured
        policy_version (str, optional): Policy version
        start_date (str, optional): Start date
        end_date (str, optional): End date
        limit (int, optional): Limit
        deductible (str, optional): Deductible
        amount (int, optional): Amount
        gsipk1 (str, optional): GSI1 partition key
        gsisk1 (str, optional): GSI1 sort key
        gsipk2 (str, optional): GSI2 partition key
        gsisk2 (str, optional): GSI2 sort key
        
    Attributes:
        All constructor arguments plus:
        created_at (str): Timestamp of creation
        updated_at (str): Timestamp of last update
        time_to_live (TTLAttribute): TTL for automatic deletion
        version (VersionAttribute): Version number for optimistic locking
        gsipk1_gsisk1_index: GSI for querying by gsipk1 and gsisk1
        gsipk2_gsisk2_index: GSI for querying by gsipk2 and gsisk2
        lsi_policy_version_index: LSI for querying by pk and policy_version
    """
    
    class Meta:
        table_name = os.environ.get('POLICY_MANAGER', 'PolicyManager')
        region = os.environ.get('AWS_REGION', 'us-east-1')
        billing_mode = 'PAY_PER_REQUEST'

    # Primary keys
    pk = UnicodeAttribute(hash_key=True, null=False)
    sk = UnicodeAttribute(range_key=True, null=False)

    # Attributes
    policy_id = UnicodeAttribute(null=True)
    policy_name = UnicodeAttribute(null=True)
    risk_id = UnicodeAttribute(null=True)
    address = UnicodeAttribute(null=True)
    premium = NumberAttribute(null=True)
    sum_insured = NumberAttribute(null=True)
    policy_version = UnicodeAttribute(null=True)
    start_date = UnicodeAttribute(null=True)
    end_date = UnicodeAttribute(null=True)
    limit = NumberAttribute(null=True)
    deductible = UnicodeAttribute(null=True)
    amount = NumberAttribute(null=True)
    gsipk1 = UnicodeAttribute(null=True)
    gsisk1 = UnicodeAttribute(null=True)
    gsipk2 = UnicodeAttribute(null=True)
    gsisk2 = UnicodeAttribute(null=True)

    # Additional attributes
    created_at = UnicodeAttribute(null=True)
    updated_at = UnicodeAttribute(null=True)

    # Special attributes
    time_to_live = TTLAttribute(null=True)
    version = VersionAttribute()

    # GSI 1
    class Gsipk1Gsisk1Index(GlobalSecondaryIndex):
        """
        GSI for gsipk1 (hash) and gsisk1 (range).
        Includes policy_name, risk_id, address, premium, sum_insured, policy_version, start_date, end_date.
        """
        class Meta:
            index_name = "gsi__gsipk1__gsisk1__index"
            projection = IncludeProjection(non_attr_keys=['policy_name', 'risk_id', 'address', 'premium', 'sum_insured', 'policy_version', 'start_date', 'end_date'])
            billing_mode = 'PAY_PER_REQUEST'

        gsipk1 = UnicodeAttribute(hash_key=True)
        gsisk1 = UnicodeAttribute(range_key=True)
    
    gsipk1_gsisk1_index = Gsipk1Gsisk1Index()

    # GSI 2
    class Gsipk2Gsisk2Index(GlobalSecondaryIndex):
        """
        GSI for gsipk2 (hash) and gsisk2 (range).
        Keys only projection.
        """
        class Meta:
            index_name = "gsi__gsipk2__gsisk2__index"
            projection = KeysOnlyProjection()
            billing_mode = 'PAY_PER_REQUEST'

        gsipk2 = UnicodeAttribute(hash_key=True)
        gsisk2 = UnicodeAttribute(range_key=True)
    
    gsipk2_gsisk2_index = Gsipk2Gsisk2Index()

    # LSI
    class LsiPolicyVersionIndex(LocalSecondaryIndex):
        """
        LSI for pk (hash) and policy_version (range).
        Includes policy_name, risk_id.
        """
        class Meta:
            index_name = "lsi__policy_version__index"
            projection = IncludeProjection(non_attr_keys=['policy_name', 'risk_id'])

        pk = UnicodeAttribute(hash_key=True)
        policy_version = UnicodeAttribute(range_key=True)
    
    lsi_policy_version_index = LsiPolicyVersionIndex()

    @staticmethod
    def is_test_object(partition_key: str) -> bool:
        """
        Check if the partition key contains 'TEST' string (case-insensitive).
        
        Args:
            partition_key (str): Partition/hash key value to check
            
        Returns:
            bool: True if partition key contains 'TEST', False otherwise
        """
        if not partition_key:
            return False
        
        return 'TEST' in partition_key.upper()

    @staticmethod
    def get_ttl_days():
        """
        Get TTL days from environment variable or default to 0.
        
        Returns:
            int: Number of days for TTL, 0 if not set
        """
        ttl_days = os.environ.get('POLICY_MANAGER_TTL_DAYS', '0')
        return int(ttl_days) if ttl_days else 0

    @staticmethod
    def get_test_ttl_days():
        """
        Get test TTL days from environment variable or default to 30.
        
        Returns:
            int: Number of days for test TTL
        """
        test_ttl_days = os.environ.get('POLICY_MANAGER_TEST_TTL_DAYS', '30')
        return int(test_ttl_days) if test_ttl_days else 30

    def get_ttl(self):
        """
        Return timedelta for TTL based on whether object is a test object.
        
        Returns:
            timedelta or None: Time delta representing TTL period, None if TTL is 0
        """
        if self.is_test_object(self.pk):
            return timedelta(days=self.get_test_ttl_days())
        else:
            ttl_days = self.get_ttl_days()
            if ttl_days != 0:
                return timedelta(days=ttl_days)
            else:
                return None

    def save(self, condition=None):
        """
        Override save to set updated_at and time_to_live.
        
        Args:
            condition: Optional condition expression for conditional writes
            
        Returns:
            Result of the save operation
            
        Raises:
            PutError: If the save operation fails
        """
        self.updated_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        self.time_to_live = self.get_ttl()
        return super().save(condition=condition)
