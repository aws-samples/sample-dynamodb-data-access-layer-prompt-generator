import os
from datetime import datetime, timedelta, timezone
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute, MapAttribute, TTLAttribute, VersionAttribute
from pynamodb.indexes import GlobalSecondaryIndex, IncludeProjection


class DdbTableOne(Model):
    """
    PynamoDB model for ddb_table_one DynamoDB table.
    
    This class represents the ddb_table_one DynamoDB table and provides
    the schema definition including primary keys, attributes, and GSIs.
    
    Constructor Arguments:
        pk_attribute_str_1 (str): Primary partition/hash key
        sk_attribute_str_2 (str): Sort/range key
        attribute_str_3 (str): String attribute
        attribute_str_4 (str): String attribute
        attribute_num_5 (int, optional): Number attribute
        attribute_map_6 (dict, optional): Map attribute
        gsipk_attribute_str_7 (str, optional): GSI partition key
        gsisk_attribute_str_8 (str, optional): GSI sort key
        
    Attributes:
        All constructor arguments plus:
        created_at (str): Timestamp of creation
        updated_at (str): Timestamp of last update
        time_to_live (TTLAttribute): TTL for automatic deletion
        version (VersionAttribute): Version number for optimistic locking
        gsipk_attribute_str_7_gsisk_attribute_str_8_index: GSI for querying by gsipk_attribute_str_7 and gsisk_attribute_str_8
    """
    
    class Meta:
        table_name = os.environ.get('DDB_TABLE_ONE', 'ddb_table_one')
        region = os.environ.get('AWS_REGION', 'us-east-1')
        billing_mode = 'PAY_PER_REQUEST'

    # Primary keys
    pk_attribute_str_1 = UnicodeAttribute(hash_key=True, null=False)
    sk_attribute_str_2 = UnicodeAttribute(range_key=True, null=False)

    # Attributes
    attribute_str_3 = UnicodeAttribute(null=False)
    attribute_str_4 = UnicodeAttribute(null=False)
    attribute_num_5 = NumberAttribute(null=True)
    attribute_map_6 = MapAttribute(null=True)
    gsipk_attribute_str_7 = UnicodeAttribute(null=True)
    gsisk_attribute_str_8 = UnicodeAttribute(null=True)

    # Additional attributes
    created_at = UnicodeAttribute(null=True)
    updated_at = UnicodeAttribute(null=True)

    # Special attributes
    time_to_live = TTLAttribute(null=True)
    version = VersionAttribute()

    # GSI
    class GsipkAttributeStr7GsiskAttributeStr8Index(GlobalSecondaryIndex):
        """
        GSI for gsipk_attribute_str_7 (hash) and gsisk_attribute_str_8 (range).
        Includes pk_attribute_str_1, sk_attribute_str_2, attribute_str_3, attribute_str_4, attribute_num_5.
        """
        class Meta:
            index_name = "gsi__gsipk_attribute_str_7__gsisk_attribute_str_8__index"
            projection = IncludeProjection(non_attr_keys=['attribute_str_3', 'attribute_str_4', 'attribute_num_5'])
            billing_mode = 'PAY_PER_REQUEST'

        gsipk_attribute_str_7 = UnicodeAttribute(hash_key=True)
        gsisk_attribute_str_8 = UnicodeAttribute(range_key=True)
    
    gsipk_attribute_str_7_gsisk_attribute_str_8_index = GsipkAttributeStr7GsiskAttributeStr8Index()

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
        ttl_days = os.environ.get('DDB_TABLE_ONE_TTL_DAYS', '0')
        return int(ttl_days) if ttl_days else 0

    @staticmethod
    def get_test_ttl_days():
        """
        Get test TTL days from environment variable or default to 30.
        
        Returns:
            int: Number of days for test TTL
        """
        test_ttl_days = os.environ.get('DDB_TABLE_ONE_TEST_TTL_DAYS', '30')
        return int(test_ttl_days) if test_ttl_days else 30

    def get_ttl(self):
        """
        Return timedelta for TTL based on whether object is a test object.
        
        Returns:
            timedelta or None: Time delta representing TTL period, None if TTL is 0
        """
        if self.is_test_object(self.pk_attribute_str_1):
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
