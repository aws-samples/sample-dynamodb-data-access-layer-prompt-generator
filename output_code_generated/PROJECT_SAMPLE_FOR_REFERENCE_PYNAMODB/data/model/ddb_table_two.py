import os
from datetime import datetime, timedelta, timezone
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, TTLAttribute, VersionAttribute
from pynamodb.indexes import GlobalSecondaryIndex, IncludeProjection, AllProjection, KeysOnlyProjection


class DdbTableTwo(Model):
    """
    PynamoDB model for ddb_table_two DynamoDB table.
    
    This class represents the ddb_table_two DynamoDB table and provides
    the schema definition including primary keys, attributes, and GSIs.
    
    Constructor Arguments:
        pk_attr_str_1 (str): Primary partition/hash key
        sk_attr_str_2 (str): Sort/range key
        attr_str_3 (str): String attribute
        attr_str_4 (str): String attribute
        attr_str_5 (str): String attribute
        attr_str_6 (str): String attribute
        gsi1pk_attr_str_7 (str): GSI1 partition key
        gsi1sk_attr_str_8 (str): GSI1 sort key
        gsi2pk_attr_str_9 (str): GSI2 partition key
        gsi2sk_attr_str_10 (str): GSI2 sort key
        gsi3pk_attr_str_11 (str, optional): GSI3 partition key
        
    Attributes:
        All constructor arguments plus:
        time_to_live (TTLAttribute): TTL for automatic deletion
        version (VersionAttribute): Version number for optimistic locking
        gsi1pk_attr_str_7_gsi1sk_attr_str_8_index: GSI for querying by gsi1pk_attr_str_7 and gsi1sk_attr_str_8
        gsi2pk_attr_str_9_gsi2sk_attr_str_10_index: GSI for querying by gsi2pk_attr_str_9 and gsi2sk_attr_str_10
        gsi3pk_attr_str_11_index: GSI for querying by gsi3pk_attr_str_11
    """
    
    class Meta:
        table_name = os.environ.get('DDB_TABLE_TWO', 'ddb_table_two')
        region = os.environ.get('AWS_REGION', 'us-east-1')
        billing_mode = 'PAY_PER_REQUEST'

    # Primary keys
    pk_attr_str_1 = UnicodeAttribute(hash_key=True, null=False)
    sk_attr_str_2 = UnicodeAttribute(range_key=True, null=False)

    # Attributes
    attr_str_3 = UnicodeAttribute(null=False)
    attr_str_4 = UnicodeAttribute(null=False)
    attr_str_5 = UnicodeAttribute(null=False)
    attr_str_6 = UnicodeAttribute(null=False)
    gsi1pk_attr_str_7 = UnicodeAttribute(null=False)
    gsi1sk_attr_str_8 = UnicodeAttribute(null=False)
    gsi2pk_attr_str_9 = UnicodeAttribute(null=False)
    gsi2sk_attr_str_10 = UnicodeAttribute(null=False)
    gsi3pk_attr_str_11 = UnicodeAttribute(null=True)

    # Special attributes
    time_to_live = TTLAttribute(null=True)
    version = VersionAttribute()

    # GSI 1
    class Gsi1pkAttrStr7Gsi1skAttrStr8Index(GlobalSecondaryIndex):
        """
        GSI for gsi1pk_attr_str_7 (hash) and gsi1sk_attr_str_8 (range).
        Includes pk_attr_str_1, sk_attr_str_2, attr_str_3, attr_str_4, attr_str_5.
        """
        class Meta:
            index_name = "gsi__gsi1pk_attr_str_7__gsi1sk_attr_str_8__index"
            projection = IncludeProjection(non_attr_keys=['attr_str_3', 'attr_str_4', 'attr_str_5'])
            billing_mode = 'PAY_PER_REQUEST'

        gsi1pk_attr_str_7 = UnicodeAttribute(hash_key=True)
        gsi1sk_attr_str_8 = UnicodeAttribute(range_key=True)
    
    gsi1pk_attr_str_7_gsi1sk_attr_str_8_index = Gsi1pkAttrStr7Gsi1skAttrStr8Index()

    # GSI 2
    class Gsi2pkAttrStr9Gsi2skAttrStr10Index(GlobalSecondaryIndex):
        """
        GSI for gsi2pk_attr_str_9 (hash) and gsi2sk_attr_str_10 (range).
        Keys only projection.
        """
        class Meta:
            index_name = "gsi__gsi2pk_attr_str_9__gsi2sk_attr_str_10__index"
            projection = KeysOnlyProjection()
            billing_mode = 'PAY_PER_REQUEST'

        gsi2pk_attr_str_9 = UnicodeAttribute(hash_key=True)
        gsi2sk_attr_str_10 = UnicodeAttribute(range_key=True)
    
    gsi2pk_attr_str_9_gsi2sk_attr_str_10_index = Gsi2pkAttrStr9Gsi2skAttrStr10Index()

    # GSI 3
    class Gsi3pkAttrStr11Index(GlobalSecondaryIndex):
        """
        GSI for gsi3pk_attr_str_11 (hash) with no sort key.
        All attributes projection.
        """
        class Meta:
            index_name = "gsi__gsi3pk_attr_str_11__index"
            projection = AllProjection()
            billing_mode = 'PAY_PER_REQUEST'

        gsi3pk_attr_str_11 = UnicodeAttribute(hash_key=True)
    
    gsi3pk_attr_str_11_index = Gsi3pkAttrStr11Index()

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
        ttl_days = os.environ.get('DDB_TABLE_TWO_TTL_DAYS', '0')
        return int(ttl_days) if ttl_days else 0

    @staticmethod
    def get_test_ttl_days():
        """
        Get test TTL days from environment variable or default to 30.
        
        Returns:
            int: Number of days for test TTL
        """
        test_ttl_days = os.environ.get('DDB_TABLE_TWO_TEST_TTL_DAYS', '30')
        return int(test_ttl_days) if test_ttl_days else 30

    def get_ttl(self):
        """
        Return timedelta for TTL based on whether object is a test object.
        
        Returns:
            timedelta or None: Time delta representing TTL period, None if TTL is 0
        """
        if self.is_test_object(self.pk_attr_str_1):
            return timedelta(days=self.get_test_ttl_days())
        else:
            ttl_days = self.get_ttl_days()
            if ttl_days != 0:
                return timedelta(days=ttl_days)
            else:
                return None

    def save(self, condition=None):
        """
        Override save to set time_to_live.
        
        Args:
            condition: Optional condition expression for conditional writes
            
        Returns:
            Result of the save operation
            
        Raises:
            PutError: If the save operation fails
        """
        self.time_to_live = self.get_ttl()
        return super().save(condition=condition)
