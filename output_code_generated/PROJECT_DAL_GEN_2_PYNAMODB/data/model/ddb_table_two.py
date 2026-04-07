import os
from datetime import datetime, timedelta, timezone
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, TTLAttribute, VersionAttribute


class DdbTableTwo(Model):
    """
    PynamoDB model for DdbTableTwo DynamoDB table.

    This class represents the DdbTableTwo DynamoDB table and provides
    the schema definition including primary keys and attributes.

    Constructor Arguments:
        pk_attr_str_1 (str): Primary partition/hash key
        sk_attr_str_2 (str): Sort/range key
        attr_str_3 (str, optional): String attribute 3
        attr_str_4 (str, optional): String attribute 4
        attr_str_5 (str, optional): String attribute 5
        attr_str_6 (str, optional): String attribute 6

    Attributes:
        All constructor arguments plus:
        time_to_live (TTLAttribute): TTL for automatic deletion
        version (VersionAttribute): Version number for optimistic locking
    """

    class Meta:
        table_name = os.environ.get('DDB_TABLE_TWO', 'ddb_table_two')
        region = os.environ.get('AWS_REGION', 'us-east-1')
        billing_mode = 'PAY_PER_REQUEST'

    # Primary keys
    pk_attr_str_1 = UnicodeAttribute(hash_key=True, null=False)
    sk_attr_str_2 = UnicodeAttribute(range_key=True, null=False)

    # Attributes
    attr_str_3 = UnicodeAttribute(null=True)
    attr_str_4 = UnicodeAttribute(null=True)
    attr_str_5 = UnicodeAttribute(null=True)
    attr_str_6 = UnicodeAttribute(null=True)

    # Special attributes
    time_to_live = TTLAttribute(null=True)
    version = VersionAttribute()

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

        This table does not have created_at or updated_at attributes,
        so only time_to_live is set on save.

        Args:
            condition: Optional condition expression for conditional writes

        Returns:
            Result of the save operation

        Raises:
            PutError: If the save operation fails
        """
        self.time_to_live = self.get_ttl()
        return super().save(condition=condition)
