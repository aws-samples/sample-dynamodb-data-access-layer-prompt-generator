import os
from datetime import datetime, timedelta, timezone
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute, TTLAttribute, VersionAttribute
from pynamodb.indexes import GlobalSecondaryIndex, IncludeProjection


class CustomerOrders(Model):
    """
    PynamoDB model for CustomerOrders DynamoDB table.
    
    This class represents the CustomerOrders DynamoDB table and provides
    the schema definition including primary keys, attributes, and GSIs.
    
    Constructor Arguments:
        pk (str): Primary partition/hash key
        sk (str): Sort/range key
        cust_address (str, optional): Customer address
        first_name (str, optional): First name
        last_name (str, optional): Last name
        phone (str, optional): Phone number
        is_premium_member (str, optional): Premium member flag
        order_date (str, optional): Order date
        order_quantity (int, optional): Order quantity
        sku (str, optional): SKU
        gsipk1 (str, optional): GSI partition key
        gsisk1 (str, optional): GSI sort key
        
    Attributes:
        All constructor arguments plus:
        created_at (str): Timestamp of creation
        updated_at (str): Timestamp of last update
        time_to_live (TTLAttribute): TTL for automatic deletion
        version (VersionAttribute): Version number for optimistic locking
        gsipk1_gsisk1_index: GSI for querying by gsipk1 and gsisk1
    """
    
    class Meta:
        table_name = os.environ.get('CUSTOMER_ORDERS', 'CustomerOrders')
        region = os.environ.get('AWS_REGION', 'us-east-1')
        billing_mode = 'PAY_PER_REQUEST'

    # Primary keys
    pk = UnicodeAttribute(hash_key=True, null=False)
    sk = UnicodeAttribute(range_key=True, null=False)

    # Attributes
    cust_address = UnicodeAttribute(null=True)
    first_name = UnicodeAttribute(null=True)
    last_name = UnicodeAttribute(null=True)
    phone = UnicodeAttribute(null=True)
    is_premium_member = UnicodeAttribute(null=True)
    order_date = UnicodeAttribute(null=True)
    order_quantity = NumberAttribute(null=True)
    sku = UnicodeAttribute(null=True)
    gsipk1 = UnicodeAttribute(null=True)
    gsisk1 = UnicodeAttribute(null=True)

    # Additional attributes
    created_at = UnicodeAttribute(null=True)
    updated_at = UnicodeAttribute(null=True)

    # Special attributes
    time_to_live = TTLAttribute(null=True)
    version = VersionAttribute()

    # GSI
    class Gsipk1Gsisk1Index(GlobalSecondaryIndex):
        """
        GSI for gsipk1 (hash) and gsisk1 (range).
        Includes pk, sk, order_date, order_quantity, sku.
        """
        class Meta:
            index_name = "gsi__gsipk1__gsisk1__index"
            projection = IncludeProjection(non_attr_keys=['order_date', 'order_quantity', 'sku'])
            billing_mode = 'PAY_PER_REQUEST'

        gsipk1 = UnicodeAttribute(hash_key=True)
        gsisk1 = UnicodeAttribute(range_key=True)
    
    gsipk1_gsisk1_index = Gsipk1Gsisk1Index()

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
        ttl_days = os.environ.get('CUSTOMER_ORDERS_TTL_DAYS', '0')
        return int(ttl_days) if ttl_days else 0

    @staticmethod
    def get_test_ttl_days():
        """
        Get test TTL days from environment variable or default to 30.
        
        Returns:
            int: Number of days for test TTL
        """
        test_ttl_days = os.environ.get('CUSTOMER_ORDERS_TEST_TTL_DAYS', '30')
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
