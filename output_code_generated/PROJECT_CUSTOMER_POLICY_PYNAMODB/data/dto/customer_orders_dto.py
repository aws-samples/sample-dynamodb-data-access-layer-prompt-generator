import json


class CustomerOrdersDto:
    """
    Data Transfer Object for CustomerOrders (excludes audit and system attributes).
    
    This DTO represents the CustomerOrders entity without internal audit
    and system attributes like created_at, updated_at, time_to_live, and version.
    
    Constructor Arguments:
        pk (str, optional): Primary partition/hash key
        sk (str, optional): Sort/range key
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
        All constructor arguments are stored as instance attributes.
    """

    def __init__(self, pk=None, sk=None, cust_address=None, first_name=None,
                 last_name=None, phone=None, is_premium_member=None, order_date=None,
                 order_quantity=None, sku=None, gsipk1=None, gsisk1=None):
        """
        Initialize CustomerOrdersDto instance.
        
        Args:
            pk (str, optional): Primary partition/hash key
            sk (str, optional): Sort/range key
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
        """
        self.pk = pk
        self.sk = sk
        self.cust_address = cust_address
        self.first_name = first_name
        self.last_name = last_name
        self.phone = phone
        self.is_premium_member = is_premium_member
        self.order_date = order_date
        self.order_quantity = order_quantity
        self.sku = sku
        self.gsipk1 = gsipk1
        self.gsisk1 = gsisk1

    def __str__(self):
        """
        Return JSON string representation of DTO.
        
        Returns:
            str: JSON string representation of the DTO
        """
        dto_dict = {
            'pk': self.pk,
            'sk': self.sk,
            'cust_address': self.cust_address,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'phone': self.phone,
            'is_premium_member': self.is_premium_member,
            'order_date': self.order_date,
            'order_quantity': self.order_quantity,
            'sku': self.sku,
            'gsipk1': self.gsipk1,
            'gsisk1': self.gsisk1
        }
        return json.dumps(dto_dict)
