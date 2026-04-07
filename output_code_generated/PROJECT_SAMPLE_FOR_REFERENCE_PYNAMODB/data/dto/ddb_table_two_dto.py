import json


class DdbTableTwoDto:
    """
    Data Transfer Object for DdbTableTwo (excludes system attributes).
    
    This DTO represents the DdbTableTwo entity without internal
    system attributes like time_to_live and version.
    
    Constructor Arguments:
        pk_attr_str_1 (str, optional): Primary partition/hash key
        sk_attr_str_2 (str, optional): Sort/range key
        attr_str_3 (str, optional): String attribute 3
        attr_str_4 (str, optional): String attribute 4
        attr_str_5 (str, optional): String attribute 5
        attr_str_6 (str, optional): String attribute 6
        gsi1pk_attr_str_7 (str, optional): GSI1 partition key
        gsi1sk_attr_str_8 (str, optional): GSI1 sort key
        gsi2pk_attr_str_9 (str, optional): GSI2 partition key
        gsi2sk_attr_str_10 (str, optional): GSI2 sort key
        gsi3pk_attr_str_11 (str, optional): GSI3 partition key (hash only)
        
    Attributes:
        All constructor arguments are stored as instance attributes.
    """

    def __init__(self, pk_attr_str_1=None, sk_attr_str_2=None, attr_str_3=None,
                 attr_str_4=None, attr_str_5=None, attr_str_6=None,
                 gsi1pk_attr_str_7=None, gsi1sk_attr_str_8=None,
                 gsi2pk_attr_str_9=None, gsi2sk_attr_str_10=None,
                 gsi3pk_attr_str_11=None):
        """
        Initialize DdbTableTwoDto instance.
        
        Args:
            pk_attr_str_1 (str, optional): Primary partition/hash key
            sk_attr_str_2 (str, optional): Sort/range key
            attr_str_3 (str, optional): String attribute 3
            attr_str_4 (str, optional): String attribute 4
            attr_str_5 (str, optional): String attribute 5
            attr_str_6 (str, optional): String attribute 6
            gsi1pk_attr_str_7 (str, optional): GSI1 partition key
            gsi1sk_attr_str_8 (str, optional): GSI1 sort key
            gsi2pk_attr_str_9 (str, optional): GSI2 partition key
            gsi2sk_attr_str_10 (str, optional): GSI2 sort key
            gsi3pk_attr_str_11 (str, optional): GSI3 partition key (hash only)
        """
        self.pk_attr_str_1 = pk_attr_str_1
        self.sk_attr_str_2 = sk_attr_str_2
        self.attr_str_3 = attr_str_3
        self.attr_str_4 = attr_str_4
        self.attr_str_5 = attr_str_5
        self.attr_str_6 = attr_str_6
        self.gsi1pk_attr_str_7 = gsi1pk_attr_str_7
        self.gsi1sk_attr_str_8 = gsi1sk_attr_str_8
        self.gsi2pk_attr_str_9 = gsi2pk_attr_str_9
        self.gsi2sk_attr_str_10 = gsi2sk_attr_str_10
        self.gsi3pk_attr_str_11 = gsi3pk_attr_str_11

    def __str__(self):
        """
        Return JSON string representation of DTO.
        
        Returns:
            str: JSON string representation of the DTO
        """
        dto_dict = {
            'pk_attr_str_1': self.pk_attr_str_1,
            'sk_attr_str_2': self.sk_attr_str_2,
            'attr_str_3': self.attr_str_3,
            'attr_str_4': self.attr_str_4,
            'attr_str_5': self.attr_str_5,
            'attr_str_6': self.attr_str_6,
            'gsi1pk_attr_str_7': self.gsi1pk_attr_str_7,
            'gsi1sk_attr_str_8': self.gsi1sk_attr_str_8,
            'gsi2pk_attr_str_9': self.gsi2pk_attr_str_9,
            'gsi2sk_attr_str_10': self.gsi2sk_attr_str_10,
            'gsi3pk_attr_str_11': self.gsi3pk_attr_str_11
        }
        return json.dumps(dto_dict)
