import json


class DdbTableTwoDto:
    """
    Data Transfer Object for ddb_table_two DynamoDB table.

    Excludes internal system attributes: time_to_live and version.

    Constructor Arguments:
        pk_attr_str_1 (str, optional): Primary partition/hash key
        sk_attr_str_2 (str, optional): Sort/range key
        attr_str_3 (str, optional): String attribute 3
        attr_str_4 (str, optional): String attribute 4
        attr_str_5 (str, optional): String attribute 5
        attr_str_6 (str, optional): String attribute 6

    Attributes:
        All constructor arguments are stored as instance attributes.
    """

    def __init__(self, pk_attr_str_1=None, sk_attr_str_2=None,
                 attr_str_3=None, attr_str_4=None,
                 attr_str_5=None, attr_str_6=None):
        """
        Initialize DdbTableTwoDto instance.

        Args:
            pk_attr_str_1 (str, optional): Primary partition/hash key
            sk_attr_str_2 (str, optional): Sort/range key
            attr_str_3 (str, optional): String attribute 3
            attr_str_4 (str, optional): String attribute 4
            attr_str_5 (str, optional): String attribute 5
            attr_str_6 (str, optional): String attribute 6
        """
        self.pk_attr_str_1 = pk_attr_str_1
        self.sk_attr_str_2 = sk_attr_str_2
        self.attr_str_3 = attr_str_3
        self.attr_str_4 = attr_str_4
        self.attr_str_5 = attr_str_5
        self.attr_str_6 = attr_str_6

    def __str__(self):
        """
        Return JSON string representation of DTO.

        Returns:
            str: JSON string with all table attributes
        """
        dto_dict = {
            'pk_attr_str_1': self.pk_attr_str_1,
            'sk_attr_str_2': self.sk_attr_str_2,
            'attr_str_3': self.attr_str_3,
            'attr_str_4': self.attr_str_4,
            'attr_str_5': self.attr_str_5,
            'attr_str_6': self.attr_str_6
        }
        return json.dumps(dto_dict)
