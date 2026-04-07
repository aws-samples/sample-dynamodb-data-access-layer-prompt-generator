import json


class DdbTableOneDto:
    """
    Data Transfer Object for ddb_table_one DynamoDB table.

    Excludes internal audit and system attributes: created_at, updated_at,
    time_to_live, and version.

    Constructor Arguments:
        pk_attribute_str_1 (str, optional): Primary partition/hash key
        sk_attribute_str_2 (str, optional): Sort/range key
        attribute_str_3 (str, optional): String attribute 3
        attribute_str_4 (str, optional): String attribute 4
        attribute_num_5 (int, optional): Numeric attribute 5
        attribute_map_6 (dict, optional): Map/dict attribute 6
        gsipk_attribute_str_7 (str, optional): GSI partition key
        gsisk_attribute_str_8 (str, optional): GSI sort key

    Attributes:
        All constructor arguments are stored as instance attributes.
    """

    def __init__(self, pk_attribute_str_1=None, sk_attribute_str_2=None,
                 attribute_str_3=None, attribute_str_4=None,
                 attribute_num_5=None, attribute_map_6=None,
                 gsipk_attribute_str_7=None, gsisk_attribute_str_8=None):
        """
        Initialize DdbTableOneDto instance.

        Args:
            pk_attribute_str_1 (str, optional): Primary partition/hash key
            sk_attribute_str_2 (str, optional): Sort/range key
            attribute_str_3 (str, optional): String attribute 3
            attribute_str_4 (str, optional): String attribute 4
            attribute_num_5 (int, optional): Numeric attribute 5
            attribute_map_6 (dict, optional): Map/dict attribute 6
            gsipk_attribute_str_7 (str, optional): GSI partition key
            gsisk_attribute_str_8 (str, optional): GSI sort key
        """
        self.pk_attribute_str_1 = pk_attribute_str_1
        self.sk_attribute_str_2 = sk_attribute_str_2
        self.attribute_str_3 = attribute_str_3
        self.attribute_str_4 = attribute_str_4
        self.attribute_num_5 = attribute_num_5
        self.attribute_map_6 = attribute_map_6
        self.gsipk_attribute_str_7 = gsipk_attribute_str_7
        self.gsisk_attribute_str_8 = gsisk_attribute_str_8

    def __str__(self):
        """
        Return JSON string representation of DTO.

        Returns:
            str: JSON string with all table attributes
        """
        dto_dict = {
            'pk_attribute_str_1': self.pk_attribute_str_1,
            'sk_attribute_str_2': self.sk_attribute_str_2,
            'attribute_str_3': self.attribute_str_3,
            'attribute_str_4': self.attribute_str_4,
            'attribute_num_5': self.attribute_num_5,
            'attribute_map_6': self.attribute_map_6,
            'gsipk_attribute_str_7': self.gsipk_attribute_str_7,
            'gsisk_attribute_str_8': self.gsisk_attribute_str_8
        }
        return json.dumps(dto_dict)
