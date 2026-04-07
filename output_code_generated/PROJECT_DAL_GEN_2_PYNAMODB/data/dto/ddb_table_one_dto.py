import json


class DdbTableOneDto:
    """
    Data Transfer Object for DdbTableOne (excludes audit and system attributes).

    This DTO represents the DdbTableOne entity without internal audit
    and system attributes like created_at, updated_at, time_to_live, and version.

    Constructor Arguments:
        pk_attribute_str_1 (str, optional): Primary partition/hash key
        sk_attribute_str_2 (str, optional): Sort/range key
        attribute_str_3 (str, optional): String attribute 3
        attribute_str_4 (str, optional): String attribute 4
        attribute_num_5 (int, optional): Number attribute 5
        attribute_map_6 (dict, optional): Map attribute 6

    Attributes:
        All constructor arguments are stored as instance attributes.
    """

    def __init__(self, pk_attribute_str_1=None, sk_attribute_str_2=None, attribute_str_3=None,
                 attribute_str_4=None, attribute_num_5=None, attribute_map_6=None):
        """
        Initialize DdbTableOneDto instance.

        Args:
            pk_attribute_str_1 (str, optional): Primary partition/hash key
            sk_attribute_str_2 (str, optional): Sort/range key
            attribute_str_3 (str, optional): String attribute 3
            attribute_str_4 (str, optional): String attribute 4
            attribute_num_5 (int, optional): Number attribute 5
            attribute_map_6 (dict, optional): Map attribute 6
        """
        self.pk_attribute_str_1 = pk_attribute_str_1
        self.sk_attribute_str_2 = sk_attribute_str_2
        self.attribute_str_3 = attribute_str_3
        self.attribute_str_4 = attribute_str_4
        self.attribute_num_5 = attribute_num_5
        self.attribute_map_6 = attribute_map_6

    def __str__(self):
        """
        Return JSON string representation of DTO.

        Returns:
            str: JSON string representation of the DTO
        """
        dto_dict = {
            'pk_attribute_str_1': self.pk_attribute_str_1,
            'sk_attribute_str_2': self.sk_attribute_str_2,
            'attribute_str_3': self.attribute_str_3,
            'attribute_str_4': self.attribute_str_4,
            'attribute_num_5': self.attribute_num_5,
            'attribute_map_6': self.attribute_map_6
        }
        return json.dumps(dto_dict)
