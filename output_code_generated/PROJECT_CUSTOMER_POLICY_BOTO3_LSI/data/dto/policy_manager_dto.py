import json


class PolicyManagerDto:
    """
    Data Transfer Object for PolicyManager DynamoDB table.

    Excludes internal audit and system attributes: created_at, updated_at,
    time_to_live, and version.

    Constructor Arguments:
        pk (str, optional): Primary partition/hash key
        sk (str, optional): Sort/range key
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
        All constructor arguments are stored as instance attributes.
    """

    def __init__(self, pk=None, sk=None, policy_id=None, policy_name=None,
                 risk_id=None, address=None, premium=None, sum_insured=None,
                 policy_version=None, start_date=None, end_date=None, limit=None,
                 deductible=None, amount=None, gsipk1=None, gsisk1=None,
                 gsipk2=None, gsisk2=None):
        """
        Initialize PolicyManagerDto instance.

        Args:
            pk (str, optional): Primary partition/hash key
            sk (str, optional): Sort/range key
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
        """
        self.pk = pk
        self.sk = sk
        self.policy_id = policy_id
        self.policy_name = policy_name
        self.risk_id = risk_id
        self.address = address
        self.premium = premium
        self.sum_insured = sum_insured
        self.policy_version = policy_version
        self.start_date = start_date
        self.end_date = end_date
        self.limit = limit
        self.deductible = deductible
        self.amount = amount
        self.gsipk1 = gsipk1
        self.gsisk1 = gsisk1
        self.gsipk2 = gsipk2
        self.gsisk2 = gsisk2

    def __str__(self):
        """
        Return JSON string representation of DTO.

        Returns:
            str: JSON string with all table attributes
        """
        dto_dict = {
            'pk': self.pk,
            'sk': self.sk,
            'policy_id': self.policy_id,
            'policy_name': self.policy_name,
            'risk_id': self.risk_id,
            'address': self.address,
            'premium': self.premium,
            'sum_insured': self.sum_insured,
            'policy_version': self.policy_version,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'limit': self.limit,
            'deductible': self.deductible,
            'amount': self.amount,
            'gsipk1': self.gsipk1,
            'gsisk1': self.gsisk1,
            'gsipk2': self.gsipk2,
            'gsisk2': self.gsisk2
        }
        return json.dumps(dto_dict)
