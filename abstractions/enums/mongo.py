from enum import Enum


class MongoActions(Enum):
    Insert = 'Insert'
    Delete = 'Delete'
    Update = 'Update'
    Find = 'Find'
    Count = 'Count'
    Distinct = 'Distinct'


class MongoSortOrder(Enum):
    Ascending = 1
    Descending = -1


class MongoPredicates(Enum):
    EqualTo = 'EqualTo'
    In = '$in'
    Gt = '$gt'
    Lt = '$lt'


class MongoColumnSelection(Enum):
    Keep = 1
    Drop = -1
