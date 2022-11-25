from enum import Enum
from abstractions.type_definitions.mongo import *


class MongoActions(Enum):
    Insert = Insert()
    Delete = Delete()
    Update = Update()
    Aggregate = Aggregate()
    Find = Find()
    Count = Count()
    Distinct = Distinct()


class MongoSortOrder(Enum):
    Ascending = 1
    Descending = -1


class MongoPredicates(Enum):
    EqualTo = EqualTo()
    In = In()
    GreaterThan = GreaterThan()
    LessThan = LessThan()
    GreaterThanOrEqual = GreaterThanOrEqual()
    LessThanOrEqual = LessThanOrEqual()
    NotEqual = NotEqual()
    NotIn = NotIn()


class MongoColumnSelection(Enum):
    Keep = 1
    Drop = -1
