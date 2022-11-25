from datetime import datetime
from multipledispatch import dispatch
from abstractions.enums.mongo import MongoSortOrder, MongoColumnSelection
from abstractions.exceptions.mongo_exceptions import InvalidQueryCombinations
from abstractions.type_definitions.mongo import *
from abstractions.type_definitions.mongo_query_attributes import DateTimeQuery, ObjectIdQuery, StringQuery, BooleanQuery
from typing import Union


class MongoFilter:
    def __init__(self, query: Union[DateTimeQuery, ObjectIdQuery, StringQuery, BooleanQuery]):
        self.attribute = query.attribute
        self.predicate = query.predicate
        self.value = query.value
        self.filter = self.parse_filter(self.predicate.value)

    @dispatch(EqualTo)
    def parse_filter(self, predicate) -> dict:
        if type(self.value) not in [float, int, str, datetime]:
            raise InvalidQueryCombinations('Mongo equal to operation does not take list or dict as input')
        return {f'{self.attribute}': self.value}

    @dispatch(In)
    def parse_filter(self, predicate) -> dict:
        if type(self.value) == list:
            return {f'{self.attribute}': {"$in": self.value}}
        raise InvalidQueryCombinations('Mongo $in operation requires a list but received different input')

    @dispatch(NotIn)
    def parse_filter(self, predicate) -> dict:
        if type(self.value) == list:
            return {f'{self.attribute}': {"$nin": self.value}}
        raise InvalidQueryCombinations('Mongo $in operation requires a list but received different input')

    @dispatch(GreaterThan)
    def parse_filter(self, predicate) -> dict:
        if type(self.value) not in [float, int, str, datetime]:
            raise InvalidQueryCombinations('Mongo $gt operation does not take list or dict as input')
        return {f'{self.attribute}': {"$gt": self.value}}

    @dispatch(GreaterThanOrEqual)
    def parse_filter(self, predicate) -> dict:
        if type(self.value) not in [float, int, str, datetime]:
            raise InvalidQueryCombinations('Mongo $gte operation does not take list or dict as input')
        return {f'{self.attribute}': {"$gte": self.value}}

    @dispatch(LessThan)
    def parse_filter(self, predicate) -> dict:
        if type(self.value) not in [float, int, str, datetime]:
            raise InvalidQueryCombinations('Mongo $lt operation does not take list or dict as input')
        return {f'{self.attribute}': {"$lt": self.value}}

    @dispatch(LessThanOrEqual)
    def parse_filter(self, predicate) -> dict:
        if type(self.value) not in [float, int, str, datetime]:
            raise InvalidQueryCombinations('Mongo $lt operation does not take list or dict as input')
        return {f'{self.attribute}': {"$lte": self.value}}


class MongoColumn:
    def __init__(self, column: str, keep: MongoColumnSelection):
        self.columns = {column: keep.value}


class MongoSort:
    def __init__(self, column: str, order: MongoSortOrder):
        self.column = column
        self.order = order.value


class MongoFindFilter:
    def __init__(self):
        self.filters = {}
        self.columns = {}

    @dispatch(MongoFilter, MongoColumn)
    def add_filter(self, filter_, columns) -> None:
        self.filters[list(filter_.filter.keys())[0]] = list(filter_.filter.values())[0]
        self.columns[list(columns.columns.keys())[0]] = list(columns.columns.values())[0]

    @dispatch(MongoColumn)
    def add_filter(self, filter_) -> None:
        self.columns[list(filter_.columns.keys())[0]] = list(filter_.columns.values())[0]

    @dispatch(MongoFilter)
    def add_filter(self, filter_) -> None:
        self.filters[list(filter_.filter.keys())[0]] = list(filter_.filter.values())[0]
