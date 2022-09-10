from datetime import datetime
from typing import Union, List

from SO.enums.mongo import MongoPredicates, MongoSortOrder, MongoColumnSelection


class MongoFilter:
    def __init__(self, attribute, predicate: MongoPredicates, value: Union[List, str, bool, datetime]):
        self.attribute = attribute
        self.predicate = predicate
        self.value = value
        self.filter = self.parse_filter()

    def parse_filter(self) -> dict:
        if self.predicate == MongoPredicates.EqualTo:
            if type(self.value) not in [float, int, str, datetime]:
                raise Exception('Mongo equal to operation does not take list or dict as input')
            return {f'{self.attribute}': self.value}
        if self.predicate == MongoPredicates.In:
            if type(self.value) == list:
                return {f'{self.attribute}': {self.predicate.name: self.value}}
            raise Exception('Mongo $in operation requires a list but received different input')
        if self.predicate == MongoPredicates.Gt:
            if type(self.value) not in [float, int, str, datetime]:
                raise Exception('Mongo $gt operation does not take list or dict as input')
            return {f'{self.attribute}': {self.predicate.value: self.value}}
        if self.predicate == MongoPredicates.Lt:
            if type(self.value) not in [float, int, str, datetime]:
                raise Exception('Mongo $lt operation does not take list or dict as input')
            return {f'{self.attribute}': {self.predicate.value: self.value}}


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

    def add_filter(self, filter: MongoFilter = None, columns: MongoColumn = None) -> None:
        if columns is not None and filter is not None:
            self.filters[list(filter.filter.keys())[0]] = list(filter.filter.values())[0]
            self.columns[list(columns.columns.keys())[0]] = list(columns.columns.values())[0]
        elif filter is None and columns is not None:
            self.columns[list(columns.columns.keys())[0]] = list(columns.columns.values())[0]
        elif columns is None and filter is not None:
            self.filters[list(filter.filter.keys())[0]] = list(filter.filter.values())[0]


class MongoUpdateFilter:
    def __init__(self, multi: bool = False):
        self.existing = {}
        self.update = {}
        self.multi = multi
        self.update_query = None

    def add_filter(self, update: MongoFilter, existing: MongoFilter = None) -> None:
        if existing is not None:
            self.existing[list(existing.filter.keys())[0]] = list(existing.filter.values())[0]
            self.update[list(update.filter.keys())[0]] = list(update.filter.values())[0]
        else:
            self.update[list(update.filter.keys())[0]] = list(update.filter.values())[0]
        self.update_query = {'$set': self.update}
