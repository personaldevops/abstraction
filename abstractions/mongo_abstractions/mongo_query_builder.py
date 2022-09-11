from typing import List, Optional, Union

from abstractions.enums.mongo import MongoActions
from abstractions.mongo_abstractions.mongo_client_loader import MongoClientLoader
from abstractions.mongo_abstractions.operations import MongoFindFilter, MongoSort, MongoUpdateFilter


class MongoQueryBuilder:

    def __init__(self, schema: str, collection: str, action: MongoActions,
                 filters: Optional[Union[MongoFindFilter, MongoUpdateFilter]] = None,
                 data: Optional[List] = None, limit: Optional[int] = None, sort: Optional[MongoSort] = None):
        self.mc: MongoClientLoader = MongoClientLoader().get_db(schema)[collection]
        self.action = action
        self.data = data
        self.filters = filters
        self.limit_val = limit
        self.sort_val = sort

    def insert(self) -> None:
        if type(self.data) == dict:
            self.data = [self.data]
        self.mc.insert_many(self.data)

    def find(self) -> None:
        if self.filters is not None:
            self.mc = self.mc.find(self.filters.filters, self.filters.columns)
        else:
            self.mc = self.mc.find()

    def limit(self) -> None:
        if self.limit_val is not None:
            self.mc = self.mc.limit(self.limit_val)

    def sort(self) -> None:
        if self.sort_val is not None:
            self.mc = self.mc.sort(self.sort_val.column, self.sort_val.order)

    def update(self) -> None:
        if self.filters.multi == True:
            self.mc.update_many(self.filters.existing, self.filters.update_query)
        else:
            self.mc.update_one(self.filters.existing, self.filters.update_query)

    def delete(self) -> None:
        self.mc.delete_many(self.filters.filters)

    def execute_query(self) -> List:
        if self.action == MongoActions.Insert and self.data is not None:
            self.insert()
        elif self.action == MongoActions.Find:
            self.find()
            self.limit()
            self.sort()
            result = [i for i in self.mc]
            return result
        elif self.action == MongoActions.Update:
            self.update()
        elif self.action == MongoActions.Delete:
            self.delete()
