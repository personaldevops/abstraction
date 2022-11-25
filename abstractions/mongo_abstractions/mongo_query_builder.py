from typing import List, Optional, Self, Union
from pymongo import MongoClient
from abstractions.enums.mongo import MongoActions
from abstractions.mongo_abstractions.mongo_client_loader import MongoClientLoader
from abstractions.mongo_abstractions.operations import MongoFindFilter, MongoSort
from abstractions.exceptions.mongo_exceptions import NoDataToInsert


# noinspection PyTypeChecker
class MongoQueryBuilder:

    def __init__(self, schema: str, collection: str, action: MongoActions,
                 filters: MongoFindFilter = MongoFindFilter(), update_multiple: bool = True,
                 data: Optional[Union[List, dict]] = None, limit: Optional[int] = None,
                 sort: Optional[MongoSort] = None) -> Self:
        """

        Args:
            schema:Database in which query gets executed
            collection:Collection in which query gets executed
            action:Action to be carried out
            filters:Filters to be applied while executing the query
            update_multiple:Boolean value whether to update single document or multiple documents | Default is True
            data:Data that needs to be inserted or updated
            limit:Number of documents to be returned after executing a find query
            sort:Sort to be applied before returning documents
        """
        self.mc: MongoClient = MongoClientLoader().get_db(schema)[
            collection]
        self.action: MongoActions = action
        self.data = data
        self.filters: MongoFindFilter = filters
        self.limit_val: int = limit
        self.sort_val: MongoSort = sort
        self.update_multiple: bool = update_multiple

    def insert(self) -> None:
        """
        Inserts given data into initialized schema and collection.
        :raise NoDataToInsert: If data is None after initialization
        :return: Returns None
        """
        if self.data is None:
            raise NoDataToInsert("Nothing to insert to database.")
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
        if self.update_multiple:
            self.mc.update_many(self.filters.filters,
                                {"$set": self.data})

    def delete(self) -> None:
        self.mc.delete_many(self.filters.filters)

    def execute_query(self, data: Union[List, dict] = None) -> List:
        if data:
            if type(data) == dict:
                self.data = [data]
            else:
                self.data = data
        if self.action == MongoActions.Insert:
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
