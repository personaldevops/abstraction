from pydantic.dataclasses import dataclass
from typing import Union
from datetime import datetime
from bson import ObjectId
from abstractions.enums.mongo import MongoPredicates


@dataclass
class BooleanQuery:
    attribute: str
    predicate: MongoPredicates
    value: bool


@dataclass
class DateTimeQuery:
    attribute: str
    predicate: MongoPredicates
    value: Union[str, datetime]

    def __post_init__(self):
        if isinstance(self.value, str):
            self.value = datetime.fromisoformat(self.value)


@dataclass
class StringQuery:
    attribute: str
    predicate: MongoPredicates
    value: str


@dataclass
class ObjectIdQuery:
    attribute: str
    predicate: MongoPredicates
    value: Union[ObjectId, str]

    def __post_init__(self):
        if isinstance(self.value, ObjectId):
            return
        self.value = ObjectId(self.value)
