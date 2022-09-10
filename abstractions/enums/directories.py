from enum import Enum


class Directories(Enum):
    Configs = 'configs'
    StorageConfigs = 'storage'


class ConfigFiles(Enum):
    MongoConfig = 'db_mongo.json'
