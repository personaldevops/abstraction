import json
import os

from pymongo import MongoClient

from SO.enums.directories import Directories, ConfigFiles


class MongoClientLoader(object):
    __instance = None
    dbs = {}
    db_store = {}

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(MongoClientLoader, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self._load_config()
        self.initialize_mongo()

    def _load_config(self) -> None:
        codebase_dir = os.environ['CODEBASE_DIR']
        sub_dir = Directories.StorageConfigs.value
        main_dir = Directories.Configs.value
        filename = ConfigFiles.MongoConfig.value
        path = os.path.join(codebase_dir, main_dir, sub_dir, filename)
        env_mode = os.environ['ENV_MODE']
        if not os.path.exists(path):
            raise Exception(f'{filename} not found')
        with open(path, 'r') as file:
            data = file.read()
        databases = json.loads(data)
        databases = databases['databases'][env_mode]
        for i in databases:
            self.dbs[i['schema']] = i

    def initialize_mongo(self) -> None:
        for db in self.dbs.values():
            host = db['base_url']
            port = db['port']
            if db['username'] is not None and db['password'] is not None:
                username = db['username']
                password = db['password']
                self.client = MongoClient(host=host, port=port, username=username, password=password)
            else:
                self.client = MongoClient(host=host, port=port)
            m_db = self.client[db['schema']]
            self.db_store[db['schema']] = m_db

    def get_db(self, db_name: str) -> MongoClient:
        client = self.db_store.get(db_name)
        if client is not None:
            return client
        raise Exception('Not Initialized: Database not initialized in config')
