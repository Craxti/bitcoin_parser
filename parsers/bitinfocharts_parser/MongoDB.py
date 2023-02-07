import pymongo
from parsers.bitinfocharts_parser.config import MONGO_URL
from dataclasses import dataclass, asdict, astuple
import time

@dataclass
class Model:
    wallet: str
    wallet_link: str
    btc: str
    btc_link: str
    timestamp: int = int(time.time())

class MongoDB:
    DB_LINK = MONGO_URL
    DB_NAME = 'sites'
    COLLECTION_NAME = 'users_btc'

    def __init__(self):
        self._client = pymongo.MongoClient(MongoDB.DB_LINK)
        self._db = self._client[MongoDB.DB_NAME]
        self._collection = self._db[MongoDB.COLLECTION_NAME]
        self.new_added_count = 0

    def is_exists_btc(self, btc):
        if self._collection.find({'btc': btc}).count() > 0:
            return True
            
        return False

    def insert_unique_many(self, data):
        for item in data:
            if self.is_exists_btc(item['btc']): continue

            self._collection.insert_one(item)
            self.new_added_count += 1

    def insert_one(self,data):
        self._collection.insert_one(data)