import pymongo
from config import MONGO_URI
import time

class MongoDB:
    DB_LINK = MONGO_URI
    DB_NAME = 'sites'
    COLLECTION_NAME = 'users_btc'

    def __init__(self):
        self._client = pymongo.MongoClient(MongoDB.DB_LINK)
        self._db = self._client[MongoDB.DB_NAME]
        self._collection = self._db[MongoDB.COLLECTION_NAME]
        self.new_added_count = 0

    def remove_exists_btc(self, btc_addresses):
        is_exists = False

        for idx, _ in enumerate(btc_addresses.copy()):
            for btc in btc_addresses.copy():
                btc_name = f'btc_address_{idx + 1}'
                if self._collection.find({btc_name: btc}).count() > 0:
                    is_exists = True
                    btc_addresses.remove(btc)

        return is_exists

    def insert_unique_many(self, data):
        for item in data:
            self.remove_exists_btc(item['btc_addresses'])
            if not item['btc_addresses']: continue
           
            for idx, btc_address in enumerate(item['btc_addresses']):
                item[f'btc_address_{idx + 1}'] = btc_address

            item.pop('btc_addresses', None)
            item['timestamp'] = int(time.time())

            self._collection.insert_one(item)
            self.new_added_count += 1

    def insert_one(self,data):
        self._collection.insert_one(data)