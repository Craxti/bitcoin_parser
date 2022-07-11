import pymongo
from src.config import MONGO_LINK, DOMAIN

class MongoDB:
    DB_LINK = MONGO_LINK
    DB_NAME = 'sites'
    COLLECTION_NAME = 'users_btc'

    def __init__(self):
        self._client = pymongo.MongoClient(MONGO_LINK)
        self._db = self._client[MongoDB.DB_NAME]
        self._collection = self._db[MongoDB.COLLECTION_NAME]
        self.model_keys = ['url', 'name', 'avatar_url', 'btc_addresses', 'btc_posts', 'date_registered', 'local_time', 'last_active', 'signature']
        self.default_empty_keys = ['icq', 'aim', 'msn', 'email', 'gender', 'website', 'description', 'location']

    def close(self):
        self._client.close()

    def get_payload_with_default_keys(self):
        return dict.fromkeys(self.default_empty_keys, '')

    def insert(self, data):
        payload = self.get_payload_with_default_keys()
        data_copy = data.copy()

        for key in data_copy:
            if key in self.model_keys:
                payload[key] = data[key]

        for btc_address in payload['btc_addresses']:
            payload_copy = payload.copy()
            payload_copy['btc_address'] = btc_address
            payload_copy['btc_posts'] = ', '.join(payload_copy['btc_posts'])
            payload_copy['domain'] = DOMAIN
            
            payload_copy.pop('btc_addresses', None)

            self._collection.insert_one(payload_copy)