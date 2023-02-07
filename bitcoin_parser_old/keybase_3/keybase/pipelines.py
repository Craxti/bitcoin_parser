from itemadapter import ItemAdapter
import pymongo


class userPipeline:
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        db = client['BtcFromKeybase']
        self.collection = db['users']
        self.collection.create_index('btc', unique=True)

    def process_item(self, item, spider):
        self.collection.insert_one(dict(item))
        return item
