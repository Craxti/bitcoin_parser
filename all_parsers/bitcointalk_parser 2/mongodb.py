import pymongo
import time

DB_LINK = "mongodb://localhost:27017/sites"
COLLECTION_NAME = 'bitcointalk'

validator = {
    "properties": {
        "id": {
            "bsonType": ["int"]
        },
        "name": {
            "bsonType": ["string"],
        },
        "domain": {
            "bsonType": ["string"],
        },
        "link": {
            "bsonType": ["string"],
        },
        "avatar": {
            "bsonType": ["string"],
        },
        "date_registered": {
            "bsonType": ["string"],
        },
        "last_active": {
            "bsonType": ["string"],
        },
        "icq": {
            "bsonType": ["string"],
        },
        "aim": {
            "bsonType": ["string"],
        },
        "msn": {
            "bsonType": ["string"],
        },
        "email": {
            "bsonType": ["string"],
        },
        "website": {
            "bsonType": ["string"],
        },
        "gender": {
            "bsonType": ["string"],
        },
        "age": {
            "bsonType": ["string"],
        },
        "location": {
            "bsonType": ["string"],
        },
        "signature": {
            "bsonType": ["string"],
        },
        "bitcoin_address": {
            "bsonType": ["string"],
        },
        "all_links_btc": {
            "bsonType": ["string"],
        },
        "description": {
            "bsonType": ["string"],
        }
    }
}

client = pymongo.MongoClient(DB_LINK)
db = client["sites"]

def create_collection():
    try:
        db.create_collection(name=COLLECTION_NAME)
    except Exception as ex:
        print(ex)

    return db[COLLECTION_NAME]

def insert_filtered_data(data):
    filteredData = {
        'timestamp': int(time.time())
    }

    for key in validator['properties']:
        try:
            filteredData[key] = data[key]
        except KeyError as ex:
            print('Error, key not found: ', ex)
            pass

    db[COLLECTION_NAME].insert_one(filteredData)
    
    return filteredData

dbCollection = create_collection()