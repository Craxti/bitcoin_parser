from motor.motor_asyncio import AsyncIOMotorClient

class Mongo:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = AsyncIOMotorClient(host, port)
        self.db = self.client.sites
        self.collection = self.db.users_btc

    async def get_address_info(self, address, category):
        result = await self.collection.find_one({"bitcoin_adress": address})
        dict = {}
        for key in result.keys(): dict[key] = str(result[key])
        self.__init__(self.host, self.port)
        return dict
    
    async def get_collection_stat(self):
        countDoc = await self.collection.count_documents({})
        countProfile = await self.collection.count_documents({"category": "profile"})
        countPost = await self.collection.count_documents({"category": "post"})
        self.__init__(self.host, self.port)
        return {"addresses": countDoc, "category": [{"profile": countProfile}, {"post": countPost}]}
    
    async def add_to_address(self, data):
        result = await self.collection.insert_one(data)
        self.__init__(self.host, self.port)
        return str(result.inserted_id)

    async def get_address_category(self, category, page, q):
        if q:
            result = await self.collection.count_documents({"category": category})
            self.__init__(self.host, self.port)
            return {category: result}
        result = self.collection.find({"category": category},  allow_disk_use=True)
        self.__init__(self.host, self.port)
        result = []
        if page:
            for address in result[page * 50:][:(page + 1) * 50]: result.append(address['bitcoin_address'])
        else:
            for address in result: result.append(address['bitcoin_address'])
        return result

    async def get_collection_address(self, address):
        countProfile = await self.collection.count_documents({"bitcoin_address": address, "category": "post"})
        countPost = await self.collection.count_documents({"bitcoin_address": address, "category": "profile"})
        self.__init__(self.host, self.port)
        return {"profile": countProfile, "post": countPost}