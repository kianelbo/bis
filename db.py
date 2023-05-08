import pymongo


class DB:
    def __init__(self, host, db_name="egeos_db", collection_name="events") -> None:
        self.client = pymongo.MongoClient(host)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def insert(self, event):
        self.collection.insert_one(event)

    def get_events(self, date):
        query = {"datetime": {"$lt": date}}
        return self.collection.find(query)

