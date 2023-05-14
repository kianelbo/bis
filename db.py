from pymongo import MongoClient


class DB:
    def __init__(self, host, db_name="egeos_db", collection_name="events") -> None:
        self.client = MongoClient(host)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def insert(self, events):
        self.collection.insert_many(events)

    def get_events(self, start_date=None, end_date=None):
        query = {}
        if start_date or end_date:
            query["date"] = {}
        if start_date:
            query["date"]["$gte"] = start_date
        if end_date:
            query["date"]["$lt"] = end_date
        cursor = self.collection.find(query)
        return [self.serialize_event(doc) for doc in cursor]

    @staticmethod
    def serialize_event(doc):
        return {
            "event_id": doc["event_id"],
            "type": doc["type"],
            "country": doc["country"],
            "date": doc["date"],
            "locations": doc["locations"],
            "images": doc["images"]
        }
