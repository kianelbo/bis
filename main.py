from crawler import Crawler
from db import DB


hostname = "mongodb://localhost:27017/"
db_name = "egeos_db"
collection_name = "events"


if __name__ == "__main__":
    db = DB(hostname, db_name, collection_name)
    crawler = Crawler(db)
    crawler.run()
