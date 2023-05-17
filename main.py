import pickle
import sys
from configparser import ConfigParser
from datetime import datetime

from crawler import Crawler
from db import DB


if __name__ == "__main__":
    configs = ConfigParser()
    configs.read("configs.ini")

    db = DB(**configs["db"])
    with open(configs["models"]["articles_sieve"], "rb") as clf_file:
        articles_sieve = pickle.load(clf_file)
    with open(configs["models"]["vectorizer"], "rb") as cls_file:
        vectorizer = pickle.load(cls_file)

    start_date = None
    if len(sys.argv) > 1:
        start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d-%H")
    crawler = Crawler(db, articles_sieve, vectorizer, start_date)
    crawler.run(skip_download=True)
