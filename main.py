import pandas as pd
from pymongo import MongoClient
from pymongo.collection import ReturnDocument
import mongo_ingestion as mi


if __name__ == '__main__':
    mi.do_upsert()
