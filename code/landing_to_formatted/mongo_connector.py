from pymongo import MongoClient
import os
import glob


def load_collection(collection):
    # Connect to MongoDB
    client = MongoClient("mongodb://10.4.41.48")

    # Access the desired database and collection
    db = client["landing_zone"]
    documents_collection = db[collection]
    documents = list(documents_collection.find())
    return documents