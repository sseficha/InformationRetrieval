import pymongo
import time
from pymongo import MongoClient
from numpy import log
import threading
import math
from operator import itemgetter


class Index(threading.Thread):

    client = MongoClient('localhost', 27017)
    db = client.InformationRetrieval
    collection = db["Indexer"]
    documentsCollection = db["Documents"]
    miniIndex = []
    word_queue = []
    mini_index_queue_lock = None
    word_queue_lock = None
    mini_index_pull_lock = None


    def __init__(self):
        threading.Thread.__init__(self)


    @staticmethod
    # deletes everything in database
    def clear():
        Index.collection.delete_many({})
        Index.documentsCollection.delete_many({})

    @staticmethod
    def set_word_queue(w_queue):
        Index.word_queue = w_queue

    @staticmethod
    def set_locks(lock,lock2):
        Index.word_queue_lock = lock
        Index.mini_index_queue_lock = lock2

    @staticmethod
    def set_mini_index(index):
        Index.miniIndex = index

    # query

    @staticmethod
    def topkDocuments(query):
        start = time.time()
        print("----------")
        accumulators = []
        N = Index.documentsCollection.find().count()
        for term in query:
            # search for term in database , and store in variable var the term's data
            var = Index.collection.find_one({"_id": term}, {"sumOfDocuments" : 1, "nameTf" : 1})
            # if term is not in database , program checks the next term of the query
            if var is None:
                continue
            idf = log(N / var.get("sumOfDocuments"))
            documents = var.get("nameTf")
            for document in documents:
                docName = document["name"]
                # if accumulator does not exist for a term, create one.
                if next((item for item in accumulators if item["name"] == docName), None) is None:
                    accumulators.append({"name" : docName, "value" : 0})

                tf = document["tf"]
                # check if an accumulator already exists and updates him
                x = next(item for item in accumulators if item["name"] == docName)
                x.update({"value": x.get("value") + (tf * idf)})

        # normalization with Td(number of unique terms in a document)
        for item in accumulators:
            # search database for the document name and store in variable td the Td of the document
            td = Index.documentsCollection.find_one({"_id": item["name"]}, {"_id": 0, "Td": 1})
            x = item["value"]
            # normalize document score
            item.update({"value" : x/(td.get("Td"))})

        # sorting
        sortedAccumulators = sorted(accumulators, key=itemgetter('value'), reverse=True)

        print("Query time")
        print(time.time() - start)
        print("----------")

        return sortedAccumulators
