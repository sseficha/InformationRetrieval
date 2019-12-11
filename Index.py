import pymongo
import time
from pymongo import MongoClient
from numpy import log
import threading


# updateMiniIndex-> locks only for append
# updateIndex->anoigeis vasi kai meta olo ena lock...oso adiazeis mini index

class Index(threading.Thread):
    cluster = MongoClient(
        "mongodb+srv://chris:12340987@cluster0-i10z0.azure.mongodb.net/test?retryWrites=true&w=majority")
    db = cluster["InformationRetrieval"]
    collection = db["Indexer"]
    documentsCollection = db["Documents"]
    mini_size = 0
    mini_count = 0
    miniIndex = []
    word_queue = []
    mini_index_queue_lock = None
    word_queue_lock = None
    mini_index_pull_lock = None

    def __init__(self):
        threading.Thread.__init__(self)


    @staticmethod
    def clear():
        Index.collection.delete_many({})
        Index.documentsCollection.delete_many({})
    #
    # @staticmethod
    # def set_mini_size(n):
    #     Index.mini_size = n
    #     Index.mini_count = n

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

    # top-k taksinomimena

    @staticmethod
    def topkDocuments(query):
        start = time.time()
        print(start)
        C = []
        N = Index.documentsCollection.find().count()
        for term in query:
            nt = Index.collection.find_one({"_id": term}, {"sumOfDocuments" : 1, "nameTf" : 1})
            if nt is None:
                print("word not in db")
                continue
            idf = log(N / nt.get("sumOfDocuments"))  # N*nt
            documents = nt.get("nameTf")
            for document in documents:
                docName = document["name"]
                if next((item for item in C if item["name"] == docName), None) is None:
                    C.append({"name" : docName, "value" : 0})
                tf = document["tf"]
                x = next(item for item in C if item["name"] == docName)
                x.update({"value": x.get("value") + (tf * idf)})

        # normalization with Td(number of unique terms in a document)
        for item in C:
            td = Index.documentsCollection.find_one({"_id": item["name"]}, {"_id": 0, "Td": 1})
            x = item["value"]
            print(td.get("Td"))
            item.update({"value" : x/(td.get("Td"))})
        print(C)
        print(time.time() - start)
        return C
