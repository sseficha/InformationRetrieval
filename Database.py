import pymongo
import time
from pymongo import MongoClient
import threading


# updateMiniIndex-> locks only for append
# updateIndex->anoigeis vasi kai meta olo ena lock...oso adiazeis mini index

class Database(threading.Thread):
    cluster = MongoClient(
        "mongodb+srv://chris:12340987@cluster0-i10z0.azure.mongodb.net/test?retryWrites=true&w=majority")
    db = cluster["InformationRetrieval"]
    collection = db["Indexer"]
    documentsCollection = db["Documents"]
    miniIndex = []
    word_queue_lock = None

    def __init__(self):
        threading.Thread.__init__(self)

    @staticmethod
    def set_mini_index(index):
        Database.miniIndex = index

    def updateIndex(self):


        start = time.time()
        for termObject in Database.miniIndex:
            term = termObject.get("_id")
            DocsWithTf = termObject.get("nameTf")
            Database.collection.find_one_and_update(
                {"_id": term},
                {"$inc" : {"sumOfDocuments": len(DocsWithTf)},"$push" : {"nameTf" : {"$each" : DocsWithTf }}},
                upsert = True
            )
        print(time.time() - start)

        # setup for next call of updateMiniIndex
        Database.miniIndex.clear()


    def run(self):
        time.sleep(5)
        print("Start updating Index")
        self.updateIndex()
        print('End of Index run!!!!')
            # if not self.word_queue:
        #     print("Start updating Index")
        #     self.updateIndex()
        # print('End of Index run!!!!')

