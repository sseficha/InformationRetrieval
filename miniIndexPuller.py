import pymongo
import time
from pymongo import MongoClient
import threading
from Index import Index


# updateMiniIndex-> locks only for append
# updateIndex->anoigeis vasi kai meta olo ena lock...oso adiazeis mini index

class miniIndexPuller(Index, threading.Thread):

    nof_pages = 0

    def __init__(self):
        Index.__init__(self)
        threading.Thread.__init__(self)

    @staticmethod
    def set_page_number(n):
        miniIndexPuller.nof_pages = n

    def updateIndex(self, t):

        if not Index.miniIndex:
            time.sleep(0.5)
            t += 1
        else:
            start = time.time()
            termObject = Index.miniIndex.pop(0)
            term = termObject.get("_id")
            DocsWithTf = termObject.get("nameTf")
            Index.collection.find_one_and_update(
                {"_id": term},
                {"$inc" : {"sumOfDocuments": len(DocsWithTf)},"$push" : {"nameTf" : {"$each" : DocsWithTf }}},
                upsert = True
            )
            print(time.time() - start)
            t = 0
        return t


    def run(self):
        t = 0
        while t < 20:
            print("Start updating Index")
            t = self.updateIndex(t)
            print('End of Index run!!!!')


