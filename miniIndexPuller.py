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
        Index().__init__()
        threading.Thread.__init__(self)


    def updateIndex(self):

        if not Index.miniIndex:
            time.sleep(1)
        else:
            Index.nof_pages -= 1
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




    def run(self):
        while Index.nof_pages  > 0:
            print("Start updating Index")
            self.updateIndex()
            print('End of Index run!!!!')
            time.sleep(2)

