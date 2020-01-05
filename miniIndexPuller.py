import pymongo
import time
import threading
from Index import Index


class miniIndexPuller(Index, threading.Thread):

    nof_pages = 0

    def __init__(self):
        Index.__init__(self)
        threading.Thread.__init__(self)

    @staticmethod
    def set_page_number(n):
        miniIndexPuller.nof_pages = n

    # updates database
    def updateIndex(self, t):

        if not Index.miniIndex:
            time.sleep(0.5)
            t += 2
        else:
            Index.mini_index_queue_lock.acquire()
            termObject = Index.miniIndex.pop(0)
            Index.mini_index_queue_lock.release()
            term = termObject.get("_id")
            DocsWithTf = termObject.get("nameTf")
            Index.collection.find_one_and_update(
                {"_id": term},
                {"$inc" : {"sumOfDocuments": 1},"$push" : {"nameTf" : {"$each" : DocsWithTf }}},
                upsert = True
            )
            t = 0
        return t


    def run(self):
        # t is incremented everytime miniindex is empty.If mini index is empty for some time program stops,because it means that crawlers ended their work.
        t = 0
        time.sleep(5)
        while t < 20:
            t = self.updateIndex(t)

