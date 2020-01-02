import time
import pymongo
import threading
from Index import Index

class miniIndexPusher (Index,threading.Thread):
    nof_pages = 0

    def __init__(self):
        Index.__init__(self)

    # creates an object for each word that represents :
    # 1) name of a document a word is contained
    # 2) the word frequency in that document

    @staticmethod
    def set_page_number(n):
        miniIndexPusher.nof_pages = n

    def createEmbeddedObject(self, nameOfDocument, tf):

        embeddedObject = {
            "name": nameOfDocument,
            "tf": tf
        }
        return embeddedObject

    # creates and pushes a new miniIndex entry to miniIndex
    def createMiniIndexEntry(self, word, embeddedObject):

        miniIndexEntry = ({"_id": word,
                           "nameTf": [embeddedObject]})  # "sumOfDocuments": 1,  # nomizw einai perito pleon auto alla vlepoume

        Index.miniIndex.append(miniIndexEntry)


    def updateMiniIndex(self):

        # checks if word_queue is empty
        if not Index.word_queue:
            time.sleep(0.5)
        else:
            miniIndexPusher.nof_pages -= 1
            # Index.mini_count -= 1
            # +lock!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            Index.word_queue_lock.acquire()
            nextEntry = Index.word_queue.pop(0)
            Index.word_queue_lock.release()
            title = nextEntry.get("link")
            words = nextEntry.get("words")

            # {term 1 : tf , term 2 : tf}
            tfDict = dict((x, words.count(x)) for x in set(words))

            # update unique terms count of a document
            Td = {
                "_id": title,
                "Td": len(tfDict.keys())
            }
            Index.documentsCollection.insert_one(Td)


            # for each word in a document
            for word in tfDict.keys():

                embObj = self.createEmbeddedObject(title, tfDict.get(word))

                Index.mini_index_queue_lock.acquire()
                self.createMiniIndexEntry(word, embObj)
                Index.mini_index_queue_lock.release()


    def printMiniIndex(self):
        time.sleep(5)
        print(len(Index.miniIndex))

    def run(self):
        time.sleep(3)
        while miniIndexPusher.nof_pages > 0:
            # while Index.mini_count > 0:
            #     print(Index.mini_count)
            self.updateMiniIndex()
            self.printMiniIndex()
            # Index.mini_count = Index.mini_size

