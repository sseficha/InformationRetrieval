import time
import pymongo
import threading
from Index import Index
from pymongo import errors

class miniIndexPusher (Index,threading.Thread):
    nof_pages = 0

    def __init__(self):
        Index.__init__(self)


    @staticmethod
    def set_page_number(n):
        miniIndexPusher.nof_pages = n

    # creates an object for a word that represents :
    # 1) name of a document a word is contained
    # 2) the word's frequency in that document

    def createEmbeddedObject(self, nameOfDocument, tf):

        embeddedObject = {
            "name": nameOfDocument,
            "tf": tf
        }
        return embeddedObject

    # creates and pushes a new miniIndex entry to miniIndex
    def createMiniIndexEntry(self, word, embeddedObject):

        miniIndexEntry = ({"_id": word,
                           "nameTf": [embeddedObject]})

        Index.miniIndex.append(miniIndexEntry)


    def updateMiniIndex(self):

        # checks if word_queue is empty
        if not Index.word_queue:
            time.sleep(0.5)
        else:
            miniIndexPusher.nof_pages -= 1
            Index.word_queue_lock.acquire()
            nextEntry = Index.word_queue.pop(0)
            Index.word_queue_lock.release()
            title = nextEntry.get("link")
            words = nextEntry.get("words")

            # {term 1 : tf , term 2 : tf , term3 ...}
            tfDict = dict((x, words.count(x)) for x in set(words))

            # store unique terms count of a document
            Td = {
                "_id": title,
                "Td": len(tfDict.keys())
            }

            try:
                # updates collection Documents of database that holds the information for normalization at the query.
                Index.documentsCollection.insert_one(Td)

                # for each word in a document
                for word in tfDict.keys():
                    embObj = self.createEmbeddedObject(title, tfDict.get(word))

                    Index.mini_index_queue_lock.acquire()
                    self.createMiniIndexEntry(word, embObj)
                    Index.mini_index_queue_lock.release()

            # a site might have a link to itself or crawlers might find the same link again.
            except pymongo.errors.DuplicateKeyError:
                pass


    def run(self):
        time.sleep(3)
        while miniIndexPusher.nof_pages > 0:
            self.updateMiniIndex()
            print("number of documents left to be processed : ", miniIndexPusher.nof_pages)


