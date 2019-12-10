import pymongo
import time
from pymongo import MongoClient
from numpy import log
import threading
from Index import Index

class miniIndexPusher (Index,threading.Thread):
    nof_pages = 0

    def __init__(self):
        Index().__init__()

    # creates an object for each word that represents :
    # 1) name of a document a word is contained
    # 2) the word frequency in that document
    def createEmbeddedObject(self, nameOfDocument, tf):

        embeddedObject = {
            "name": nameOfDocument,
            "tf": tf
        }
        return embeddedObject

    # creates and pushes a new miniIndex entry to miniIndex
    def createMiniIndexEntry(self, word, embeddedObject):

        miniIndexEntry = ({"_id": word,
                           "sumOfDocuments": 1,  # nomizw einai perito pleon auto alla vlepoume
                           "nameTf": [embeddedObject]})

        Index.miniIndex.append(miniIndexEntry)

    # updates a term that already exists in miniIndex
    def updateMiniIndexEntry(self, termObject, embObj):

        # documentNames = [document1, document2, ...]
        documentNames = []

        # namesAntTfs structure = {document1 : tf, document2 : tf, ...}
        namesAndTfs = termObject.get("nameTf", {})

        for item in namesAndTfs:
            documentNames.append(item.get("name"))

        # if a word is in the same document more than once, there is no need for update,
        # because embeddedObject already holds all the info for a word in a document
        if embObj.get("name") not in documentNames:
            namesAndTfs.append(embObj)
            termObject["sumOfDocuments"] += 1


    def updateMiniIndex(self):

        # checks if word_queue is empty
        if not Index.word_queue:
            time.sleep(0.5)
        else:
            Index.nof_pages -= 1
            Index.mini_count -= 1
            # +lock!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            Index.word_queue_lock.acquire()
            nextEntry = Index.word_queue.pop(0)
            Index.word_queue_lock.release()
            print("===========================================================")
            print(nextEntry)
            title = nextEntry.get("link")
            words = nextEntry.get("words")
            Td = {
                "_id": title,
                "Td": len(words)  #allagi
            }
            Index.documentsCollection.insert_one(Td)

            # {term 1 : tf , term 2 : tf}
            tfDict = dict((x, words.count(x)) for x in set(words))

            # for each word in a document
            for word in tfDict.keys():

                embObj = self.createEmbeddedObject(title, tfDict.get(word))

                # checks if the word is already in miniIndex or miniIndex is empty (for the first word only)
                if word not in [Index.miniIndex[i]["_id"] for i in range(len(Index.miniIndex))] or not Index.miniIndex:
                    Index.mini_index_queue_lock.acquire()
                    self.createMiniIndexEntry(word, embObj)
                    Index.mini_index_queue_lock.release()

                else:
                    for termObject in Index.miniIndex:
                        if termObject.get("_id") == word:
                            self.updateMiniIndexEntry(termObject, embObj)
                            break

    def printMiniIndex(self):
        for x in Index.miniIndex:
            print(x)

    def run(self):
        time.sleep(3)
        while Index.nof_pages > 0:
            while Index.mini_count > 0:
                print(Index.mini_count)
                self.updateMiniIndex()
            Index.mini_count = Index.mini_size

