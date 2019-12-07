import pymongo
import time
from pymongo import MongoClient
from numpy import log
import threading


# ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣀⣀⣀⣀⣀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⠀⠀⠀⠀⣀⣤⣶⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶⣤⣀⠀⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⠀⠀⣠⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣄⠀⠀⠀⠀⠀
# ⠀⠀⠀⢠⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⡄⠀⠀⠀
# ⠀⠀⢠⣿⡿⠿⠿⠿⠿⠿⠿⠿⣿⠿⠿⠿⠿⠿⠿⢿⣿⣿⣿⠿⠿⢿⣿⡄⠀⠀
# ⠀⢀⣿⣿⡇⠀⠀⣠⣤⣄⣀⣠⣿⠀⠀⢀⣤⣀⡀⠀⠘⣿⣿⠀⠀⢸⣿⣿⡀⠀
# ⠀⢸⣿⣿⡇⠀⠀⣿⣿⣿⣿⣿⣿⠀⠀⢸⣿⣿⠟⠀⠀⣿⣿⠀⠀⢸⣿⣿⡇⠀
# ⠀⢸⣿⣿⡇⠀⠀⠀⠀⠀⠀⢸⣿⠀⠀⠀⠀⠀⠀⠀⠺⣿⣿⣿ ⠀ ⢸⣿⣿⡧⠀
# ⠀⢸⣿⣿⡇⠀⠀⣿⣿⣿⣿⣿⣿⠀⠀⢸⣿⣿⣿⠀⠀⢹⣿⠀⠀⢸⣿⣿⡇⠀ Locks sto mini index
# ⠀⠈⣿⣿⡇⠀⠀⣿⣿⣿⣿⣿⣿⠀⠀⠈⠛⠛⠉⠀⢀⣾⣿⠀⠀⢸⣿⣿⠃⠀
# ⠀⠀⠸⣿⣷⣶⣶⣿⣿⣿⣿⣿⣿⣶⣶⣶⣶⣶⣶⣾⣿⣿⣿⣶⣶⣾⣿⠇⠀⠀
# ⠀⠀⠀⠘⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠋⠀⠀⠀
# ⠀⠀⠀⠀⠀⠛⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠁⠀⠀⠀⠀
# ⠀⠀⠀⠀⠀⠀⠀⠙⠻⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠋⠀⠀⠀⠀⠀⠀⠀
# ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠉⠙⠛⠛⠋⠉⠉⠀⠀⠀⠀⠀⠀


# updateMiniIndex-> locks only for append
# updateIndex->anoigeis vasi kai meta olo ena lock...oso adiazeis mini index

class Index(threading.Thread):
    cluster = MongoClient(
        "mongodb+srv://chris:12340987@cluster0-i10z0.azure.mongodb.net/test?retryWrites=true&w=majority")
    db = cluster["InformationRetrieval"]
    collection = db["Indexer"]
    documentsCollection = db["Documents"]
    nof_pages = 0
    mini_size = 0
    mini_count = 0
    miniIndex = []
    word_queue = []
    word_queue_lock = None

    def __init__(self):
        threading.Thread.__init__(self)


    @staticmethod
    def clear():
        Index.collection.delete_many({})
        Index.documentsCollection.delete_many({})

    @staticmethod
    def set_page_number(n):
        Index.nof_pages = n

    @staticmethod
    def set_mini_size(n):
        Index.mini_size = n
        Index.mini_count = n

    @staticmethod
    def set_word_queue(w_queue):
        Index.word_queue = w_queue

    @staticmethod
    def set_word_queue_lock(lock):
        Index.word_queue_lock = lock

    @staticmethod
    def set_mini_index(index):
        Index.miniIndex = index

    def updateIndex(self):

        start = time.time()
        for termObject in Index.miniIndex:
            term = termObject.get("_id")
            DocsWithTf = termObject.get("nameTf")
            Index.collection.find_one_and_update(
                {"_id": term},
                {"$inc" : {"sumOfDocuments": len(DocsWithTf)},"$push" : {"nameTf" : {"$each" : DocsWithTf }}},
                upsert = True
            )
        print(time.time() - start)

        # setup for next call of updateMiniIndex
        Index.miniIndex.clear()

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
                           "sumOfDocuments": 1,   # nomizw einai perito pleon auto alla vlepoume
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

    # ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣀⣀⣀⣀⣀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
    # ⠀⠀⠀⠀⠀⠀⠀⣀⣤⣶⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶⣤⣀⠀⠀⠀⠀⠀⠀⠀
    # ⠀⠀⠀⠀⠀⣠⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣄⠀⠀⠀⠀⠀
    # ⠀⠀⠀⢠⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⡄⠀⠀⠀
    # ⠀⠀⢠⣿⡿⠿⠿⠿⠿⠿⠿⠿⣿⠿⠿⠿⠿⠿⠿⢿⣿⣿⣿⠿⠿⢿⣿⡄⠀⠀
    # ⠀⢀⣿⣿⡇⠀⠀⣠⣤⣄⣀⣠⣿⠀⠀⢀⣤⣀⡀⠀⠘⣿⣿⠀⠀⢸⣿⣿⡀⠀
    # ⠀⢸⣿⣿⡇⠀⠀⣿⣿⣿⣿⣿⣿⠀⠀⢸⣿⣿⠟⠀⠀⣿⣿⠀⠀⢸⣿⣿⡇⠀
    # ⠀⢸⣿⣿⡇⠀⠀⠀⠀⠀⠀⢸⣿⠀⠀⠀⠀⠀⠀⠀⠺⣿⣿⣿ ⠀⠀⣿⣿⡧⠀
    # ⠀⢸⣿⣿⡇⠀⠀⣿⣿⣿⣿⣿⣿⠀⠀⢸⣿⣿⣿⠀⠀⢹⣿⠀⠀⢸⣿⣿⡇⠀ Locks sto mini index
    # ⠀⠈⣿⣿⡇⠀⠀⣿⣿⣿⣿⣿⣿⠀⠀⠈⠛⠛⠉⠀⢀⣾⣿⠀⠀⢸⣿⣿⠃⠀
    # ⠀⠀⠸⣿⣷⣶⣶⣿⣿⣿⣿⣿⣿⣶⣶⣶⣶⣶⣶⣾⣿⣿⣿⣶⣶⣾⣿⠇⠀⠀
    # ⠀⠀⠀⠘⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠋⠀⠀⠀
    # ⠀⠀⠀⠀⠀⠛⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠁⠀⠀⠀⠀
    # ⠀⠀⠀⠀⠀⠀⠀⠙⠻⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠋⠀⠀⠀⠀⠀⠀⠀
    # ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠉⠙⠛⠛⠋⠉⠉⠀⠀⠀⠀⠀⠀

    def updateMiniIndex(self):

        # checks if word_queue is empty
        if not Index.word_queue:
            time.sleep(0.5)
        else:
            Index.nof_pages -= 1
            Index.mini_count -= 1
            #+lock!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            Index.word_queue_lock.acquire()
            nextEntry = Index.word_queue.pop(0)
            Index.word_queue_lock.release()
            title = nextEntry.get("link")
            words = nextEntry.get("words")
            Td = {
                "_id" : title,
                "Td" : len(words)
            }
            Index.documentsCollection.insert_one(Td)

            # {term 1 : tf , term 2 : tf}
            tfDict = dict((x, words.count(x)) for x in set(words))

            # for each word in a document
            for word in tfDict.keys():

                embObj = self.createEmbeddedObject(title, tfDict.get(word))

                # checks if the word is already in miniIndex or miniIndex is empty (for the first word only)
                if word not in [Index.miniIndex[i]["_id"] for i in range(len(Index.miniIndex))] or not Index.miniIndex:

                    self.createMiniIndexEntry(word, embObj)

                else:
                    for termObject in Index.miniIndex:  # maybe there is a different implementation
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
            # if not self.word_queue:
            print("Start updating Index")
            self.updateIndex()
        print('End of Index run!!!!')

    # top-k slides, not changed
    # not ready yet
    def topkDocuments(query):
        C = {}
        N = Index.documentsCollection.find().count()
        for term in query:
            if Index.collection.find_one({"_id": term}) is not None:
                nt = Index.collection.find({"_id": term}).distinct("sumOfDocuments")
                idf = log(N / nt[0])  # N*nt
                #idf = N / nt[0]
                documents = Index.collection.find({"_id": term}).distinct("nameTf")
                for docu in documents:
                    mydoc = docu["name"]
                    if mydoc not in C.keys():
                        C.update({"name" : mydoc, "value" : 0})
                    tf = docu["tf"]
                    x = C.get("value")
                    C.update({"value": x + (tf * idf)})
        print(C)
        return C

            # leipei kanonikopoihsh sth monada ousiastika ti einai to Ld
