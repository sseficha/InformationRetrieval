import bisect
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
    nof_pages = 0
    mini_size = 0
    mini_count = 0
    miniIndex = []
    word_queue = []
    word_queue_lock = None

    def __init__(self):
        threading.Thread.__init__(self)

        self.nofDocs = 0  # des ti tha valeis edo gia tin ora pernaei panta 0



    @staticmethod
    def clear():
        Index.collection.delete_many({})

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
            if Index.collection.find_one({"_id": term}) is not None:
                DocsWithTf = termObject.get("nameTf")
                Index.collection.update_one({"_id": term}, {"$inc": {"sumOfDocuments": len(DocsWithTf)}})
                for document in DocsWithTf:
                    Index.collection.update_one({"_id": term}, {"$push": {"nameTf": document}})
            else:
                Index.collection.insert_one(termObject)

        # update the number of different documents(links) in collection
        Index.collection.update_one({"_id": "NumOfDocumentsInBase"}, {"$inc": {"count": self.nofDocs}},
                                   upsert=True)
        print(time.time() - start)

        # setup for next call of updateMiniIndex
        Index.miniIndex.clear()
        self.nofDocs = 0

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
                           "sumOfDocuments": 1,
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
            #+lock!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            Index.word_queue_lock.acquire()
            nextEntry = Index.word_queue.pop(0)
            Index.word_queue_lock.release()
            title = nextEntry.get("link")
            words = nextEntry.get("words")

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

            # update N for idf


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

    def topkDocuments(query):
        C = {}
        list_of_terms = list(Index.collection.find().distinct("_id"))
        # list_of_terms.remove("NumOfDocumentsInBase")
        #num = self.collection.find({"_id": "NumOfDocumentsInBase"}).distinct("count")
        #N = num[0]
        N=4
        for term in query:
            if term in list_of_terms:
                nt = Index.collection.find({"_id": term}).distinct("sumOfDocuments")
                idf = log(N / nt[0])  # N*nt
                #idf = N / nt[0]
                documents = Index.collection.find({"_id": term}).distinct("nameTf")
                for docu in documents:
                    mydoc = docu["name"]
                    if mydoc not in C.keys():
                        C.update({mydoc: 0})
                    tf = docu["tf"]
                    x = C.get(mydoc)
                    C.update({mydoc: x + (tf * idf)})
        print(C)
        return C

            # leipei kanonikopoihsh sth monada ousiastika ti einai to Ld
