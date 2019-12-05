import bisect
import time
from pymongo import MongoClient
from numpy import log


# updateMiniIndex-> locks only for append
# updateIndex->anoigeis vasi kai meta olo ena lock...oso adiazeis mini index

class Index():

    def __init__(self, word_queue, word_queue_lock):
        self.cluster = MongoClient("mongodb+srv://chris:12340987@cluster0-i10z0.azure.mongodb.net/test?retryWrites=true&w=majority")
        self.db = self.cluster["InformationRetrieval"]
        self.collection = self.db["Indexer"]
        self.miniIndex = []
        self.numOfDocsToBeAdded = 0
        self.word_queue = word_queue
        # self.collection.delete_many({})

    def updateIndex(self):

        start = time.time()
        for termObject in self.miniIndex:
            term = termObject.get("_id")
            if self.collection.find_one({"_id": term}) is not None:
                DocsWithTf = termObject.get("nameTf")
                self.collection.update_one({"_id": term}, {"$inc": {"sumOfDocuments": len(DocsWithTf)}})
                for document in DocsWithTf:
                    self.collection.update_one({"_id": term}, {"$push": {"nameTf": document}})
            else:
                self.collection.insert_one(termObject)

        # update the number of different documents(links) in collection
        self.collection.update_one({"_id": "NumOfDocumentsInBase"}, {"$inc": {"count": self.numOfDocsToBeAdded}},
                                   upsert=True)
        print(time.time() - start)

        # setup for next call of updateMiniIndex
        self.miniIndex.clear()
        self.numOfDocsToBeAdded = 0

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

        self.miniIndex.append(miniIndexEntry)

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
        if not self.word_queue:
            pass
        else:
            nextEntry = self.word_queue.pop(0)
            title = nextEntry.get("link")
            theWords = nextEntry.get("words")

            # {term 1 : tf , term 2 : tf}
            tfDict = dict((x, theWords.count(x)) for x in set(theWords))

            # for each word in a document
            for word in tfDict.keys():

                embObj = self.createEmbeddedObject(title, tfDict.get(word))

                # checks if the word is already in miniIndex or miniIndex is empty (for the first word only)
                if word not in [self.miniIndex[i]["_id"] for i in range(len(self.miniIndex))] or not self.miniIndex:

                    self.createMiniIndexEntry(word, embObj)

                else:
                    for termObject in self.miniIndex:          # maybe there is a different implementation
                        if termObject.get("_id") == word:
                            self.updateMiniIndexEntry(termObject, embObj)
                            break

            # update N for idf
            self.numOfDocsToBeAdded += 1

    def printMiniIndex(self):
        for x in self.miniIndex:
            print(x)


    # top-k slides, not changed

    def topKDocuments(self, query):
        C = {}
        list_of_terms = list(self.collection.find().distinct("_id"))
        list_of_terms.remove("NumOfDocumentsInBase")
        num = self.collection.find({"_id": "NumOfDocumentsInBase"}).distinct("count")
        N = num[0]
        for term in query:
            if term in list_of_terms:
                nt = self.collection.find({"_id": term}).distinct("sumOfDocuments")
                idf = log(N / nt[0])  # N*nt
                documents = self.collection.find({"_id": term}).distinct("nameTf")
                for docu in documents:
                    mydoc = docu["name"]
                    if mydoc not in C.keys():
                        C.update({mydoc: 0})
                    tf = docu["tf"]
                    x = C.get(mydoc)
                    C.update({mydoc : x + (tf * idf)})

            #leipei kanonikopoihsh sth monada ousiastika ti einai to Ld


