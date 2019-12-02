import pymongo
from pymongo import MongoClient
from numpy import log

#updateMiniIndex-> locks only for append
#updateIndex->anoigeis vasi kai meta olo ena lock...oso adiazeis mini index

class Index:

    '''Σύνδεση με MongoDB'''
    # word_queue, word_queue_lock
    def __init__(self, word_queue, word_queue_lock):
        self.cluster = MongoClient("mongodb+srv://chris:12340987@cluster0-i10z0.azure.mongodb.net/test?retryWrites=true&w=majority")
        self.db = self.cluster["InformationRetrieval"]
        self.collection = self.db["Indexer"]
        self.found = []
        self.miniIndex = []
        self.nofExtraDocs = 0
        self.word_queue = word_queue
        self.word_queue_lock = word_queue_lock
        self.collection.delete_many({})

    def updateIndex(self):

        list_of_terms = list(self.collection.find().distinct("_id"))

        '''
        Ενημέρωση του ανεστραμμένου καταλόγου
        '''
        # lock
        for post in self.miniIndex:
            term = post.get("_id")
            if term in list_of_terms:
                list_to_append = post.get("docPos")
                self.collection.update_one({"_id":term},{"$inc" : {"counter":len(list_to_append)}})
                for doc in list_to_append:
                    self.collection.update_one({"_id":term},{"$push" :{"docPos":doc}})
            else:
                list_of_terms.append(term)
                self.collection.insert_one(post)

        #update for N of IDF
        self.collection.update_one({"_id": "NumOfDocumentsInBase"}, {"$inc": {"count": self.nofExtraDocs}},upsert=True)

        self.miniIndex.clear()
        self.found.clear()
        self.nofExtraDocs = 0
        # lock

    def updateMiniIndex(self):

        # self.miniIndex
        # acquire
        # item=pop(0) from word_queue  item['link']->string item['words']->array of words
        # release
        #for in words
        #search in mini_index
        #acquiree
        #append
        #release

        #checks if word_queue is empty
        if not self.word_queue:
            pass
        else:

            next = self.word_queue.pop(0)
            title = next.get("link")
            wordsInList = next.get("words")
            # {term 1 : tf , term 2 : tf}
            tfDict = dict((x, wordsInList.count(x)) for x in set(wordsInList))
            for word in tfDict.keys():
                embeddedDict = {
                    "nameDoc": title,
                    "tf": tfDict.get(word)
                }
                # found is a list that stores all words a crawler sends to miniIndex before each update to db
                if word not in self.found:          #sorted # future commit
                    miniIndexEntry = ({"_id": word,
                                       "counter": 1,
                                       "docPos": [embeddedDict]})
                    self.miniIndex.append(miniIndexEntry)
                    self.found.append(word)
                else:
                    for pos in self.miniIndex:
                        if pos.get("_id") == word:
                            namedocs = []
                            x = pos.get("docPos", {})
                            for y in x:
                                namedocs.append(y.get("nameDoc"))
                            if embeddedDict.get("nameDoc") not in namedocs:
                                x.append(embeddedDict)
                                pos["counter"] += 1
            #update N for idf
            self.nofExtraDocs += 1

    def print_posts(self):
        for x in self.miniIndex:
            print(x)

    #top-k (slides)
    def top_k_documents(self,query):
        C = {}
        list_of_terms = list(self.collection.find().distinct("_id"))
        list_of_terms.remove("NumOfDocumentsInBase")
        num = self.collection.find({"_id": "NumOfDocumentsInBase"}).distinct("count")
        N = num[0]
        for term in query:
            if term in list_of_terms:
                nt = self.collection.find({"_id": term}).distinct("counter")
                idf = log(N/nt[0])
                documents = self.collection.find({"_id": term}).distinct("docPos")
                for docu in documents:
                    mydoc = docu["nameDoc"]
                    if mydoc not in C.keys():
                        C.update({mydoc : 0})
                    tf = docu["tf"]
                    x = C.get(mydoc)
                    C.update({mydoc : x + (tf * idf)})

            #leipei kanonikopoihsh sth monada ousiastika ti einai to Ld


# ind.top_k_documents(["the","ahahahha","and","company"])
# ind = Index()
# ind.updateIndex
# ind.print_posts()
# ind.update_indexer()
# ind.print_posts()
# ind.update_indexer()



