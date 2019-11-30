import pymongo
from pymongo import MongoClient
from collections import defaultdict
from numpy import log

class Index:

    '''Σύνδεση με MongoDB'''

    def __init__(self):
        self.cluster = MongoClient("mongodb+srv://chris:12340987@cluster0-i10z0.azure.mongodb.net/test?retryWrites=true&w=majority")
        self.db = self.cluster["InformationRetrieval"]
        self.collection = self.db["Indexer"]
        self.alist = []
        self.found = []
        self.posts = []
        self.numberOfCurrentDocumentsToBeUpdated = 0
        self.collection.delete_many({})

    def update_indexer(self):


        to_be_updated = self.posts

        '''
        Paramater:
        -----------------------------------------
        list_of_terms : κάθε φορα που τρέχει το πρόγραμμα τραβάει από τη βάση όλους τους όρους και τους αποθηκεύει στη list_of_terms
        '''

        list_of_terms = list(self.collection.find().distinct("_id"))
        '''
        Ενημέρωση του ανεστραμμένου καταλόγου
        '''

        for x in to_be_updated:
            term = x.get("_id")
            if term in list_of_terms:
                list_to_append = x.get("docPos")
                self.collection.update_one({"_id":term},{"$inc" : {"counter":len(list_to_append)}})
                for doc in list_to_append:
                    self.collection.update_one({"_id":term},{"$push" :{"docPos":doc}})
            else:
                list_of_terms.append(term)
                self.collection.insert_one(x)

        #update for N of IDF
        self.collection.update_one({"_id": "NumOfDocumentsInBase"}, {"$inc": {"count": self.numberOfCurrentDocumentsToBeUpdated}},upsert=True)

        self.posts.clear()
        self.alist.clear()
        self.found.clear()
        self.numberOfCurrentDocumentsToBeUpdated = 0

    def create_inverted_index_in_ram(self,data):
        content = []
        for i, line in enumerate(data):
            if i % 2 == 0:
                title = line
            else:
                content.append(line)

        words_in_list = []
        for phrase in content:
            words_in_list = phrase.split(" ")

        # for x in words_in_list:
        #     print(x)



        # print(self.di)
        # print("=====================")
        #creation of index ,will put comments later


        s = []
        d = defaultdict(list)
        for i,word in enumerate(words_in_list):
            s.append((word,i))
        for k,v in s:
            d[k].append(v)
        self.alist.append(d)
        for word in(words_in_list):
            flag = False
            embeddedDict = {
                "nameDoc": title,
                "tf": len(d.get(word)),
                "positions": d.get(word)
            }
            if word not in self.found:
                post = ({"_id": word,
                         "counter": 1,
                         "docPos": [embeddedDict]})
                self.posts.append(post)
                self.found.append(word)
            else:
                for pos in self.posts:
                    if pos.get("_id") == word:
                        namedocs = []
                        x = pos.get("docPos", {})
                        for y in x:
                            namedocs.append(y.get("nameDoc"))
                        if embeddedDict.get("nameDoc") not in namedocs:
                            x.append(embeddedDict)
                            pos["counter"] += 1

        self.numberOfCurrentDocumentsToBeUpdated += 1

    def print_posts(self):
        for x in self.posts:
            print(x)

    #top-k (in progress)
    def top_k_documents(self,query):
        C = []
        list_of_terms = list(self.collection.find().distinct("_id"))
        list_of_terms.remove("NumOfDocumentsInBase")
        num = self.collection.find({"_id": "NumOfDocumentsInBase"}).distinct("count")
        N = num[0]
        print(N)
        for term in query:
            if term in list_of_terms:
                nt = self.collection.find({"_id": term}).distinct("counter")
                idf = log(N/nt[0])
                print(idf)


with open("document.txt", "r") as doc:
    #different documents
    data = doc.readlines()

with open("document2.txt", "r") as doc:
    #different documents
    data2 = doc.readlines()

with open("document3.txt", "r") as doc:
    #different documents
    data3 = doc.readlines()

with open("document4.txt", "r") as doc:
    #different documents
    data4 = doc.readlines()

ind = Index()
# ind.top_k_documents(["information","need"])
ind.create_inverted_index_in_ram(data)
ind.create_inverted_index_in_ram(data3)
# ind.update_indexer()
ind.create_inverted_index_in_ram(data2)
# ind.print_posts()
ind.update_indexer()
ind.create_inverted_index_in_ram(data4)
# ind.print_posts()
ind.update_indexer()

#query = ["information","retrieval","and"]
#ind.top_k_documents(query)

