import pymongo
from pymongo import MongoClient
from collections import defaultdict


class Index:

    '''Σύνδεση με MongoDB'''

    def __init__(self):
        self.cluster = MongoClient("mongodb+srv://chris:12340987@cluster0-i10z0.azure.mongodb.net/test?retryWrites=true&w=majority")
        self.db = self.cluster["InformationRetrieval"]
        self.collection = self.db["Indexer"]
        # self.doc_names = self.db["doc_names"]
        self.di = {}
        # self.collection.delete_many({})
    def update_indexer(self, data):

        to_be_updated = self.create_inverted_index_in_ram(data)

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
                for doc in list_to_append:
                    self.collection.update_one({"_id":term},{"$push" :{"docPos":doc}})
            else:
                list_of_terms.append(term)
                self.collection.insert_one(x)
        self.di.clear()
    def create_inverted_index_in_ram(self,data):
        titles = []
        content = []
        for i, line in enumerate(data):
            if i % 2 == 0:
                titles.append(line)
            else:
                content.append(line)

        words_in_list = []
        for phrase in content:
            li = phrase.split(" ")
            words_in_list.append(li)

        for i in range(len(titles)):
            self.di.update({titles[i] : words_in_list[i]})

        #creation of index ,will put comments later

        alist = []
        found = []
        posts = []
        for title in titles:
            s = []
            d = defaultdict(list)
            for i,word in enumerate(self.di.get(title)):
                s.append((word,i))
            for k,v in s:
                d[k].append(v)
            alist.append(d)
            for word in(self.di.get(title)):
                embeddedDict = {
                    "nameDoc": title,
                    "tf": len(d.get(word)),
                    "positions": d.get(word)
                }
                if word not in found:
                    post = ({"_id": word,
                             "counter": 1,
                             "docPos": [embeddedDict]})
                    posts.append(post)
                    found.append(word)
                else:
                    for pos in posts:
                       namedocs = []
                       if pos.get("_id") == word:
                            x = pos.get("docPos",{})
                            for y in x:
                                namedocs.append(y.get("nameDoc"))
                            if embeddedDict.get("nameDoc") not in namedocs:
                                x.append(embeddedDict)

        # for x in posts:
        #     print(x)

        return posts

    #top-k (in progress)
    def top_k_documents(self,query):
        C = []
        list_of_terms = list(self.collection.find().distinct("_id"))
        for term in query:
            if term in list_of_terms:
                nt = self.collection.find({"_id":term}).distinct("counter")
                N = self.collection.count_documents({})
                print(N)

with open("document.txt", "r") as doc:
    #different documents
    data = doc.readlines()

ind = Index()
ind.update_indexer(data)
#query = ["information","retrieval","and"]
#ind.top_k_documents(query)

