import pymongo
from pymongo import MongoClient
from collections import defaultdict


class Index:

    '''Σύνδεση με MongoDB'''

    def __init__(self):
        self.cluster = MongoClient("mongodb+srv://chris:12340987@cluster0-i10z0.azure.mongodb.net/test?retryWrites=true&w=majority")
        self.db = self.cluster["InformationRetrieval"]
        self.collection = self.db["Indexer"]
        self.di = {}
        self.alist = []
        self.found = []
        self.posts = []
        # self.collection.delete_many({})
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
                for doc in list_to_append:
                    self.collection.update_one({"_id":term},{"$push" :{"docPos":doc}})
            else:
                list_of_terms.append(term)
                self.collection.insert_one(x)

        self.di.clear()
        self.posts.clear()
        self.alist.clear()
        self.found.clear()

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
        # for x in words_in_list:
        #     print(x)

        for i in range(len(titles)):
            self.di.update({titles[i] : words_in_list[i]})
        # print(self.di)
        # print("=====================")
        #creation of index ,will put comments later


        for title in titles:
            s = []
            d = defaultdict(list)
            for i,word in enumerate(self.di.get(title)):
                s.append((word,i))
            for k,v in s:
                d[k].append(v)
            self.alist.append(d)
            for word in(self.di.get(title)):
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
                            x = pos.get("docPos",{})
                            for y in x:
                                namedocs.append(y.get("nameDoc"))
                            if embeddedDict.get("nameDoc") not in namedocs:
                                x.append(embeddedDict)

    def print_posts(self):
        for x in self.posts:
            print(x)
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
ind.create_inverted_index_in_ram(data)
ind.create_inverted_index_in_ram(data3)
ind.create_inverted_index_in_ram(data2)
ind.print_posts()
ind.update_indexer()
ind.create_inverted_index_in_ram(data4)
ind.print_posts()
ind.update_indexer()

#query = ["information","retrieval","and"]
#ind.top_k_documents(query)

