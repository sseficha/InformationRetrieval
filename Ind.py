import pymongo
from pymongo import MongoClient
from collections import defaultdict


class Index:

    '''Σύνδεση με MongoDB'''

    def __init__(self):
        self.cluster = MongoClient("mongodb+srv://chris:12340987@cluster0-i10z0.azure.mongodb.net/test?retryWrites=true&w=majority")
        self.db = self.cluster["InformationRetrieval"]
        self.collection = self.db["Indexer"]
        self.doc_names = self.db["doc_names"]
        self.di = {}
        self.collection.delete_many({})
    def update_indexer(self, data):

        #gemizei to di ,lockaroun ta threads, travaei olo to data i vasi ,adeiazei to di

        self.create_inverted_index_in_ram(data)
        x = 0 # tha gurnaei pisw enan aritmo oste na arxizei apo ekei to kainourgio coubt , episis borei na dimiourgoude pedia xoris na einai embedded
        '''

        Parameters:
        -----------------------------------------
        titles : λίστα που περιέχει τους τίτλους των εγγράφων(1η, 3η, 5η, ... γραμμή του δοκιμαστικού αρχείου document.txt)
        content : λίστα από λιστες, όπου κάθε λίστα αποτελεί το περιεχόμενο ενός εγγράφου
        words_in_list : λίστα που περιέχει τις λέξεις ενός εγγράφου
        Στην αρχή γίνεται μια απλή επεξεργασία της λίστας data για να διαμορφωθούν οι titles,content.Αποτελεί προσωρινή υλοποίηση

        '''
        '''
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

        '''
        '''
        Paramater:
        -----------------------------------------
        list_of_terms : κάθε φορα που τρέχει το πρόγραμμα τραβάει από τη βάση όλους τους όρους και τους αποθηκεύει στη list_of_terms'''

        list_of_terms = list(self.collection.find().distinct("_id"))
        list_of_document_names = list(self.doc_names.find().distinct("_id"))
        '''debugging purposes'''

        #self.collection.delete_many({})
        # x = self.collection.find({"_id" : "information"})
        # for i in x:
        #     print(i)
        '''
        Ενημέρωση του ανεστραμμένου καταλόγου
        
        Parameters:
        -----------------------------------------
        title : ο τίτλος ενός εγγράφου
        word : λέξη ενός εγγράφου
        post : πως αποθηκεύεται στη βάση ένας όρος
            _id : μοναδικό όνομα όρου
            counter : μετράει σε πόσα διαφορετικά έγγραφα εμφανίζεται ένας όρος
            docPos : μια λίστα απο δυάδες της μορφής (τίτλος εγγράφου, θέση στην οποία εμφανίστηκε ο όρος σε αυτό το έγγραφο)
        word_document_titles : λίστα που περιέχει σε ποια έγγραφα(τίτλους) υπάρχει ένας όρος.
            Χρειάζεται για να ξέρουμε αν ένας όρος υπάρχει ήδη μια φορά σε ένα έγγραφο , ώστε να μην αυξηθεί το @counter,
            αλλά μόνο να προσθέσουμε τη θέση στην οποία ξαναβρέθηκε
        '''

        for i, title in enumerate(self.di.keys(),x):
            found = []
            if title not in list_of_document_names:
                list_of_document_names.append(title)
                li = list(self.di.get(title))
                self.doc_names.insert_one({"_id" : title, "terms":li})
            for pos, word in enumerate(list(self.di.get(title))):
                post = ({"_id": word,
                        "counter": 1,
                        "docPos": [{
                            "nameDoc":title,
                            "tf":1,
                            "positions":[pos]
                        }]})
                # print(post.get("counter"))
                # if word in list_of_terms:
                #     flag = True
                #     if word in found:
                #        flag = False
                #     else:
                #        found.append(word)
                #     a = "docPos." + str(i) + ".positions"
                #     b = "docPos." + str(i) + ".tf"
                #     if flag:
                #         self.collection.update_one({"_id": word ,"docPos.nameDoc" : title},{"$inc" : {b : 1}})   #ftiakse ta insert na doulevoun sosta
                #         self.collection.update_one({"_id" : word,"docPos.nameDoc" : title},{"$push" : {a : pos}})
                #     else:
                #         self.collection.aggregate([
                #             {
                #                 "$addFields":{
                #                     "nameDoc": title,
                #                     "tf": 1,
                #                     "positions": [pos]
                #                 }
                #             }
                #         ])
                #         self.collection.update_one({"_id": word, "docPos.nameDoc": title}, {"$inc": {b : 1}})
                #         self.collection.update_one({"_id": word },{"$inc" : {"counter" : 1}})
                #         self.collection.update_one({"_id" : word,"docPos.nameDoc" : title},{"$push" : {a : pos }})
                # else:
                #     self.collection.insert_one(post)
                #     list_of_terms.append(word)

    #successfully creates inverted index in ram
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

        # print(self.di)
        # post = ({"_id": word,
        #          "counter": 1,
        #          "docPos": [{
        #              "nameDoc": title,
        #              "tf": 1,
        #              "positions": [pos]
        #          }]})
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
            # for word in (self.di.get(title)):
            #     print(d.get(word))
            # embeddedDict = {
            #                  "nameDoc": title,
            #                  "tf": len(d.get(word)),
            #                  "positions": d.get(word)
            #              }
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
            #     print(x.get("docPos",{})[0].get("positions"))

        for x in posts:
            print(x)

        # for item in alist:
        #     print(item.items())

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

'''

cluster = MongoClient("mongodb+srv://chris:12340987@cluster0-i10z0.azure.mongodb.net/test?retryWrites=true&w=majority")
db = cluster["InformationRetrieval"]
collection = db["Indexer"]
posts = {"_id":"hi" , "counter":1, "docsPos":[("what",0)]}
print(posts)


collection.insert_one(posts)
#collection.update_one({"_id":"hello"}, {"$inc":{"counter":1}})
#collection.update_one({"_id":"hello"}, {"$addToSet":{"docsPos":(my_document, 1)}})

'''