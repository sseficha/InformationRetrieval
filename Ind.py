import pymongo
from pymongo import MongoClient


class Index:

    '''Σύνδεση με MongoDB'''

    def __init__(self):
        self.cluster = MongoClient("mongodb+srv://chris:12340987@cluster0-i10z0.azure.mongodb.net/test?retryWrites=true&w=majority")
        self.db = self.cluster["InformationRetrieval"]
        self.collection = self.db["Indexer"]

    def update_indexer(self, data):
        '''

        Parameters:
        -----------------------------------------
        titles : λίστα που περιέχει τους τίτλους των εγγράφων(1η, 3η, 5η, ... γραμμή του δοκιμαστικού αρχείου document.txt)
        content : λίστα από λιστες, όπου κάθε λίστα αποτελεί το περιεχόμενο ενός εγγράφου
        words_in_list : λίστα που περιέχει τις λέξεις ενός εγγράφου
        Στην αρχή γίνεται μια απλή επεξεργασία της λίστας data για να διαμορφωθούν οι titles,content.Αποτελεί προσωρινή υλοποίηση

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
        Paramater:
        -----------------------------------------
        list_of_terms : κάθε φορα που τρέχει το πρόγραμμα τραβάει από τη βάση όλους τους όρους και τους αποθηκεύει στη list_of_terms'''

        list_of_terms = list(self.collection.find().distinct("_id"))

        '''debugging purposes'''

        #self.collection.delete_many({})

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
        for i, title in enumerate(titles):
            for pos, word in enumerate(words_in_list[i]):
                post = {"_id": word, "counter": 1, "docPos": [(title, pos)]}
                if word in list_of_terms:
                    flag = False
                    word_document_titles = list(self.collection.find({"_id":word}).distinct("docPos"))
                    for t in word_document_titles:
                        if title == t[0]:
                            flag = True
                            break
                    if flag:
                        self.collection.update_one({"_id": word},
                                               {"$addToSet": {"docPos": (title, pos)}})
                    else:
                        self.collection.update_one({"_id": word},
                                               {"$inc": {"counter": 1}, "$addToSet": {"docPos": (title, pos)}})
                else:
                    self.collection.insert_one(post)
                    list_of_terms.append(word)


with open("document.txt", "r") as doc:
    data = doc.readlines()

ind = Index()
ind.update_indexer(data)


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