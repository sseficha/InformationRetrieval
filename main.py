import threading
import time
from Index import Index
from crawler import Crawler
from miniIndexPuller import miniIndexPuller
from miniIndexPusher import miniIndexPusher
import sys

# python main.py sample1.html 4 1 2 trexto etsi






link = sys.argv[1]
nof_pages = int(sys.argv[2])
reset = bool(sys.argv[3])
nof_threads = int(sys.argv[4])

word_queue = []
link_queue = []
word_queue_lock = threading.Lock()
link_queue_lock = threading.Lock()
mini_index_queue_lock = threading.Lock()

#Crawler.set_page_number(4)


crawler_list = []
for i in range(0, nof_threads):
    crawler_list.append(Crawler())

Crawler.set_page_number(nof_pages)
#PULLER set documents max gia na stamataei tha paei panw
Crawler.addQueues(word_queue, link_queue)
Crawler.addLocks(word_queue_lock, link_queue_lock)
Crawler.addLink(link)

#crawler1 = Crawler(word_queue, link_queue, word_queue_lock)
#crawler1.addLink('https://www.geeksforgeeks.org/inverted-index/')
#crawler1.addLink('sample1.html')

#crawler1.addLink(link)

# crawler2 = Crawler(word_queue, link_queue, word_queue_lock)




print('Starting threads')

for crawler in crawler_list:
    crawler.start()




#crawler1.start()
# crawler2.start()

#if reset:
#    empty Index

miniIndex = []
pusher1 = miniIndexPusher()
puller1 = miniIndexPuller()

Index.set_document_number(nof_pages)
Index.set_word_queue(word_queue)
Index.set_locks(word_queue_lock,mini_index_queue_lock)
Index.set_mini_index(miniIndex)
Index.set_page_number(nof_pages)
#Index.set_page_number(4)
Index.set_mini_size(2)
if reset:
    Index.clear()
pusher1.start()
puller1.start()





#for crawler in crawler_list:
#    crawler.join()

#index1.join()

#while time.time() < End:
#    index.updateMiniIndex()

# index.updateWeights()
# index.printPosts()
#index.updateIndex()


#crawler1.join()
# crawler2.join()



print('Exiting main thread')

# temp = Index(word_queue, word_queue_lock)
# temp.topKDocuments(['cat','allies'])
