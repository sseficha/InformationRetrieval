import threading
import time
from Index import Index
from crawler import Crawler
from miniIndexPuller import miniIndexPuller
from miniIndexPusher import miniIndexPusher
import sys

# python main.py sample1.html 4 1 2 trexto etsi


#prepei na kanoume kati otan den exei alla outlinks mia selida p.x teleport. p.x an arxisei sto geeks gia 50 selides stamataei stis 17 
#petaei ena error duplicate key sto insert Td gia kapoio logo.

start = time.time()

link = sys.argv[1]
nof_pages = int(sys.argv[2])
reset = bool(sys.argv[3])
nof_threads = int(sys.argv[4])

word_queue = []
link_queue = []
word_queue_lock = threading.Lock()
link_queue_lock = threading.Lock()
mini_index_queue_lock = threading.Lock()




crawler_list = []
miniIndexPuller_list = []
for i in range(0, nof_threads):
    crawler_list.append(Crawler())

for i in range(0, 4):
    miniIndexPuller_list.append(miniIndexPuller())

Crawler.set_page_number(nof_pages)
Crawler.addQueues(word_queue, link_queue)
Crawler.addLocks(word_queue_lock, link_queue_lock)
Crawler.addLink(link)




print('Starting threads')

for crawler in crawler_list:
    crawler.start()


miniIndex = []

pusher1 = miniIndexPusher()

for puller in miniIndexPuller_list:
    puller.start()

Index.set_word_queue(word_queue)
Index.set_locks(word_queue_lock,mini_index_queue_lock)
Index.set_mini_index(miniIndex)
miniIndexPusher.set_page_number(nof_pages)
miniIndexPuller.set_page_number(nof_pages)
#Index.set_page_number(4)
# Index.set_mini_size(2)
if reset:
    Index.clear()
pusher1.start()





print('Exiting main thread')

# temp = Index(word_queue, word_queue_lock)
# temp.topKDocuments(['cat','allies'])
