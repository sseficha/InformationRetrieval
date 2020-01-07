import threading
from Index import Index
from crawler import Crawler
from miniIndexPuller import miniIndexPuller
from miniIndexPusher import miniIndexPusher
import sys


link = sys.argv[1]
nof_pages = int(sys.argv[2])
reset = int(sys.argv[3])
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

for i in range(0, 6):
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


if reset:
    Index.clear()

pusher1.start()


print('Exiting main thread')


