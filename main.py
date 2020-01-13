import threading
from Index import Index
from crawler import Crawler
from miniIndexPuller import miniIndexPuller
from miniIndexPusher import miniIndexPusher
import sys


link = sys.argv[1]
nof_pages = int(sys.argv[2])    #number of pages to crawl
reset = int(sys.argv[3])
nof_threads = int(sys.argv[4])      #number of crawler threads

word_queue = []
link_queue = []

#all the lists below are accessed by different threads/classes
#the threads share a common lock so that there are no issues when adding/fetching elements from them


word_queue_lock = threading.Lock()
link_queue_lock = threading.Lock()
mini_index_queue_lock = threading.Lock()

print("\n")
print("""██▓ ███▄    █   █████▒▒█████   ██▀███   ███▄ ▄███▓ ▄▄▄     ▄▄▄█████▓ ██▓ ▒█████   ███▄    █     ██▀███  ▓█████▄▄▄█████▓ ██▀███   ██▓▓█████ ██▒   █▓ ▄▄▄       ██▓    
▓██▒ ██ ▀█   █ ▓██   ▒▒██▒  ██▒▓██ ▒ ██▒▓██▒▀█▀ ██▒▒████▄   ▓  ██▒ ▓▒▓██▒▒██▒  ██▒ ██ ▀█   █    ▓██ ▒ ██▒▓█   ▀▓  ██▒ ▓▒▓██ ▒ ██▒▓██▒▓█   ▀▓██░   █▒▒████▄    ▓██▒    
▒██▒▓██  ▀█ ██▒▒████ ░▒██░  ██▒▓██ ░▄█ ▒▓██    ▓██░▒██  ▀█▄ ▒ ▓██░ ▒░▒██▒▒██░  ██▒▓██  ▀█ ██▒   ▓██ ░▄█ ▒▒███  ▒ ▓██░ ▒░▓██ ░▄█ ▒▒██▒▒███   ▓██  █▒░▒██  ▀█▄  ▒██░    
░██░▓██▒  ▐▌██▒░▓█▒  ░▒██   ██░▒██▀▀█▄  ▒██    ▒██ ░██▄▄▄▄██░ ▓██▓ ░ ░██░▒██   ██░▓██▒  ▐▌██▒   ▒██▀▀█▄  ▒▓█  ▄░ ▓██▓ ░ ▒██▀▀█▄  ░██░▒▓█  ▄  ▒██ █░░░██▄▄▄▄██ ▒██░    
░██░▒██░   ▓██░░▒█░   ░ ████▓▒░░██▓ ▒██▒▒██▒   ░██▒ ▓█   ▓██▒ ▒██▒ ░ ░██░░ ████▓▒░▒██░   ▓██░   ░██▓ ▒██▒░▒████▒ ▒██▒ ░ ░██▓ ▒██▒░██░░▒████▒  ▒▀█░   ▓█   ▓██▒░██████▒
░▓  ░ ▒░   ▒ ▒  ▒ ░   ░ ▒░▒░▒░ ░ ▒▓ ░▒▓░░ ▒░   ░  ░ ▒▒   ▓▒█░ ▒ ░░   ░▓  ░ ▒░▒░▒░ ░ ▒░   ▒ ▒    ░ ▒▓ ░▒▓░░░ ▒░ ░ ▒ ░░   ░ ▒▓ ░▒▓░░▓  ░░ ▒░ ░  ░ ▐░   ▒▒   ▓▒█░░ ▒░▓  ░
 ▒ ░░ ░░   ░ ▒░ ░       ░ ▒ ▒░   ░▒ ░ ▒░░  ░      ░  ▒   ▒▒ ░   ░     ▒ ░  ░ ▒ ▒░ ░ ░░   ░ ▒░     ░▒ ░ ▒░ ░ ░  ░   ░      ░▒ ░ ▒░ ▒ ░ ░ ░  ░  ░ ░░    ▒   ▒▒ ░░ ░ ▒  ░
 ▒ ░   ░   ░ ░  ░ ░   ░ ░ ░ ▒    ░░   ░ ░      ░     ░   ▒    ░       ▒ ░░ ░ ░ ▒     ░   ░ ░      ░░   ░    ░    ░        ░░   ░  ▒ ░   ░       ░░    ░   ▒     ░ ░   
 ░           ░            ░ ░     ░            ░         ░  ░         ░      ░ ░           ░       ░        ░  ░           ░      ░     ░  ░     ░        ░  ░    ░  ░
                                                                                                                                                ░                     """)



crawler_list = []       #list with instances of Crawler
miniIndexPuller_list = []  #list with instances of miniIndexPuller

for i in range(0, nof_threads):
    crawler_list.append(Crawler())

for i in range(0, 6):
    miniIndexPuller_list.append(miniIndexPuller())    # 6 instances of miniIndexPuller cause searching in DB is a hefty task


Crawler.set_page_number(nof_pages)
Crawler.addQueues(word_queue, link_queue)
Crawler.addLocks(word_queue_lock, link_queue_lock)
Crawler.addLink(link)       #starter link


for crawler in crawler_list:
    crawler.start()


miniIndex = []

pusher1 = miniIndexPusher()        # 1 instance of miniIndexPusher is enough cause it doesn't do that much work

for puller in miniIndexPuller_list:
    puller.start()

Index.set_word_queue(word_queue)
Index.set_locks(word_queue_lock,mini_index_queue_lock)
Index.set_mini_index(miniIndex)
miniIndexPusher.set_page_number(nof_pages)


if reset:
    Index.clear()

pusher1.start()





