import threading
import time
from Index import Index
from crawler import Crawler

End = time.time() + 30

Crawler.set_page_number(3)
word_queue = []
link_queue = []
word_queue_lock = threading.Lock()

crawler1 = Crawler(word_queue, link_queue, word_queue_lock)
crawler1.addLink('https://www.geeksforgeeks.org/inverted-index/')
# crawler1.addLink('sample1.html')
# crawler2 = Crawler(word_queue, link_queue, word_queue_lock)



print('Starting threads')


crawler1.start()
# crawler2.start()

index1 = Index(word_queue, word_queue_lock)
index1.start()


#while time.time() < End:
#    index.updateMiniIndex()

# index.updateWeights()
# index.printPosts()
#index.updateIndex()


crawler1.join()
# crawler2.join()



print('Exiting main thread')

# temp = Index(word_queue, word_queue_lock)
# temp.topKDocuments(['cat','allies'])
