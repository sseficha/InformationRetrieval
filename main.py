import threading
import time
from Ind import Index
from crawler import Crawler

End = time.time() + 30

Crawler.set_page_number(200)
word_queue = []
link_queue = []
word_queue_lock = threading.Lock()

crawler1 = Crawler(word_queue, link_queue, word_queue_lock)
# crawler1.addLink('https://www.geeksforgeeks.org/inverted-index/')
crawler1.addLink('sample1.html')
crawler2 = Crawler(word_queue, link_queue, word_queue_lock)

print('Starting threads')


crawler1.start()
# crawler2.start()

ind = Index(word_queue,word_queue_lock)
while time.time() < End:
    ind.updateMiniIndex()
    ind.print_posts()

ind.updateIndex()

crawler1.join()
# crawler2.join()


print('Exiting main thread')