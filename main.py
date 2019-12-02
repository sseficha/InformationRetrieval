import threading
import Ind

from crawler import Crawler


Crawler.set_page_number(200)
word_queue = []
link_queue = []
word_queue_lock = threading.Lock()

crawler1 = Crawler(word_queue, link_queue, word_queue_lock)
# crawler1.addLink('https://www.geeksforgeeks.org/inverted-index/')
crawler1.addLink('sample1.html')
crawler2 = Crawler(word_queue, link_queue)

print('Starting threads')


crawler1.start()
crawler2.start()

index = Ind(word_queue,word_queue_lock)

crawler1.join()
crawler2.join()


print('Exiting main thread')