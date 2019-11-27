import threading

from crawler import Crawler

threadLock = threading.Lock()

Crawler.set_page_number(200)
word_queue = []
link_queue = []

crawler1 = Crawler(word_queue, link_queue, threadLock)
crawler1.addLink('https://www.geeksforgeeks.org/inverted-index/')
#crawler2 = Crawler(word_queue, link_queue, threadLock)

print('Starting threads')


crawler1.start()

crawler1.join()

print('Exiting main thread')