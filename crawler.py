from extractor import html_extractor
import threading
import time


class Crawler(threading.Thread):
    nof_pages = 0       #number of pages to be processed/saved
    word_queue = []
    link_queue = []
    link_queue_lock = None
    word_queue_lock = None


    def __init__(self):
        threading.Thread.__init__(self)


    @staticmethod
    def set_page_number(number):
        Crawler.nof_pages = number

    @staticmethod
    def decrement_page_number():
        Crawler.nof_pages -= 1

    @staticmethod
    def addLink(url):
        Crawler.link_queue.append(url)

    @staticmethod
    def addQueues(w_queue, l_queue):
        Crawler.word_queue = w_queue
        Crawler.link_queue = l_queue

    @staticmethod
    def addLocks(w_lock, l_lock):
        Crawler.word_queue_lock = w_lock
        Crawler.link_queue_lock = l_lock

    def run(self):
        while Crawler.nof_pages > 0:


            if len(Crawler.link_queue) == 0:
                time.sleep(0.5)
            else:


                Crawler.link_queue_lock.acquire()
                link = Crawler.link_queue.pop(0)
                # print(link)
                Crawler.link_queue_lock.release()
                extracted_words, extracted_links = html_extractor(link)  # crawl that link and get then links and the words of the document
                Crawler.word_queue_lock.acquire()
                Crawler.word_queue.append({'link':link,'words': extracted_words})   #save the words in word_queue
                Crawler.word_queue_lock.release()
                Crawler.link_queue_lock.acquire()
                [Crawler.link_queue.append(link) for link in extracted_links]       #save the links to be processed in link_queue
                Crawler.link_queue_lock.release()
                Crawler.decrement_page_number()






