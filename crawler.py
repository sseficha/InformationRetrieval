from extractor import html_extractor
import threading
import time


class Crawler(threading.Thread):
    nof_pages = 0
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
<<<<<<< HEAD
                # print(len(Crawler.link_queue))
=======
                #print(Crawler.link_queue)
>>>>>>> a899d6710931d979ca4849a10085bcee77477c62

                Crawler.link_queue_lock.acquire()
                link = Crawler.link_queue.pop(0)
                Crawler.link_queue_lock.release()
                extracted_words, extracted_links = html_extractor(link)  # crawl that link
                Crawler.word_queue_lock.acquire()
                Crawler.word_queue.append({'link':link,'words': extracted_words})   #save the words
                Crawler.word_queue_lock.release()
                Crawler.link_queue_lock.acquire()
                [Crawler.link_queue.append(link) for link in extracted_links]       #save the links
                Crawler.link_queue_lock.release()
                Crawler.decrement_page_number()

            # print(len(Crawler.word_queue))
            # print("$$%$%$%%$")



<<<<<<< HEAD
            # if len(Crawler.word_queue) == 0:
            #     time.sleep(0.5)
            # else:
            #     Crawler.word_queue_lock.acquire()
            #     words = Crawler.word_queue.pop(0)
            #     Crawler.word_queue_lock.release()

            # send them for processing at index
            # time.sleep(2)
            #print(Crawler.nof_pages)
=======
            #time.sleep(2)
>>>>>>> a899d6710931d979ca4849a10085bcee77477c62
