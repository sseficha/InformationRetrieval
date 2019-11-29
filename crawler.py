from extractor import html_extractor
import threading
import time


class Crawler(threading.Thread):
    nof_pages = 0

    # def __init__(self, page, nof_pages, reset, nof_threads):

    def __init__(self, word_queue, link_queue):
        threading.Thread.__init__(self)
        self.word_queue_lock = threading.Lock()
        self.link_queue_lock = threading.Lock()

        #    self.nof_threads=nof_threads
        #    if reset:
        #       new index
        # else:
        #     previous index

        self.word_queue = word_queue
        self.link_queue = link_queue

    @staticmethod
    def set_page_number(number):
        Crawler.nof_pages = number

    @staticmethod
    def decrement_page_number():
        Crawler.nof_pages -= 1

    def addLink(self, url):
        self.link_queue.append(url)

    def run(self):
        while Crawler.nof_pages >= 0:

            if len(self.link_queue) == 0:
                time.sleep(0.5)
            else:
                print(self.link_queue)

                self.link_queue_lock.acquire()
                link = self.link_queue.pop(0)
                self.link_queue_lock.release()
                extracted_words, extracted_links = html_extractor(link)  # crawl that link
                self.word_queue_lock.acquire()
                self.word_queue.append(extracted_words)
                self.word_queue_lock.release()
                self.link_queue_lock.acquire()
                [self.link_queue.append(link) for link in extracted_links]
                self.link_queue_lock.release()

            print(self.word_queue)


# for indexer to fetch words

            # if len(self.word_queue) == 0:
            #     time.sleep(0.5)
            # else:
            #     self.word_queue_lock.acquire()
            #     words = self.word_queue.pop(0)
            #     self.word_queue_lock.release()

            # send them for processing at index
            Crawler.decrement_page_number()
            time.sleep(5)
