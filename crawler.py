from extractor import html_extractor
import threading
import time


class Crawler(threading.Thread):
    nof_pages = 0

    # def __init__(self, page, nof_pages, reset, nof_threads):

    def __init__(self, word_queue, link_queue, threadLock):
        threading.Thread.__init__(self)
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
            link = self.link_queue.pop(0)
            extracted_words, extracted_links = html_extractor(link)  # crawl that link
            self.word_queue.append(extracted_words)
            [self.link_queue.append(link) for link in extracted_links]
            print(self.word_queue)
            print(self.link_queue)
            words = self.word_queue.pop(0)
            # send them for processing at index
            Crawler.decrement_page_number()
            time.sleep(5)
