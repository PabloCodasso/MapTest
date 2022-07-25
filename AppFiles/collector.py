from multiprocessing import Queue, Process
from urllib.parse import urlparse
from time import sleep
import logging
from logging.handlers import QueueHandler
from AppFiles.requestor import make_requestor
from AppFiles.parser import make_parser
from AppFiles.logger import logger_prcs


class Collector:  # Application core main class. Response for general logic and global data.
    def __init__(self, url):
        self.skip_counter = 0
        self.loop_switch = True

        self.user_url = url

        self.all_url_set = set()
        self.processed_url_set = set()
        self.unprocessed_url_set = set()

        self.queue_to_request = Queue(maxsize=1000)
        self.queue_to_parse = Queue(maxsize=1000)
        self.queue_to_set = Queue(maxsize=10000)

        self.logging_queue = Queue()
        self.coll_loger = logging.getLogger('Main_Logger')
        self.coll_loger.addHandler(QueueHandler(self.logging_queue))
        self.coll_loger.setLevel(logging.DEBUG)

    def collecting(self):  # Main loop method
        if not self.url_try():
            return

        self.all_url_set.add(self.user_url)
        domain = urlparse(self.user_url).netloc

        req_prcs = Process(target=make_requestor, args=(self.queue_to_request,
                                                        self.queue_to_parse, self.logging_queue), daemon=True)
        req_prcs.start()

        pars_prcs = Process(target=make_parser, args=(self.queue_to_parse,
                                                      self.queue_to_set, domain, self.logging_queue), daemon=True)
        pars_prcs.start()

        log_prcs = Process(target=logger_prcs, args=(self.logging_queue,), daemon=True)
        log_prcs.start()

        while self.loop_switch:
            self.coll_loger.debug('Vizited URLs: %d' % len(self.processed_url_set))  # Logging
            if (self.skip_counter > 100) and \
               (self.queue_to_parse.qsize() == 0) and \
               (self.queue_to_request.qsize() == 0):
                # self.queue_to_request.put('stop')
                self.loop_switch = False
                continue

            while not self.queue_to_set.empty():
                url_fromque = self.queue_to_set.get()

                if url_fromque == 'stop':
                    self.loop_switch = False
                    self.coll_loger.warning('Got STOP in main process')
                    continue

                self.all_url_set.add(url_fromque)

            self.unprocessed_url_set = self.all_url_set.difference(self.processed_url_set)
            self.coll_loger.debug('Unprocessed URLs: %d' % len(self.unprocessed_url_set))
            if len(self.unprocessed_url_set) == 0:
                self.skip_counter += 1
            else:
                self.skip_counter = 0

            while len(self.unprocessed_url_set) != 0:
                temp_url = self.unprocessed_url_set.pop()
                self.processed_url_set.add(temp_url)

                if len(self.processed_url_set) == 10000:
                    self.coll_loger.warning('Processed URL reached to its limit!')  # Logging
                    self.loop_switch = False
                    break

                self.queue_to_request.put(temp_url)
                self.coll_loger.info('URL(%s) putted to request queue!' % temp_url)  # Logging
                self.all_url_set.clear()

            sleep(0.5)

        return self.processed_url_set

    def loggerr(self):
        while True:
            print('Logger start---------------------------------------------------------------')
            print('Collector skip counter value: %d' % self.skip_counter)
            print('Vizited URLs: %d' % len(self.processed_url_set))
            print('Tasks in torequest Queue: %s' % self.queue_to_request.qsize())
            print('Tasks in topars Queue: %s' % self.queue_to_parse.qsize())
            print('Tasks in toset Queue: %s' % self.queue_to_set.qsize())
            print('Logger finished------------------------------------------------------------')
            sleep(1)

    def url_try(self):
        return True
