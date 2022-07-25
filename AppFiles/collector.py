import xml.etree.ElementTree as ET
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
        self.result_file_name = urlparse(self.user_url).netloc + '-sitemap.xml'

        self.all_url_set = set()
        self.processed_url_set = set()
        self.unprocessed_url_set = set()

        self.queue_to_request = Queue(maxsize=2000)
        self.queue_to_parse = Queue(maxsize=2000)
        self.queue_to_set = Queue(maxsize=10000)
        self.file_queue = Queue()


        self.logging_queue = Queue()
        self.coll_loger = logging.getLogger('Main_Logger')
        self.coll_loger.addHandler(QueueHandler(self.logging_queue))
        self.coll_loger.setLevel(logging.DEBUG)

    def init_result_file(self):  # Result file initialising
        et_root = ET.Element('urlset')
        et_root.set('xmlns', 'https://www.sitemaps.org/schemas/sitemap/0.9')

        init_url = ET.SubElement(et_root, 'url')
        init_url_loc = ET.SubElement(init_url, 'loc')
        init_url_loc.text = self.user_url

        ET.ElementTree(et_root).write(self.result_file_name, encoding='UTF-8', xml_declaration=True)

    def result_xml_writer(self):  # Reading URLs from parser out queue and write it to result file
        tmp_url_list = list()
        while True:
            while len(tmp_url_list) != 500:
                tmp_url = self.queue_to_set.get()
                if tmp_url not in self.all_url_set:
                    tmp_url_list.append(tmp_url)
                    self.all_url_set.add(tmp_url)
            position_tuple = self.file_queue.get()


    def result_xml_reader(self):

    def collecting(self):  # Main loop method

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

        que_log_prcs = Process(target=self.loggerr, daemon=True)
        que_log_prcs.start()

        while self.loop_switch:
            self.coll_loger.debug('Vizited URLs: %d' % len(self.processed_url_set))  # Logging
            if (self.skip_counter > 100) and \
               (self.queue_to_parse.qsize() == 0) and \
               (self.queue_to_request.qsize() == 0):
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
        queues_loger = logging.getLogger('Ques_Logger')
        queues_loger.addHandler(QueueHandler(self.logging_queue))
        queues_loger.setLevel(logging.DEBUG)
        while True:
            queues_loger.debug('Collector skip counter value: %d' % self.skip_counter)
            queues_loger.debug('Tasks in torequest Queue: %s' % self.queue_to_request.qsize())
            queues_loger.debug('Tasks in topars Queue: %s' % self.queue_to_parse.qsize())
            queues_loger.debug('Tasks in toset Queue: %s' % self.queue_to_set.qsize())
            sleep(1)
