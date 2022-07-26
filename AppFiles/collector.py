import xml.etree.ElementTree as ET
from multiprocessing import Queue, Pipe, Process
from urllib.parse import urlparse
from time import sleep
from datetime import datetime, timedelta

import logging
from logging.handlers import QueueHandler

from AppFiles.requestor import make_requestor
from AppFiles.parser import make_parser
from AppFiles.logger import logger_prcs


class Collector:  # Application core main class. Response for general logic and global data.
    def __init__(self, url):
        self.timeout = 0

        self.url_counter = 0
        self.user_url = url
        self.result_file_name = r'Results\%s' % urlparse(self.user_url).netloc + '-sitemap.xml'

        self.all_url_set = set()

        self.queue_to_request = Queue(maxsize=2000)
        self.queue_to_parse = Queue(maxsize=2000)
        self.queue_to_set = Queue(maxsize=10000)

        self.load_value = 15
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

    def result_xml_writer(self, stop_recv, stop_transm):
        switch = True
        tmp_url_list = list()
        while switch:
            if stop_recv.poll():
                switch = False
                continue
            sleep(0.5)
            while self.queue_to_set.qsize() != 0 and switch and len(tmp_url_list) < 1000:
                if len(self.all_url_set) == 50000:
                    stop_transm.send(True)
                    switch = False
                    continue
                tmp_url = self.queue_to_set.get()

                if not (tmp_url in self.all_url_set):
                    tmp_url_list.append(tmp_url)
                    self.all_url_set.add(tmp_url)

            posit = self.file_queue.get()

            xml_tree = ET.parse(self.result_file_name)
            xml_root = xml_tree.getroot()
            for url_to_write in tmp_url_list:
                self.url_counter += 1
                init_url = ET.SubElement(xml_root, 'url')
                init_url_loc = ET.SubElement(init_url, 'loc')
                init_url_loc.text = url_to_write
            ET.register_namespace("", "https://www.sitemaps.org/schemas/sitemap/0.9")
            xml_tree.write(self.result_file_name, encoding='UTF-8', xml_declaration=True)

            tmp_url_list.clear()
            self.file_queue.put(posit)
        print('Sitemap completed with %d URLs' % len(self.all_url_set))

    def result_xml_reader(self):  # Reading urls from rezult file and putting them in requestor queue
        while True:               # Unvizited URLs marked with position int var in file_queue
            sleep(1)
            start_pos = self.file_queue.get()
            read_xml_tree = ET.parse(self.result_file_name)
            read_xml_root = read_xml_tree.getroot()
            root_len = len(read_xml_root)

            if root_len == start_pos:
                self.file_queue.put(start_pos)
                continue

            if (root_len - start_pos) < self.load_value:
                finish_pos = root_len
            else:
                finish_pos = start_pos + self.load_value

            for read_pos in range(start_pos, finish_pos):
                self.queue_to_request.put(read_xml_root[read_pos][0].text)

            self.file_queue.put(finish_pos)

    def collecting(self):  # Main executing method

        self.init_result_file()

        recv, transm = Pipe()

        domain = urlparse(self.user_url).netloc

        req_prcs = Process(target=make_requestor, args=(self.queue_to_request,
                                                        self.queue_to_parse, self.logging_queue), daemon=True)
        req_prcs.start()

        pars_prcs = Process(target=make_parser, args=(self.queue_to_parse,
                                                      self.queue_to_set, domain, self.logging_queue), daemon=True)
        pars_prcs.start()

        log_prcs = Process(target=logger_prcs, args=(self.logging_queue,), daemon=True)
        log_prcs.start()

        reader_prcs = Process(target=self.result_xml_reader, daemon=True)
        reader_prcs.start()

        writer_prcs = Process(target=self.result_xml_writer, args=(recv, transm), daemon=True)
        writer_prcs.start()

        self.file_queue.put(0)

        crit_time_start = datetime.now()
        crit_time_finish = crit_time_start + timedelta(minutes=20)

        while self.timeout < 30:
            sleep(1)
            if recv.poll():
                break
            if (self.queue_to_set.qsize() == 0 and
               self.queue_to_parse.qsize() == 0 and
               self.queue_to_request.qsize() == 0) or (not datetime.now() > crit_time_finish):
                self.timeout += 1
            else:
                self.timeout = 0

        transm.send(True)
        writer_prcs.join()
