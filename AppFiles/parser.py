from urllib.parse import urlparse, urljoin
from multiprocessing import Queue
from threading import Thread, current_thread
from bs4 import BeautifulSoup
import logging
from logging.handlers import QueueHandler


class URLparser:
    def __init__(self, prs_queue, set_queue, dom_name, logfor_queue):
        self.parse_switch = True

        self.domain_name = dom_name

        self.BS_workers_queue = Queue()
        self.topars_queue = prs_queue
        self.toset_queue = set_queue

        self.BS_workers_id_list = list(range(10))
        self.BS_workers_pool = {w_id: BeautifulSoup for w_id in self.BS_workers_id_list}

        for BS_worker_id in self.BS_workers_id_list:
            self.BS_workers_queue.put(BS_worker_id)

        self.loggingfor_queue = logfor_queue
        self.pars_logger = logging.getLogger('Pars_Logger')
        self.pars_logger.addHandler(QueueHandler(self.loggingfor_queue))
        self.pars_logger.setLevel(logging.DEBUG)

    def parse_task(self, BS_worker, parse_data):
        thhr_name = current_thread().name
        temp_url_set = set()
        resp_status_code = parse_data.status_code
        response_url = parse_data.url
        resp_text = parse_data.text

        if resp_status_code != 200:
            self.pars_logger.warning('URL(%s) responsed %d STATUS CODE' % (response_url, resp_status_code))
            return
        try:
            html_soup = BS_worker(resp_text, 'html.parser')
        except Exception as exc:
            self.pars_logger.critical(exc)
            return

        tag_list = html_soup.findAll('a')
        for a_tag in tag_list:
            href_text = a_tag.attrs.get('href')

            if (href_text == "") or None:
                continue

            href_text = urljoin(response_url, href_text)
            parsed_href_text = urlparse(href_text)

            if self.domain_name != parsed_href_text.netloc.lstrip('w.'):
                continue

            href_text = parsed_href_text.scheme + "://" + parsed_href_text.netloc + parsed_href_text.path

            temp_url_set.add(href_text)

        while len(temp_url_set) != 0:
            self.toset_queue.put(temp_url_set.pop())

    def toparse_queue_cheking(self):
        thr_name = current_thread().name

        while self.parse_switch:

            current_parse_data = self.topars_queue.get()

            current_BS_worker_id = self.BS_workers_queue.get()
            current_BS_worker = self.BS_workers_pool[current_BS_worker_id]

            self.parse_task(current_BS_worker, current_parse_data)

            self.BS_workers_queue.put(current_BS_worker_id)

    def parsing(self):
        parser_threads = [Thread(target=self.toparse_queue_cheking, daemon=True) for _ in self.BS_workers_id_list]

        for prs_thread in parser_threads:
            prs_thread.start()

        for prs_thread in parser_threads:
            prs_thread.join()


def make_parser(prs_queue, set_queue, dom_name, logg_queue):
    url_parser = URLparser(prs_queue, set_queue, dom_name, logg_queue)
    url_parser.parsing()
