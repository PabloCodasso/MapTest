from urllib.parse import urlparse, urljoin
from multiprocessing import Queue, Process, parent_process
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

        self.BS_workers_id_list = list(range(5))
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
            self.pars_logger.warning('URL(%s) responsed %d STATUS CODE with %s' %
                                     (response_url, resp_status_code, thhr_name))
            return

        html_soup = BS_worker(resp_text, 'html.parser')
        self.pars_logger.info('Souping URL(%s) with %s' % (response_url, thhr_name))

        tag_list = html_soup.findAll('a')
        for a_tag in tag_list:
            href_text = a_tag.attrs.get('href')

            if (href_text == "") or None:
                continue

            href_text = urljoin(response_url, href_text)
            parsed_href_text = urlparse(href_text)

            if self.domain_name != parsed_href_text.netloc:
                continue

            href_text = parsed_href_text.scheme + "://" + parsed_href_text.netloc + parsed_href_text.path

            temp_url_set.add(href_text)

        self.pars_logger.info('%d unic URLS was found in URL(%s) response with %s' % (len(temp_url_set), response_url,
                                                                                      thhr_name))
        while len(temp_url_set) != 0:
            if not self.toset_queue.qsize() > 9800:
                self.toset_queue.put(temp_url_set.pop())
            else:
                continue
        self.pars_logger.info('%s finished to put URLs from set to set queue' % thhr_name)

    def toparse_queue_cheking(self):
        thr_name = current_thread().name
        self.pars_logger.debug('Start checking parsing queue with %s' % thr_name)

        while self.parse_switch:
            # if not parent_process().is_alive():
            #     self.parse_switch = False
            #     continue
            self.pars_logger.info('Trying to get URL parsing queue with %s' % thr_name)
            current_parse_data = self.topars_queue.get()
            self.pars_logger.info('Got URL(%s) from parsing queue with %s' % (current_parse_data.url, thr_name))

            current_BS_worker_id = self.BS_workers_queue.get()
            current_BS_worker = self.BS_workers_pool[current_BS_worker_id]

            self.parse_task(current_BS_worker, current_parse_data)

            self.BS_workers_queue.put(current_BS_worker_id)

    def parsing(self):
        self.pars_logger.debug('Started parsing function of URLparser class')
        parser_prcses = [Thread(target=self.toparse_queue_cheking, daemon=True) for _ in self.BS_workers_id_list]

        for prs_prcs in parser_prcses:
            prs_prcs.start()

        for prs_prcs in parser_prcses:
            prs_prcs.join()

        self.pars_logger.debug('Finished parsing function of URLparser class')


def make_parser(prs_queue, set_queue, dom_name, logg_queue):
    url_parser = URLparser(prs_queue, set_queue, dom_name, logg_queue)
    url_parser.parsing()
