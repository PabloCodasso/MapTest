from threading import Thread, current_thread
from multiprocessing import Queue
from requests_html import HTMLSession
import logging
from time import sleep
from logging.handlers import QueueHandler


class URLrequestor:
    def __init__(self, to_reqt_queue, to_prse_queue, for_log_queue):
        self.reqt_switch = True

        self.workers_queue = Queue()
        self.to_reqt_queue = to_reqt_queue
        self.to_prse_queue = to_prse_queue

        self.workers_id_list = list(range(3))
        self.workers_pool = {w_id: HTMLSession() for w_id in self.workers_id_list}

        for wrk_id in self.workers_id_list:
            self.workers_queue.put(wrk_id)

        self.for_logging_queue = for_log_queue
        self.requ_logger = logging.getLogger('Requ_Logger')
        self.requ_logger.addHandler(QueueHandler(self.for_logging_queue))
        self.requ_logger.setLevel(logging.DEBUG)

    def session_task(self, worker, url_data):
        sleep(3)
        try:
            response = worker.get(url_data)
        except Exception as exc:
            self.requ_logger.critical(exc)
            return
        self.requ_logger.info('Response from URL(%s): getted with %s' % (url_data, current_thread().name))
        self.to_prse_queue.put(response)
        self.requ_logger.info('Response from URL(%s): putted in pars queue with %s' % (url_data, current_thread().name))
        return

    def torequest_queue_cheking(self):
        self.requ_logger.debug('Start checking requst queue with %s' % current_thread().name)
        while self.reqt_switch:
            self.requ_logger.info('Try get URL from request url with %s' % current_thread().name)
            current_url_data = self.to_reqt_queue.get()
            self.requ_logger.info('Got URL(%s) from request queue with %s' % (current_url_data, current_thread().name))
            current_worker_id = self.workers_queue.get()

            if current_url_data == "stop":
                self.reqt_switch = False
                self.to_reqt_queue.put(current_url_data)
                continue

            current_worker = self.workers_pool[current_worker_id]

            self.session_task(current_worker, current_url_data)

            self.workers_queue.put(current_worker_id)

        # self.to_prse_queue.put('stop')

    def requesting(self):
        self.requ_logger.debug('Function requesting of URLrequestor started')
        requestor_threads = [Thread(target=self.torequest_queue_cheking, daemon=True) for _ in self.workers_id_list]

        for req_thread in requestor_threads:
            req_thread.start()

        for req_thread in requestor_threads:
            req_thread.join()
        self.requ_logger.debug('Function requesting of URLrequestor completed')


def make_requestor(req_queue, pars_queue, log_queue):
    my_requestor = URLrequestor(req_queue, pars_queue, log_queue)
    my_requestor.requesting()
