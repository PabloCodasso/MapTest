from threading import Thread
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

        self.workers_id_list = list(range(10))
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
            self.requ_logger.warning(exc)
            return
        self.to_prse_queue.put(response)
        return

    def torequest_queue_cheking(self):
        while self.reqt_switch:
            current_url_data = self.to_reqt_queue.get()
            current_worker_id = self.workers_queue.get()

            current_worker = self.workers_pool[current_worker_id]

            self.session_task(current_worker, current_url_data)

            self.workers_queue.put(current_worker_id)

    def requesting(self):
        requestor_threads = [Thread(target=self.torequest_queue_cheking, daemon=True) for _ in self.workers_id_list]

        for req_thread in requestor_threads:
            req_thread.start()

        for req_thread in requestor_threads:
            req_thread.join()


def make_requestor(req_queue, pars_queue, log_queue):
    my_requestor = URLrequestor(req_queue, pars_queue, log_queue)
    my_requestor.requesting()
