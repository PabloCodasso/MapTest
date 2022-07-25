import logging
from datetime import datetime


def logger_prcs(log_queue):
    timestamp = '-'.join(str(datetime.now())[11:19].split(':'))

    my_logger = logging.getLogger('app')
    my_logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(r"AppFiles\Logs\maptest %s.log" % timestamp)

    formatter = logging.Formatter('%(asctime)s - %(name)-12s - %(levelname)-8s - %(message)s')
    file_handler.setFormatter(formatter)

    my_logger.addHandler(file_handler)

    while True:
        message = log_queue.get()
        if message is None:
            break
        my_logger.handle(message)
