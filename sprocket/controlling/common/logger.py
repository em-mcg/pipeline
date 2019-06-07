import logging
import sys


def get_logger(name):
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    log_handler = logging.StreamHandler(sys.stdout)
    log_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s (%(threadName)s) %(name)s:%(lineno)d: %(message)s')
    log_handler.setFormatter(formatter)
    logger.handlers = [log_handler]
    return logger
