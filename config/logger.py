import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Logger

MAX_LOG_SIZE = 1 * 10 ** 6  # 1MB
LOG_FILE_NAME = Path('logs', 'data_node.log')
LOG_FILE_NAME.parent.mkdir(parents=True, exist_ok=True)


class BaseLogger:
    def __init__(self, log_file_name):
        self.log_file_name = log_file_name

    @staticmethod
    def get_console_handler(formatter):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.DEBUG)
        return console_handler

    def get_file_handler(self, formatter):
        file_handler = RotatingFileHandler(self.log_file_name, maxBytes=MAX_LOG_SIZE, backupCount=5)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        return file_handler

    def get_logger(self, logger_name):
        formatter = logging.Formatter(fmt='[%(levelname)s] %(asctime)s %(name)s.%(funcName)s: %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')

        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(self.get_console_handler(formatter))
        logger.addHandler(self.get_file_handler(formatter))
        return logger


data_node_logger = BaseLogger(LOG_FILE_NAME)
