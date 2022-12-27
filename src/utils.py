import logging


def are_incremental(input_list: list):

    flag_list = []

    for i in range(len(input_list) - 1):
        if input_list[i] < input_list[i + 1]:
            flag_list.append(True)
        else:
            flag_list.append(False)

    if flag_list.count(False) > 1:
        return False
    else:
        return True


class Logger:
    def __init__(self, level: int = logging.INFO):
        self.logger = logging.getLogger("my_logger")

    def info(self, message: str):
        self.logger.info(message)

    def warning(self, message: str):
        self.logger.warning(message)

    def error(self, message: str):
        self.logger.error(message)

    def critical(self, message: str):
        self.logger.critical(message)
