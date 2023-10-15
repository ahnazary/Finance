""" This module contains utility functions for the finance package. """

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


def custom_logger(logger_name: str, log_level: int = logging.warning):
    """Creates a custom logger.

    Args:
        logger_name (str): Name of the logger.
        log_level (int): Log level.

    Returns:
        logging.Logger: A custom logger.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")

    file_handler = logging.FileHandler(f"{logger_name}.log")
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger
