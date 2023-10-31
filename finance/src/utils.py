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


def custom_logger(logger_name: str, log_level: int = logging.WARNING):
    """Creates a custom logger.

    Args:
        logger_name (str): Name of the logger.
        log_level (int): Log level.

    Returns:
        logging.Logger: A custom logger.
    """
    logger = logging.getLogger(logger_name)

    return logger
