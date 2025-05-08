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


logger = logging.getLogger(__name__)


def emit_log(message: str, log_level: int = logging.INFO):
    """Creates a custom logger and emits logs.

    Args:
        log_level (int): Log level.
        message (str): Message to log.

    Returns:
        None
    """
    logger.setLevel(log_level)

    ch = logging.StreamHandler()
    ch.setLevel(log_level)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.log(log_level, message)
    logger.removeHandler(ch)
