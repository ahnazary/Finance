""" This module contains utility functions for the finance package. """


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
