"""
    consensus
    ~~~~~~~~~

    This file is part of kansensus project.
"""

import logging

__author__ = 'Mehdi Sadeghi'


def log(func, *args, **kwargs):
    """
    Log function call
    :param func:
    :param args:
    :param kwargs:
    :return:
    """
    def new_func(*args, **kwargs):
        logging.debug("Function name: %s" % func.__name__)
        logging.debug("Arguments: %s" % str(args))
        logging.debug("Keyword arguements: %s" % kwargs)
        res = func(*args, **kwargs)
        logging.debug("Fuction result: %s" % res)
        return res
    return new_func
