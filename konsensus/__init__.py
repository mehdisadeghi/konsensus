"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import logging
logger = logging.getLogger(__name__)
logger.propagate = False
ch = logging.StreamHandler()

formatter = logging.Formatter('%(levelname)s:%(name)s:\tPid:%(process)d:\t %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


from application import KonsensusApp