"""
    consensus
    ~~~~~~~~~

    This file is part of consensus project.
"""
import logging

import zerorpc

from api import ConsensusAPI

__author__ = 'Mehdi Sadeghi'


logging.basicConfig(level=logging.DEBUG)
logging.info("Starting server")
s = zerorpc.Server(ConsensusAPI())
s.bind("tcp://0.0.0.0:4242")
logging.info("Listening on tcp://0.0.0.0:4242")
s.run()