"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import unittest
import gevent

from application import KonsensusApp
from api import KonsensusAPI
from manager import KonsensusManager


class UseCaseOneTests(unittest.TestCase):
    """
    For use case #1
    """
    def setUp(self):
        pass

    def test_api(self):
        # import zmq
        # ctx = zmq.Context()
        # socket = zmq.socket(zmq.SUB)
        api = KonsensusAPI(KonsensusManager())
        assert api.echo('Mehdi') == 'Mehdi'

    def tearDown(self):
        gevent.killall()


if __name__ == '__main__':
    unittest.main()