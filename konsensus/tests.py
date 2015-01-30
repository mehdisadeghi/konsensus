"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import os
import unittest
import logging
#logging.basicConfig(level=logging.DEBUG)
import zerorpc

from test_helper import instance_factory


class UseCaseOneTests(unittest.TestCase):
    """
    For use case #1
    """
    def setUp(self):
        # self.pids, self.control_config, self.peer_configs = \
        #     instance_factory(2)
        #print 'got some pids! %s' % self.pids
        self.control_uri = 'tcp://127.0.0.1:9998'
        self.api = zerorpc.Client()
        self.api.connect(self.control_uri)

    # def test_ds1(self):
    #     logging.debug(self.api.use_case_1('ds1'))
    #
    # # def tearDown(self):
    # #     for pid in self.pids:
    # #         os.kill(pid, 9)
    #
    # def runTest(self):
    #     self.test_ds1()


if __name__ == '__main__':
    unittest.main()
