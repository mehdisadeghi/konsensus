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
from defaults import DefaultConfig

pid_p1 = 0
pid_p2 = 0

class UseCaseOneTests(unittest.TestCase):
    """
    For use case #1
    """
    def setUp(self):

        self.config_p1 = DefaultConfig()
        self.config_p1.update({
            'API_PORT': 9100,  # API port. Application will listen for incoming requests
            'PUB_PORT': 9101,  # Will publish all the news on this port.
            'PEERS': [('127.0.0.1', 9201)],  # Name of peers to subscribe to their publisher port.
            'HDF5_REPO': '/W5/sade/workspace/hdf5_samples/usecase1.h5'
        })
        self.config_p2 = DefaultConfig()
        self.config_p2.update({
            'API_PORT': 9200,  # API port. Application will listen for incoming requests
            'PUB_PORT': 9201,  # Will publish all the news on this port.
            'PEERS': [('127.0.0.1', 9101)],  # Name of peers to subscribe to.
            'HDF5_REPO': '/W5/sade/workspace/hdf5_samples/uc1pc560.h5'
        })

        global pid_p1, pid_p2

        #
        self.pid_p1 = pid_p1
        self.pid_p2 = pid_p2

        # import os
        # self.pid_p1 = os.fork()
        # if self.pid_p1 == 0:
        #     app1 = KonsensusApp('peer1', config=self.config_p1)
        #     app1.run()
        #     return
        #
        # self.pid_p2 = os.fork()
        # if self.pid_p2 == 0:
        #     app2 = KonsensusApp('peer2', config=self.config_p2)
        #     app2.run()
        #     return

    def test_api_p1(self):
        if self.pid_p1 == 0:
            return
        import zerorpc
        api = zerorpc.Client()
        api.connect('tcp://127.0.0.1:9100')
        assert api.echo('Mehdi') == 'Mehdi'

    def test_api_p2(self):
        if self.pid_p2 == 0:
            return
        import zerorpc
        api = zerorpc.Client()
        api.connect('tcp://127.0.0.1:9200')
        assert api.echo('Mehdi') == 'Mehdi'


    def tearDown(self):
        import os
        import signal
        #os.kill(self.pid_p1, signal.SIGKILL)
        #os.kill(self.pid_p2, signal.SIGKILL)

def run_servers():
    config_p1 = DefaultConfig()
    config_p1.update({
        'API_PORT': 9100,  # API port. Application will listen for incoming requests
        'PUB_PORT': 9101,  # Will publish all the news on this port.
        'PEERS': [('127.0.0.1', 9201)],  # Name of peers to subscribe to their publisher port.
        'HDF5_REPO': '/W5/sade/workspace/hdf5_samples/usecase1.h5'
    })
    config_p2 = DefaultConfig()
    config_p2.update({
        'API_PORT': 9200,  # API port. Application will listen for incoming requests
        'PUB_PORT': 9201,  # Will publish all the news on this port.
        'PEERS': [('127.0.0.1', 9101)],  # Name of peers to subscribe to.
        'HDF5_REPO': '/W5/sade/workspace/hdf5_samples/uc1pc560.h5'
    })

    global pid_p1, pid_p2
    pid_p1 = 0
    pid_p2 = 0

    import os
    pid_p1 = os.fork()
    if pid_p1 == 0:
        app1 = KonsensusApp('peer1', config=config_p1)
        app1.run()
        return

    pid_p2 = os.fork()
    if pid_p2 == 0:
        app2 = KonsensusApp('peer2', config=config_p2)
        app2.run()
        return

if __name__ == '__main__':
    run_servers()
    unittest.main()
