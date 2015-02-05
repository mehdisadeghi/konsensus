"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import os
import unittest
import tempfile
import multiprocessing

import zerorpc

from .application import KonsensusApp
from .settings import DefaultSettings


def make_config():
    """Makes a config with various port numbers and temp folder"""
    config = DefaultSettings()
    api_port = get_open_port()
    pub_port = get_open_port()
    num, loc = tempfile.mkstemp(suffix='.hdf5', prefix='tmp_test_')
    import h5py
    f = h5py.File(loc)
    f.close()
    config.update({
        'API_PORT': api_port,  # API port. Application will listen for incoming requests
        'PUB_PORT': pub_port,  # Will publish all the news on this port.
        'PEERS': [],  # ('127.0.0.1', 9201) Name of peers to subscribe to their publisher port.
        'HDF5_REPO': loc,
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': tempfile.mktemp(suffix='.log')})
    return config


def get_open_port():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


def _cook_and_run(config=None):
    app = KonsensusApp(config=config)
    app.run()


server = None
api = None
api_endpoint = None


def setUpModule():
    global server
    server_config = make_config()
    server = multiprocessing.Process(target=_cook_and_run, kwargs={'config': server_config})
    server.start()

    global api, api_endpoint
    api_endpoint = 'tcp://0.0.0.0:%s' % server_config['API_PORT']
    api = zerorpc.Client()
    api.connect(api_endpoint)


def tearDownModule():
    os.kill(server.pid, 9)


class UseCaseOneTests(unittest.TestCase):
    """
    For use case #1
    """
    def test_echo(self):
        assert api.echo('hi') == 'hi'

    def test_initial_peers(self):
        # For any reason zerorpc converts empty list to empty tuple
        assert api.list('peers') == ()

    def test_initial_operations(self):
        assert api.list('operations') == {}

    def test_initial_datasets(self):
        assert api.list('datasets') == {api_endpoint: {}}

    def test_use_case_1(self):
        assert api.get_dataset_map() == {}


