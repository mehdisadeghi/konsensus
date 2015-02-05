"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import os
import unittest

import zerorpc

from .test_helper import random_instance_factory


api = None
api_endpoint = None
pids = None


def setUpModule():
    global pids, configs
    pids, configs = random_instance_factory(1)

    global api, api_endpoint
    api_endpoint = 'tcp://0.0.0.0:%s' % configs[0]['API_PORT']
    api = zerorpc.Client()
    api.connect(api_endpoint)


def tearDownModule():
    for pid in pids:
        os.kill(pid, 9)


class BasicAPITests(unittest.TestCase):
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
        assert api.list('datasets') == {api_endpoint: {'ds1': {'array size': 1000}}}

    def test_get_dataset_map(self):
        assert api.get_dataset_map() == {'ds1': {'array size': 1000}}