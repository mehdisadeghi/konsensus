"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import logging
from utils import log
#import h5py
#import numpy as np
from blinker import signal

class KonsensusAPI(object):

    def __init__(self):
        self.datasets = {}
        self.peers = {}
        self.ops = {}
        self.netmap = {}

    @log
    def hello(self, name, *args, **kwargs):
        """
        Greeting.
        :param name:
        :param args:
        :param kwargs:
        :return:
        """
        return "Hello {0}. I am Konsensus #{1}".format(name, 1)

    @log
    def get_peers(self, *args, **kwargs):
        """
        Returns a list of peers
        :return:
        """
        self.peers.update({'tcp://127.0.0.1:4200':
                               {'id': 'Fake', 'address': 'Fake', 'status': 'Fake'}})
        return self.peers

    @log
    def get_dataset_map(self, *args, **kwargs):
        """
        Get the know datasets to this peer
        :return:
        """
        import os
        datasets = {}
        for dirpath, dirnames, filenames in os.walk("samples"):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                if filename.endswith(".hdf5"):
                    datasets[filename] = {'size': os.path.getsize(path),
                                          'endpoint': 'tcp://127.0.0.1:4200'}
        #self.datasets.update({'ds#1': {'size': '10000', 'endpoint': 'tcp://127.0.0.1:4200'}})
        self.datasets.update(datasets)
        return self.datasets

    def get_dataset(self, key, *args, **kwargs):
        """
        Get a specifc dataset
        :param key:
        :return:
        """
        if key in self.datasets:
            return self.datasets[key]

    def run_operation(self, name, *args, **kwargs):
        """
        To run an op
        :return:
        """
        logging.debug('Recieved a new operation request, signaling.')
        sig = signal('operation.new')
        logging.debug('signal receivers: %s' % sig.receivers)
        sig.send(self, op_name=name)
        self.ops[name] = name

    def get_operations(self):
        return self.ops

    def get_net_map(self):
        return self.netmap