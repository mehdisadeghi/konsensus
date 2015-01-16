"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import os
import logging

import h5py
import numpy as np
from blinker import signal


class KonsensusManager(object):
    """
    Implements the logic.
    """
    def __init__(self):
        self.local_datasets = {}
        self.peers = {}
        self.ops = {}
        self.netmap = {}
        self._topic_handlers = {}
        self._load_datasets()

    def _load_datasets(self):
        for dirpath, dirnames, filenames in os.walk("samples"):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                if filename.endswith(".hdf5"):
                    self.local_datasets[filename] = {'size': os.path.getsize(path)}
        return self.local_datasets

    def _default_handler(self, topic, message):
        """
        Default handler for any message type
        :param topic:
        :param message:
        :return:
        """
        logging.error('No handler assigned for topic %s' % topic)

    def register_handler(self, topic, handler):
        """
        Register a function to handle a zmq topic message
        :param topic:
        :param handler:
        :return:
        """
        if topic in self._topic_handlers:
            self._topic_handlers.append(handler)
        else:
            self._topic_handlers[topic] = [handler]

    def hello(self, name, *args, **kwargs):
        """
        Greeting.
        :param name:
        :param args:
        :param kwargs:
        :return:
        """
        return "Hello {0}. I am Konsensus #{1}".format(name, 1)

    def get_peers(self, *args, **kwargs):
        """
        Returns a list of peers
        :return:
        """
        self.peers.update({'tcp://127.0.0.1:4200':
                               {'id': 'Fake', 'address': 'Fake', 'status': 'Fake'}})
        return self.peers

    def get_dataset_map(self, *args, **kwargs):
        """
        Get the know datasets to this peer
        :return:
        """
        # import os
        # datasets = {}
        # for dirpath, dirnames, filenames in os.walk("samples"):
        #     for filename in filenames:
        #         path = os.path.join(dirpath, filename)
        #         if filename.endswith(".hdf5"):
        #             datasets[filename] = {'size': os.path.getsize(path),
        #                                   'endpoint': 'tcp://127.0.0.1:4200'}
        # #self.datasets.update({'ds#1': {'size': '10000', 'endpoint': 'tcp://127.0.0.1:4200'}})
        # self.datasets.update(datasets)
        return self.local_datasets

    def get_dataset(self, key, *args, **kwargs):
        """
        Get a specifc dataset
        :param key:
        :return:
        """
        if key in self.local_datasets:
            return self.local_datasets[key]

    def run_operation(self, name, *args, **kwargs):
        """
        To run an op
        :return:
        """
        if name in self.ops:
            raise Exception('Operation is already submitted.')

        logging.debug('Recieved a new operation request, signaling.')
        sig = signal('operation.new')
        sig.send(self, op_name=name)
        self.ops[name] = name

    def get_commands(self):
        return self.ops

    def get_net_map(self):
        return self.netmap

    def handle_topic(self, topic, message):
        """
        Lookup topic handler and call them
        :param topic:
        :param message:
        :return:
        """
        if topic not in self._topic_handlers:
            return self._default_handler(topic, message)
        else:
            for handler in self._topic_handlers[topic]:
                handler()

    @look_for_dataset()
    def use_case_1(self, dataset):
        """
        Invokes operation for use case 1 described in the report
        :param dataset:
        :return:
        """
        pass


def look_for_dataset(func, dataset):
    """
    Check if dataset is available locally
    :param func:
    :param dataset:
    :return:
    """
    pass