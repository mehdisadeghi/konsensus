"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import os
import uuid
import functools
import logging
import gevent

import h5py
import numpy as np
from blinker import signal

import constants


def delegate(func):
    @functools.wraps(func)
    def new_func(self, dataset, *args, **kwargs):
        # If the function has already been delegated don't circulate it again.
        if 'is_delegate' in kwargs:
            logging.debug('Ignoring delegation try for already delegated request')
            return func(self, dataset, *args, **kwargs)

        if dataset not in self.local_datasets:
            logging.debug('Dataset %s is not available locally, trying to delegate.' % dataset)

            # Make an id
            operation_id = str(uuid.uuid4())

            publish = signal(constants.PUBLISH)
            publish.send(self,
                         topic=constants.DELEGATE_TOPIC,
                         command=func.__name__,
                         dataset=dataset,
                         id=operation_id)
            logging.debug('Request for dataset %s published, waiting for someone to answer' % dataset)

            def accept_handler(sender, info=None):
                logging.debug('Delegate request for dataset %s accepted with this info: %s' % (dataset, info))

            accept_signal = signal(constants.PEER_ACCEPTED_DELEGATE_SIG)
            accept_signal.connect(accept_handler)

            gevent.sleep(5)

            # return proxy or call remote and return the result

        else:
            return func(self, dataset, *args, **kwargs)
    return new_func


class KonsensusManager(object):
    """
    Implements the logic.
    """
    def __init__(self, config):
        self.config = config
        self.local_datasets = {}
        self.peers = {}
        self.ops = {}
        self.netmap = {}
        self._topic_handlers = {}
        self._load_datasets()

    def _load_datasets(self):
        # for dirpath, dirnames, filenames in os.walk(self.config.DATASET_PATH):
        #     for filename in filenames:
        #         path = os.path.join(dirpath, filename)
        #         if filename.endswith('.hdf5') or filename.endswith('.h5'):
        #             self.local_datasets[filename] = {'size': os.path.getsize(path)}

        f = h5py.File(self.config.HDF5_REPO, 'r')
        for key, value in f.iteritems():
            if isinstance(value, h5py.Dataset):
                self.local_datasets[key] = {'array size': value.size}

    def _default_handler(self, topic, messages):
        """
        Default handler for any message type
        :param topic:
        :param messages:
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

    def handle_topic(self, topic, messages):
        """
        Lookup topic handler and call them
        :param topic:
        :param messages:
        :return:
        """
        #logging.debug('Manager: got a handling request for topic %s. Registered handlers: %s' % (topic, self._topic_handlers))
        if topic not in self._topic_handlers:
            return self._default_handler(topic, messages)
        else:
            for handler in self._topic_handlers[topic]:
                handler.handle(self, messages)

    def has_dataset(self, dataset):
        """
        Check if we have the dataset available
        :param dataset:
        :return:
        """
        return dataset in self.local_datasets

    def has_command(self, command):
        """
        Check if the given string represents a method on this class
        :param command:
        :return:
        """
        return hasattr(self, command)

    def get_peers(self):
        """Returns the peers"""
        return self.config.PEERS

    @delegate
    def use_case_1(self, dataset, *args, **kargs):
        """
        Invokes operation for use case 1 described in the report
        :param dataset:
        :return:
        """
        import socket
        hostname = socket.gethostname()
        logging.debug('Request for use case 1 with dataset name %s at host %s received' % (dataset, hostname))

        if dataset not in self.local_datasets:
            raise Exception("We don't have this dataset: %s" % dataset)

        f = h5py.File(self.config.HDF5_REPO, 'r')
        result = np.array(f.get(dataset))
        for i in xrange(len(result)):
            result[i] = np.mod(result[i], 2)

        return str(result)

