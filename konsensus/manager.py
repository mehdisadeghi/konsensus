"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import uuid
import functools
import gevent
import socket

import blinker
import h5py
import numpy as np
#import zmq.green as zmq

import constants
import helpers
from konsensus.store import RandomDatasetStore


def delegate(func):
    @functools.wraps(func)
    def new_func(self, dataset, result_ds_name, *args, **kwargs):
        # If the function has already been delegated don't circulate it again.
        if 'is_delegate' in kwargs:
            self.logger.debug('Ignoring delegation try for already delegated request')
            return func(self, dataset, result_ds_name, *args, **kwargs)

        if dataset not in self.local_datasets:
            self.logger.debug('Dataset %s is not available locally, trying to delegate.' % dataset)

            # Make an id
            operation_id = str(uuid.uuid4())

            helpers.publish(self,
                            topic=constants.DELEGATE_TOPIC,
                            command=func.__name__,
                            dataset=dataset,
                            result_ds_name=result_ds_name,
                            delegate_id=operation_id)
            self.logger.debug('Request for dataset %s published, waiting for someone to answer' % dataset)

            global reply
            reply = None

            def accept_handler(sender, info=None):
                self.logger.debug('Delegate request for dataset %s accepted' % dataset)
                global reply
                reply = info

            accept_signal = blinker.signal(constants.PEER_ACCEPTED_DELEGATE_SIG)
            accept_signal.connect(accept_handler)

            gevent.sleep(3)

            def proxy_func(*args, **kwargs):
                global reply
                if reply:
                    return 'You request with id %s is delivered to peer %s' % (reply['delegate_id'], reply['peer'])
            return proxy_func()
            # return proxy or call remote and return the result

        else:
            return func(self, dataset, result_ds_name, *args, **kwargs)
    return new_func


class KonsensusManager(object):
    """
    Implements the logic.
    """
    def __init__(self, config):
        import logging
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.local_datasets = {}
        self.peers = {}
        self.ops = {}
        self.netmap = {}
        self._topic_handlers = {}
        self._load_datasets()
        self._store = RandomDatasetStore(config.PEERS)

    def _load_datasets(self):
        import h5py
        if self.config.HDF5_REPO:
            f = h5py.File(self.config.HDF5_REPO, 'r')
            for key, value in f.iteritems():
                if isinstance(value, h5py.Dataset):
                    self._add_dataset(key, value)
            f.close()

    def _add_dataset(self, key, dataset):
        """
        Adds an h5py dataset to local dataset list
        :param key:
        :param dataset:
        :return:
        """
        self.local_datasets[key] = {'array size': dataset.size}

    def _default_handler(self, topic, messages):
        """
        Default handler for any message type
        :param topic:
        :param messages:
        :return:
        """
        self.logger.error('No handler assigned for topic %s' % topic)

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

        self.logger.debug('Recieved a new operation request, signaling.')
        sig = blinker.signal('operation.new')
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
    def use_case_1(self, dataset, result_ds_name, **kwargs):
        """
        Invokes operation for use case 1 described in the report
        :param dataset:
        :return:
        """

        hostname = socket.gethostname()
        self.logger.debug('Request for use case 1 with dataset name %s at host %s received' % (dataset, hostname))

        if dataset not in self.local_datasets:
            raise Exception("We don't have this dataset: %s" % dataset)

        f = h5py.File(self.config.HDF5_REPO, 'r')
        result = np.array(f.get(dataset))
        for i in xrange(len(result)):
            result[i] = np.mod(result[i], 2)
        f.close()
        # Save into the store
        self._store.store(result, result_ds_name)

        return str(result)

    # def pull_request(self, dsname, endpoint):
    #     """
    #     Will pull the dataset from the endpoint peer
    #     :param dsname:
    #     :param endpoint:
    #     :return:
    #     """
    #     helpers.whoami(self.config)
    #     logging.debug('Got a pull request for %s and endpoint %s' % (dsname, endpoint))
    #     ctx = zmq.Context()
    #     socket = ctx.socket(zmq.PULL)
    #     socket.connect(endpoint)
    #     work = self.recv_array(socket)
    #     logging.debug('Pulled dataset %s from endpoint %s' % (dsname, endpoint))

    def store_array(self, array, name):
        """
        Store the given numpy array into hdf5 repo
        :param dataset:
        :param name:
        :return:
        """
        helpers.whoami(self.config)
        self.logger.debug('Opening hdf5 repo %s' % self.config.HDF5_REPO)
        f = h5py.File(self.config.HDF5_REPO, 'r+')
        ds = None
        if name in self.local_datasets:
            self.logger.warning('Going to override dataset %s' % name)
            ds = f[name]
        else:
            ds = f.create_dataset(name, array.shape, array.dtype)
        ds[...] = array
        self._add_dataset(name, ds)
        f.close()

    def list(self, command):
        """
        List datasets or peers
        :param command:
        :return:
        """
        import zerorpc
        if command in ('data', 'datasets'):
            datasets = {}
            for peer_ip, pub_port, api_port in self.config.PEERS:
                key = '%s:%s' % (peer_ip, api_port)
                datasets[key] = []
                c = zerorpc.Client()
                c.connect('tcp://%s:%s' % (peer_ip, api_port))
                datasets[key].append(c.get_dataset_map())
            return datasets

        if command in ('peer', 'peers'):
            return self.config.PEERS