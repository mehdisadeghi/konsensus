"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import socket

import blinker
import h5py
import numpy as np

import helpers
import decorators
from konsensus.store import RandomDatasetStore, DistributedOperationStore


class KonsensusManager(object):
    """
    Implements the logic.
    """
    def __init__(self, config):
        import logging
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.local_datasets = {}
        self._load_datasets()
        self._store = RandomDatasetStore(config.PEERS)
        self._operation_store = DistributedOperationStore(self)

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

    def get_operation_store(self):
        """
        Returns operation store
        :return:
        """
        return self._operation_store

    def get_dataset(self, dataset_id, *args, **kwargs):
        """
        Get a specifc dataset
        :param dataset_id: dataset id
        :return: np.array
        """
        f = h5py.File(self.config.HDF5_REPO, 'r')
        #if dataset_id not in self.local_datasets:
        if dataset_id not in f:
            raise Exception('Dataset %s is not available in local repository' % dataset_id)

        return np.array(f.get(dataset_id))

    # def run_operation(self, name, *args, **kwargs):
    #     """
    #     To run an op
    #     :return:
    #     """
    #     if name in self.ops:
    #         raise Exception('Operation is already submitted.')
    #
    #     self.logger.debug('Received a new operation request, signaling.')
    #     sig = blinker.signal('operation.new')
    #     sig.send(self, op_name=name)
    #     self.ops[name] = name

    def get_dataset_map(self, *args, **kwargs):
        """
        Get the know datasets to this peer
        :return:
        """
        return self.local_datasets

    def get_operations(self):
        """
        Get the current operation list (in memory)
        :return:
        """
        return self._operation_store

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

    @decorators.delegate
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
        self._store.store(result, dataset_id=result_ds_name)

        return str(result)

    def use_case_2(self, first_id, second_id, **kwargs):
        """
        A linear operation on first and second datasets.
        :param first_id: first dataset id
        :param second_id: second dataset id
        :returns:
        """
        first_array = self.get_dataset(first_id)
        second_array = self.get_dataset(second_id)
        if first_array.size != second_array.size:
            return Exception('Datasets should be of the same size')
        result = []
        for i in xrange(first_array.size):
            result.append(first_array[i] + second_array[i])

        npresult = np.array(result)
        dataset_id = self._store.store(npresult)

    @decorators.register
    def dummy(self):
        pass

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
        #TODO: Use a proper command registration technique
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

        elif command == 'peers':
            return self.config.PEERS

        elif command == 'operations':
            return self.get_operations()

        else:
            return Exception('Not implemented command: %s' % command)