"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import os

import zerorpc
import h5py
import numpy as np

from . import decorators
from .store import RandomDatasetStore, DistributedOperationStore


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
        self._store = RandomDatasetStore(config['PEERS'])
        self._operation_store = DistributedOperationStore(self)

    def _load_datasets(self):
        try:
            import h5py
            if not os.path.exists(self.config['HDF5_REPO']):
                f = h5py.File(self.config['HDF5_REPO'], 'w')
                f.close()
            else:
                f = h5py.File(self.config['HDF5_REPO'], 'r')
                for key, value in f.iteritems():
                    if isinstance(value, h5py.Dataset):
                        self._add_dataset(key, value)
                f.close()
        except Exception, e:
            raise Exception('Problem loading/creating local repository, %s' % e)

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

    def get_remote_dataset(self, endpoint, dataset_id):
        """
        Fetch a dataset from a remote peer
        :return:
        """
        c = zerorpc.Client()
        c.connect(endpoint)
        dataset = c.get_dataset(dataset_id)
        return dataset

    def get_dataset(self, dataset_id):
        """
        Get a specifc dataset
        :param dataset_id: dataset id
        :return: np.array
        """
        if not self.has_dataset(dataset_id):
            # Fetch remote dataset
            # TODO: We can maintain distributed dataset table same as operation and peers
            global_dataset = self.get_global_dataset_map()
            for peer_api_endpoint, map in global_dataset.iteritems():
                if dataset_id in map:
                    return self.get_remote_dataset(peer_api_endpoint, dataset_id)
            raise Exception('Dataset %s neither is available locally nor on the network' % dataset_id)

        f = h5py.File(self.config['HDF5_REPO'], 'r')
        return np.array(f.get(dataset_id))

    def get_global_dataset_map(self):
        """
        Return a list of all datasets present on the network
        :return:
        """
        datasets = {}
        if self.config.get('PEERS'):
            for peer_ip, pub_port, api_port in self.config['PEERS']:
                key = 'tcp://%s:%s' % (peer_ip, api_port)
                c = zerorpc.Client()
                c.connect('tcp://%s:%s' % (peer_ip, api_port))
                datasets[key] = c.get_dataset_map()

        # Add local ones as well
        from .application import app
        datasets[app.get_id()] = self.get_dataset_map()
        return datasets

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

    def has_dataset(self, dataset_id):
        """
        Check if we have the dataset available
        :param dataset:
        :return:
        """
        return dataset_id in self.local_datasets

    def get_peers(self):
        """Returns the peers"""
        return self.config['PEERS']

    def get_operation(self, operation_id):
        return self.get_operation_store().get(operation_id)
    @decorators.delegate
    def use_case_1(self, dataset_id, **kwargs):
        """
        Invokes operation for use case 1 described in the report
        :param dataset_id:
        :return:
        """
        from .application import app
        # self.logger.debug('Request for use case 1 with dataset_id %s at host %s received' %
        #                   (dataset_id, app.get_api_endpoint()))

        if dataset_id not in self.local_datasets:
            raise Exception("I don't have dataset %s" % dataset_id)

        f = h5py.File(self.config['HDF5_REPO'], 'r')
        result = np.array(f.get(dataset_id))
        for i in xrange(len(result)):
            result[i] = np.mod(result[i], 2)
        f.close()
        # Save into the store
        result_dataset_id = self._store.store(result)

        # Update distributed operation store
        self._operation_store.update(result_dataset_id=result_dataset_id, **kwargs)

        # Don't return anything. The operation id will be returned by decorator.
        # Everything is async here.
        #return str(result)

    @decorators.distribute_linear(use_case_1)
    def use_case_2(self, dataset_ids, **kwargs):
        """
        A linear operation on first and second datasets.
        :param first_id: first dataset id
        :param second_id: second dataset id
        :returns:
        """
        assert len(dataset_ids) == 2
        arrays = []
        for dataset_id in dataset_ids:
            array = self.get_dataset(dataset_id)
            #if array.size != last_array.size:
            #    return Exception('Datasets should be of the same size')
            arrays.append(array)
            #last_array = array
        # Use one arbitrary array to sum up the rest
        result = list(arrays.pop())

        for array in arrays:
            for i in xrange(len(array)):
                result[i] += array[i]

        npresult = np.array(result)
        # Save into the store
        result_dataset_id = self._store.store(npresult)

        # Update distributed operation store
        self._operation_store.update(result_dataset_id=result_dataset_id, **kwargs)

    def store_array(self, array, name):
        """
        Store the given numpy array into hdf5 repo
        :param dataset:
        :param name:
        :return:
        """
        f = h5py.File(self.config['HDF5_REPO'], 'r+')
        ds = None
        if name in self.local_datasets:
            self.logger.warning('Going to override dataset %s' % name)
            ds = f[name]
        else:
            ds = f.create_dataset(name, array.shape, array.dtype)
        ds[...] = array
        self._add_dataset(name, ds)
        self.logger.debug('Dataset stored in hdf5 repo %s' % self.config['HDF5_REPO'])
        f.close()

    def list(self, command):
        """
        List datasets or peers
        :param command:
        :return:
        """
        #TODO: Use a proper command registration technique
        if command in ('data', 'datasets'):
            return self.get_global_dataset_map()

        elif command == 'peers':
            return self.get_peers()

        elif command == 'operations':
            return self.get_operations()

        else:
            return Exception('Not implemented command: %s' % command)