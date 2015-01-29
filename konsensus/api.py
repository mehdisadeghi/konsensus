"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import zerorpc


class KonsensusAPI(object):
    """
    Public RPC API to be exposed
    """
    def __init__(self, manager):
        self._manager = manager

    def echo(self, msg):
        """Echo"""
        return msg

    def get_dataset_map(self, *args, **kwargs):
        """
        Get the know datasets to this peer
        :return:
        """
        return self._manager.get_dataset_map(*args, **kwargs)

    # def run_operation(self, name, *args, **kwargs):
    #     """
    #     To run an op
    #     :return:
    #     """
    #     return self._manager.run_operation(name, *args, **kwargs)

    def dummy(self):
        return self._manager.dummy()

    # def get_operations(self):
    #     """
    #     Returns list of commands.
    #     :return:
    #     """
    #     return self._manager.get_commands()

    def use_case_1(self, dataset, *args, **kargs):
        """
        Invokes operation for use case 1 described in the report. Accepts 'peers', 'datasets' and 'operations as option.
        :param dataset:
        :return:
        """
        return self._manager.use_case_1(dataset, *args, **kargs)

    # @zerorpc.stream
    # def store(self, *args, **kwargs):
    #     """
    #     Store the given dataset in the hdf5 repo
    #     :param dataset:
    #     :param name:
    #     :return:
    #     """
    #     return self._manager.store(*args, **kwargs)

    # def pull_request(self, *args, **kwargs):
    #     return self._manager.pull_request(*args, **kwargs)

    def list(self, command):
        """
        Return a list of datasets or peers
        :param command: "data" or "peers"
        :return:
        """
        return self._manager.list(command)