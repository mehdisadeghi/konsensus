"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""


class KonsensusAPI(object):
    """
    Public RPC API to be exposed
    """
    def __init__(self, manager):
        self._manager = manager

    def echo(self, msg):
        return msg

    def hello(self, name, *args, **kwargs):
        """
        Greeting.
        :param name:
        :param args:
        :param kwargs:
        :return:
        """
        return "Hello {0}. I am Konsensus #{1}".format(name, 1)

    def get_dataset_map(self, *args, **kwargs):
        """
        Get the know datasets to this peer
        :return:
        """
        return self._manager.get_dataset_map(*args, **kwargs)

    def run_operation(self, name, *args, **kwargs):
        """
        To run an op
        :return:
        """
        return self._manager.run_operation(name, *args, **kwargs)

    def get_operations(self):
        """
        Returns list of commands.
        :return:
        """
        return self._manager.get_commands()

    def use_case_1(self, dataset, *args, **kargs):
        return self._manager.use_case_1(dataset, *args, **kargs)