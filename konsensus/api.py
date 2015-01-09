"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
from utils import log


class KonsensusAPI(object):

    def __init__(self):
        self.datasets = {}
        self.peers = {}

    @log
    def hello(self, name, *args, **kwargs):
        return "Hello {0}. I am Consensus #{1}".format(name, 1)

    @log
    def get_peers(self):
        """
        Returns a list of peers
        :return:
        """
        self.peers.update({'peer2': {'id': '#2', 'address': 'pc-p282X', 'status': 'Failed'}})
        return self.peers

    @log
    def get_datasets(self):
        self.datasets.update({'ds1': {'id': '#1', 'size': '100MB', 'peer': '#2'}})
        return self.datasets

