"""
    consensus
    ~~~~~~~~~

    This file is part of kansensus project.
"""

from utils import log

__author__ = 'Mehdi Sadeghi'


class KonsensusAPI(object):
    @log
    def hello(self, name, *args, **kwargs):
        return "Hello {0}. I am Consensus #{1}".format(name, 1)

    @log
    def get_peers(self):
        """
        Returns a list of peers
        :return:
        """
        peers = {'peer2': {'id': '#2', 'address': 'pc-p282X', 'status': 'Failed'}}
        return peers