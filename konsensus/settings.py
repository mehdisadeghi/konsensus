"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""


class DefaultSettings(dict):
    def __init__(self):
        dict.__init__(self, {
            'API_PORT': 9998,  # API port. Application will listen for incoming requests
            'PUB_PORT': 9999,  # Will publish all the news on this port.
            # Name of peers to subscribe to. Will not publish to self.
            'PEERS': [], # List of tuples(ip, publish_port, api_port)
            'HDF5_REPO': './misc/samples/small.hdf5',
            'LOG_LEVEL': 'DEBUG'
        })