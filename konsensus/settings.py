"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""


class DefaultSettings(dict):
    def __init__(self):
        dict.__init__(self)
        self.update({
            'API_PORT': 4200,  # API port. Application will listen for incoming requests
            'PUB_PORT': 4201,  # Will publish all the news on this port.
            # Name of peers to subscribe to. Will not publish to self.
            'PEERS': [('153.96.75.60', 4201, 4200),
                      ('153.96.74.161', 4201, 4200)],
            'HDF5_REPO': './misc/samples/small.hdf5',
            'LOG_LEVEL': 'DEBUG'
        })