"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""


class DefaultConfig(dict):
    def __init__(self):
        dict.__init__(self)
        self.update({
            'API_PORT': 4200,  # API port. Application will listen for incoming requests
            'PUB_PORT': 4201,  # Will publish all the news on this port.
            'PEERS': [('153.96.75.60', 4200),
                      ('153.96.74.161', 4200)],  # Name of peers to subscribe to. Localhost ip will be ignored
            'HDF5_REPO': '/W5/sade/workspace/hdf5_samples/usecase1.h5'
        })

    def __getattr__(self, item):
        if item in self:
            return self[item]
        else:
            return getattr(self, item)