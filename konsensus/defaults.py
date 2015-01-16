"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""


class DefaultConfig(object):
    # API port. Application will listen for incoming requests
    API_PORT = 4200

    # Will publish all the news on this port.
    PUB_PORT = 4201

    # Name of peers to subscribe to. Localhost ip will be ignored
    PEERS = ['153.96.75.60',
             '153.96.74.161']

    DATASET_PATH = '~/workspace/hdf5_samples'