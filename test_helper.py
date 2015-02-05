"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import argparse
import tempfile

import logging
logging.basicConfig(level=logging.DEBUG)

from konsensus import settings, KonsensusApp


def get_open_port():
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
        return port


def make_config(id):
    """Makes a config with various port numbers and temp folder"""
    config = settings.DefaultSettings()
    api_port = get_open_port()
    pub_port = get_open_port()
    peer_id = id
    num, loc = tempfile.mkstemp(suffix='.hdf5', prefix='tmp_')
    config.update({
        'PEER_ID': peer_id,
        'API_PORT': api_port,  # API port. Application will listen for incoming requests
        'PUB_PORT': pub_port,  # Will publish all the news on this port.
        'PEERS': [],  # ('127.0.0.1', 9201) Name of peers to subscribe to their publisher port.
        'HDF5_REPO': loc,
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': tempfile.mktemp(suffix='.log')
        })
    return config


def make_hdf5(path, dsname):
    import h5py, numpy as np
    f = h5py.File(path, 'w')
    ds = f.create_dataset(dsname, (1000,), dtype='i')
    ds[...] = np.random.random_integers(1, high=100, size=1000)
    f.close()


def _cook_and_run(config=None):
    app = KonsensusApp(config=config)
    app.run()


def instance_factory(count):
    if count == 0:
        return
    """Launch local application instance on random ports"""
    configs = []
    peers = []

    # Make an entry point node for sake of simplicity and ease of use
    entry_point_config = make_config(0)
    entry_point_config.update({
        #'LOG_LEVEL': 'ERROR',
        'PEER_ID': 0,
        'API_PORT': 9998,  # API port. Application will listen for incoming requests
        'PUB_PORT': 9999,  # Will publish all the news on this port.
        'PEERS': [],  # ('127.0.0.1', 9201) Name of peers to subscribe to their publisher port.
    #    'HDF5_REPO': None
    })
    configs.append(entry_point_config)
    peers.append(('127.0.0.1', entry_point_config['PUB_PORT'], entry_point_config['API_PORT']))
    for c in xrange(int(count)):
        config = make_config(c + 1)
        peers.append(('127.0.0.1', config['PUB_PORT'], config['API_PORT']))
        configs.append(config)

    from multiprocessing import Process

    pids = []
    for config in configs:
        #print('Going to cook an app #%s' % config.PEER_ID)
        if config['HDF5_REPO']:
            make_hdf5(config['HDF5_REPO'], 'ds%s' % config['PEER_ID'])
        config.PEERS = [p for p in peers if p != ('127.0.0.1', config['PUB_PORT'], config['API_PORT'])]
        p = Process(target=_cook_and_run, kwargs={'config': config})
        p.start()
        #print('App #%s started.' % config.PEER_ID)
        config.update({'pid': p.pid})
        pids.append(p.pid)

    # Let apps finish starting
    import time
    time.sleep(.1)

    configs.remove(entry_point_config)
    return pids, entry_point_config, configs


if __name__ == '__main__':
    """Helper to make konsensus servers with fake data and run them"""
    parser = argparse.ArgumentParser()
    parser.add_argument('count', help='Creates a number of Konsensus servers with different datasets')
    parser.parse_args()

    args = parser.parse_args()
    instance_factory(args.count)
