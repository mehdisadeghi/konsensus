"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import logging
import argparse
import tempfile
import logging
logging.basicConfig(level=logging.DEBUG)

import defaults
import application


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
    config = defaults.DefaultConfig()
    api_port = get_open_port()
    pub_port = get_open_port()
    peer_id = id
    num, loc = tempfile.mkstemp(suffix='.hdf5', prefix='tmp_')
    config.update({
        'PEER_ID': peer_id,
        'API_PORT': api_port,  # API port. Application will listen for incoming requests
        'PUB_PORT': pub_port,  # Will publish all the news on this port.
        'PEERS': [],  # ('127.0.0.1', 9201) Name of peers to subscribe to their publisher port.
        'HDF5_REPO': loc
        })
    return config


def make_hdf5(path, dsname):
    import h5py, numpy as np
    f = h5py.File(path, 'w')
    ds = f.create_dataset(dsname, (1000000,), dtype='i')
    ds[...] = np.random.random_integers(1, high=100, size=1000000)
    f.close()


def instance_factory(count):
    if count == 0:
        return
    """Launch local application instance on random ports"""
    configs = []
    peers = []
    apps = []

    # Make an entry point node for sake of simplicity and ease of use
    entry_point_config = make_config(0)
    entry_point_config.update({
        'PEER_ID': 0,
        'API_PORT': 9998,  # API port. Application will listen for incoming requests
        'PUB_PORT': 9999,  # Will publish all the news on this port.
        'PEERS': [],  # ('127.0.0.1', 9201) Name of peers to subscribe to their publisher port.
    #    'HDF5_REPO': None
    })
    configs.append(entry_point_config)
    peers.append(('127.0.0.1', entry_point_config.PUB_PORT, entry_point_config.API_PORT))
    for c in xrange(int(count)):
        config = make_config(c + 1)
        peers.append(('127.0.0.1', config.PUB_PORT, config.API_PORT))
        configs.append(config)

    procs = []
    from multiprocessing import Process

    pids = []
    for config in configs:
        logging.debug('*** going to cook an app')
        if config.HDF5_REPO:
            make_hdf5(config.HDF5_REPO, 'ds%s' % config.PEER_ID)
        config.PEERS = [p for p in peers if p != ('127.0.0.1', config.PUB_PORT, config.API_PORT)]
        app = application.KonsensusApp(config.PEER_ID, config=config)
        apps.append(app)
        p = Process(target=app.run)
        procs.append(p)
        p.start()
        print 'App started.'
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
