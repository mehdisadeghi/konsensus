"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import tempfile
from multiprocessing import Process

from . import KonsensusApp
from .settings import DefaultSettings


def make_config(dataset_id=None):
    """Makes a config with various port numbers and temp folder"""
    config = DefaultSettings()
    if not dataset_id:
        import uuid
        dataset_id = str(uuid.uuid4())
    api_port = get_open_port()
    pub_port = get_open_port()
    num, loc = tempfile.mkstemp(suffix='.hdf5', prefix='tmp_test_')
    import h5py
    f = h5py.File(loc)
    f.close()
    config.update({
        'DS_ID': dataset_id,
        'API_PORT': api_port,  # API port. Application will listen for incoming requests
        'PUB_PORT': pub_port,  # Will publish all the news on this port.
        'PEERS': [],  # ('127.0.0.1', 9201) Name of peers to subscribe to their publisher port.
        'HDF5_REPO': loc,
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': tempfile.mktemp(suffix='.log')})
    return config


def get_open_port():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


def _cook_and_run(config=None):
    app = KonsensusApp(config=config)
    app.run()


def make_hdf5(path, dsname):
    import h5py, numpy as np
    f = h5py.File(path, 'w')
    ds = f.create_dataset(dsname, (1000,), dtype='i')
    ds[...] = np.random.random_integers(1, high=100, size=1000)
    f.close()


def instance_factory(count):
    """Launch local application instance on random ports"""
    configs = []
    peers = []
    pids = []

    if count > 0:
        # Make an entry point node for sake of simplicity and ease of use
        entry_point_config = DefaultSettings()
        entry_point_config.update({
            'DS_ID': 'ds0',
            'PEERS': []
        })
        configs.append(entry_point_config)
        peers.append(('127.0.0.1', entry_point_config['PUB_PORT'], entry_point_config['API_PORT']))
        count -= 1
        for c in xrange(count):
            config = make_config('ds%s' % (c + 1))
            peers.append(('127.0.0.1', config['PUB_PORT'], config['API_PORT']))
            configs.append(config)

        for config in configs:
            make_hdf5(config['HDF5_REPO'], config['DS_ID'])
            config['PEERS'] = [p for p in peers if p != ('127.0.0.1', config['PUB_PORT'], config['API_PORT'])]
            p = Process(target=_cook_and_run, kwargs={'config': config})
            p.start()
            config.update({'pid': p.pid})
            pids.append(p.pid)

        return pids, configs
