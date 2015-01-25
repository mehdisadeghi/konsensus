"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import logging
import random
import zerorpc
import numpy
import zmq


class RandomDatasetStore(object):
    """
    Responsible to store a dataset randomly on the network of peers
    """
    def __init__(self, peers):
        """
        :param peers: list of ip, port tuples
        :return:
        """
        self.peers = peers

    def send_array(self, socket, A, flags=0, copy=True, track=False):
        """send a numpy array with metadata"""
        md = dict(
            dtype=str(A.dtype),
            shape=A.shape,
        )
        socket.send_json(md, flags | zmq.SNDMORE)
        return socket.send(A, flags, copy=copy, track=track)

    def store(self, dataset, dsname):
        """
        Randomly store the given dataset on one of the network peers.
        :return:
        """
        logging.debug('Got a store request for %s' % dsname)

        ip, pub_port, api_port = random.choice(self.peers)
        endpoint = 'tcp://{ip}:{port}'.format(ip=ip, port=api_port)
        c = zerorpc.Client()
        c.connect(endpoint)
        logging.debug('API ready for %s ' % endpoint)

        ctx = zmq.Context()
        socket = ctx.socket(zmq.PUSH)
        temp_port = socket.bind_to_random_port('tcp://127.0.0.1')

        print 'temp port is %s' % temp_port
        logging.debug('***Calling send_array')
        #socket.send_json('A sample text')
        #self.send_array(socket, dataset, copy=False)
        #socket.send(dataset)
        socket.send('Hi')
        logging.debug('***Calling pull')
        # Ask the other peer to pull
        c.pull(dsname, 'tcp://127.0.0.1:%s' % temp_port )
        logging.debug('Dataset %s transferred to %s' % (dsname, endpoint))