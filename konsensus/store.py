"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import random

import gevent
import zmq.green as zmq

import constants
import helpers


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
        import logging
        self.logger = logging.getLogger(__name__)

    def store(self, dataset, dsname):
        """
        Randomly store the given dataset on one of the network peers.
        :return:
        """
        # Publishing a pull request to make other peer ready for pull
        ip, pub_port, api_port = random.choice(self.peers)
        target = 'tcp://{ip}:{port}'.format(ip=ip, port=api_port)

        ctx = zmq.Context()
        socket = ctx.socket(zmq.PUSH)
        #TODO: Use streaming and do not recreate new push ports
        temp_port = socket.bind_to_random_port('tcp://127.0.0.1')
        self.logger.debug('Opened the pull port at %s' % temp_port)

        helpers.publish(self,
                        topic=constants.PULL_REQUEST_TOPIC,
                        dataset_name=dsname,
                        target=(ip, api_port),
                        endpoint='tcp://127.0.0.1:%s' % temp_port)

        # Let the other peer to catch the signal
        gevent.sleep(.1)

        self.logger.debug('Calling send_array')
        helpers.send_array(socket, dataset)