"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import random
import uuid
import time
import logging
logger = logging.getLogger(__name__)

import gevent
import zmq.green as zmq

from . import constants
from . import helpers


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

    def store(self, dataset, dataset_tags=None):
        """
        Randomly store the given dataset on one of the network peers.
        :return:
        """
        # Publishing a pull request to make other peer ready for pull
        ip, pub_port, api_port = random.choice(self.peers)
        target = 'tcp://{ip}:{port}'.format(ip=ip, port=api_port)

        # ctx = zmq.Context()
        # socket = ctx.socket(zmq.PUSH)
        # #TODO: Use streaming and do not recreate new push ports
        # temp_port = socket.bind_to_random_port('tcp://127.0.0.1')
        # logger.debug('Opened the pull port at %s' % temp_port)
        # logger.debug('Randomly storing dataset on %s' % target)
        # Make an id for result dataset
        dataset_id = str(uuid.uuid4())

        # Wait a little until ports are ready
        #gevent.sleep(.1)

        from .application import app, temp_repo
        temp_repo[dataset_id] = dataset

        # Inform others to come and get it
        helpers.publish(self,
                        constants.PULL_REQUEST_TOPIC,
                        dataset_id=dataset_id,
                        target=(ip, api_port),
                        #endpoint='tcp://127.0.0.1:%s' % temp_port,
                        api_endpoint=app.get_api_endpoint())

        # Let the other peer to catch the signal
        #gevent.sleep(.1)

        # logger.debug('Calling send_array')
        #helpers.send_array(socket, dataset)

        return dataset_id


class DistributedOperationStore(dict):
    """
    Responsible for generating operation request ids and notify peers about
    operation updates and keep requested operation state details.
    """
    def __init__(self, manager):
        self._manager = manager

    def store(self, *args, **kwargs):
        """
        Stores operation info and notify peers to do so
        :param args:
        :param kwargs:
        :return:
        """
        # logger.debug('Got a distributed operation store request %s %s' % (args, kwargs))
        operation_id = str(uuid.uuid4())
        self[operation_id] = kwargs
        kwargs['submit_moment'] = time.time()
        state = constants.OperationState.init.value
        # logger.debug('Operation %s entering %s state' % (operation_id, state))
        kwargs['state'] = state
        kwargs['args'] = args

        # Publish news about our operation, others will catch up with us
        helpers.publish(self,
                        constants.OPERATION_NEWS_TOPIC,
                        operation_id=operation_id,
                        **kwargs)
        return operation_id

    def update(self, operation_id=None, publish=True, **info):
        """
        Update the operation information and notify peers to update
        :param operation_id:
        :param info:
        :return:
        """
        # If operation_id is not provided explicitly check if it is inside kwargs
        if not operation_id and 'operation_id' not in info:
            raise Exception("Operation id is not provided, can't update the store.")

        # First check if we have this operation
        # logger.debug('Got an operation update request %s %s' %
        #              (operation_id, info))
        if operation_id not in self:
            self[operation_id] = info
        else:
            # Update our info about operation
            self[operation_id].update(info)

        # If we are the initiator peer we will publish this otherwise not.
        if publish:
            # Make sure required information are included
            info['operation_id'] = operation_id
            # Publish news about our operation, others will catch up with us
            helpers.publish(self,
                            constants.OPERATION_NEWS_TOPIC,
                            **info)