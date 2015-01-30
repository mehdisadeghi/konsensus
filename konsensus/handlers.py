"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import socket

import zmq.green as zmq
import blinker

from . import constants
from . import helpers


class ZMQTopicHandlerBase(object):
    def __init__(self):
        object.__init__(self)
        import logging
        self.logger = logging.getLogger(__name__)

    def get_topic(self):
        return NotImplementedError()

    def handle(self, manager, message_dict):
        return NotImplementedError


class DelegateTopicHandler(ZMQTopicHandlerBase):
    """
    Responsible to handle delegation news.
    """

    def get_topic(self):
        return constants.DELEGATE_TOPIC

    def handle(self, manager, delegate_info):
        self.logger.debug('Got a delegate handle request, checking data availability')

        # Check data availability
        if not manager.has_dataset(delegate_info['dataset_id']):
            # Don't need to do anything, pass with a message
            self.logger.debug('Ignoring %s, no dataset %s' % (delegate_info['command'],
                                                              delegate_info['dataset_id']))
            return

        # Check command availability
        if not manager.has_command(delegate_info['command']):
            # Have the data but not the command
            self.logger.debug('Ignoring a request for %s on dataset %s. The command is not available' %
                              (delegate_info['command'],
                               delegate_info['dataset_id']))
            raise Exception('Unknown command %s ' % delegate_info['command'])

        # Adding host info to the delegate_info
        from .application import app
        delegate_info['peer'] = app.get_api_endpoint()  # socket.gethostname()

        # Inform other peers that we take care of the operation
        helpers.publish(self,
                        constants.DELEGATE_ACCEPTED_TOPIC,
                        operation_id=delegate_info['operation_id'],
                        peer=delegate_info['peer'])

        self.logger.debug('Running the delegated command.')
        #TODO: A central service repository is required
        func = getattr(manager, delegate_info['command'])
        dataset_id = delegate_info.pop('dataset_id')
        #TODO: It should be done without this flag here
        delegate_info['is_delegate'] = True
        func(dataset_id, **delegate_info)


class DelegateAcceptedTopicHandler(ZMQTopicHandlerBase):
    """
    Responsible to handle delegation accepted news.
    """

    def get_topic(self):
        return constants.DELEGATE_ACCEPTED_TOPIC

    def handle(self, manager, info):
        self.logger.debug('A delegate for operation %s accepted by %s' %
                          (info['operation_id'], info['peer']))

        # Signaling the interested parties about it
        accept_signal = blinker.signal(constants.PEER_ACCEPTED_DELEGATE_SIG)
        accept_signal.send(self, info=info)


class PullRequestTopicHandler(ZMQTopicHandlerBase):
    """
    Responsible to handle pull request news
    """
    def get_topic(self):
        return constants.PULL_REQUEST_TOPIC

    def handle(self, manager, info):
        """

        :param manager:
        :param info:
        :return:
        """
        # Check if I should handle this
        target_ip, target_port = info['target']
        if not helpers.is_running_instance(manager.config, target_ip, target_port):
            self.logger.debug('Ignoring a pull request which is not for us')
            return
            self.logger.debug('Connecting to the peer to pull dataset %s' %
                              info['dataset_id'])
        ctx = zmq.Context()
        pull_socket = ctx.socket(zmq.PULL)
        pull_socket.connect(info['endpoint'])
        array = helpers.recv_array(pull_socket)
        self.logger.debug('Fetching finished, going to unpack and save dataset.')
        manager.store_array(array, info['dataset_id'])


class DistributedOperationNewsHandler(ZMQTopicHandlerBase):
    """
    Responsible to handle incoming news about operations running on the network.
    """
    def get_topic(self):
        return constants.OPERATION_NEWS_TOPIC

    def handle(self, manager, operation_info):
        """
        Handle the news
        :param manager:
        :param operation_info:
        :return:
        """
        self.logger.debug('Got operation news, going to update store %s' % operation_info)
        store = manager.get_operation_store()
        operation_id = operation_info.pop('operation_id')
        # Update the store but don't publish anything since we are not initiator
        store.update(operation_id, publish=False, **operation_info)

        #
        # # Check if we are the collector
        #
        #


