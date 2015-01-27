"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import socket

import zmq.green as zmq
import msgpack
import blinker

import constants
import helpers


class ZMQTopicHandlerBase(object):
    def __init__(self):
        object.__init__(self)
        import logging
        self.logger = logging.getLogger(__name__)

    def get_topic(self):
        return NotImplementedError()

    def handle(self, *args, **kwargs):
        return NotImplementedError


class DelegateTopicHandler(ZMQTopicHandlerBase):
    def __init__(self):
        ZMQTopicHandlerBase.__init__(self)

    def get_topic(self):
        return constants.DELEGATE_TOPIC

    def handle(self, manager, delegate_info):
        self.logger.debug('Got a delegate handle request')

        # Check data availability
        if not manager.has_dataset(delegate_info['dataset']):
            # Don't need to do anything, pass with a message
            self.logger.debug('Ignoring %s, no dataset %s' % (delegate_info['command'],
                                                              delegate_info['dataset']))
            return

        # Check command availability
        if not manager.has_command(delegate_info['command']):
            # Have the data but not the command
            self.logger.debug('Ignoring a request for %s on dataset %s. The command is not available' %
                          (delegate_info['command'],
                           delegate_info['dataset']))
            return

        # Adding host info to the delegate_info

        delegate_info['peer'] = socket.gethostname()

        # Inform other peers that we take care of the operation
        helpers.publish(self,
                        topic=constants.DELEGATE_ACCEPTED_TOPIC,
                        delegate_id=delegate_info['delegate_id'],
                        peer=delegate_info['peer'])

        self.logger.debug('Running the delegated command.')
        #TODO: A central service repository is required
        func = getattr(manager, delegate_info['command'])
        dataset = delegate_info['dataset']
        del(delegate_info['dataset'])
        #TODO: It should be done without this flag here
        delegate_info['is_delegate'] = True
        result = func(dataset, **delegate_info)
        self.logger.debug('Delegated command finished with result: %s' % result)

        # # Also inform other parts of the app about accepting
        # accept_signal = signal(constants.DELEGATE_ACCEPTED_SIG)
        # accept_signal.send(self, id=delegate_info['id'])


class DelegateAcceptedTopicHandler(ZMQTopicHandlerBase):
    def __init__(self):
        ZMQTopicHandlerBase.__init__(self)

    def get_topic(self):
        return constants.DELEGATE_ACCEPTED_TOPIC

    def handle(self, manager, info):
        self.logger.debug('Got a delegate accepted handle request')

        # Signaling the interested parties about it
        accept_signal = blinker.signal(constants.PEER_ACCEPTED_DELEGATE_SIG)
        accept_signal.send(self, info=info)


class PullRequestTopicHandler(ZMQTopicHandlerBase):
    def __init__(self):
        ZMQTopicHandlerBase.__init__(self)

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
            self.logger.debug('Connecting to the peer to pull dataset %s' % info['dataset_name'])
        ctx = zmq.Context()
        socket = ctx.socket(zmq.PULL)
        socket.connect(info['endpoint'])
        #dataset = socket.recv()
        array = helpers.recv_array(socket)
        self.logger.debug('Fetching finished, going to unpack and save dataset.')
        manager.store_array(array, info['dataset_name'])
        # Unpack
        #unpacked = msgpack.unpackb(dataset)
