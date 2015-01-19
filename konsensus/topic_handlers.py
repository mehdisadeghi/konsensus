"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import socket
import logging

from blinker import signal

from konsensus import constants


class ZMQTopicHandlerBase(object):

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
        logging.debug('Got a delegate handle request for message %s' % delegate_info)

        # Check data availability
        if not manager.has_dataset(delegate_info['dataset']):
            # Don't need to do anything, pass with a message
            logging.debug('Ignoring a request for %s on dataset %s' % (delegate_info['command'],
                                                                       delegate_info['dataset']))
            return

        # Check command availability
        if not manager.has_command(delegate_info['command']):
            # Have the data but not the command
            logging.debug('Ignoring a request for %s on dataset %s. The command is not available' %
                          (delegate_info['command'],
                           delegate_info['dataset']))
            return

        # Adding host info to the delegate_info

        delegate_info['peer'] = socket.gethostname()

        # Inform other peers that we take care of the operation
        publish = signal(constants.PUBLISH)
        publish.send(self, topic=constants.DELEGATE_ACCEPTED_TOPIC, id=delegate_info['id'])

        logging.debug('Running the delegated command.')
        func = getattr(manager, delegate_info['command'])
        dataset = delegate_info['dataset']
        del(delegate_info['dataset'])
        result = func(dataset, **delegate_info)
        logging.debug('Delegated command finished with result: %s' % result)


        # # Also inform other parts of the app about accepting
        # accept_signal = signal(constants.DELEGATE_ACCEPTED_SIG)
        # accept_signal.send(self, id=delegate_info['id'])

class DelegateAcceptedTopicHandler(ZMQTopicHandlerBase):
    def __init__(self):
        ZMQTopicHandlerBase.__init__(self)

    def get_topic(self):
        return constants.DELEGATE_ACCEPTED_TOPIC

    def handle(self, manager, info):
        logging.debug('Got a delegate accepted handle request for message %s' % info)

        # Signaling the interested parties about it
        accept_signal = signal(constants.PEER_ACCEPTED_DELEGATE_SIG)
        accept_signal.send(self, info=info)
