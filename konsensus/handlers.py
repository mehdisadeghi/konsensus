"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
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
        # self.logger.debug('Got a delegate handle request, checking data availability')

        # Check data availability
        if not manager.has_dataset(delegate_info['dataset_id']):
            # Don't need to do anything, pass with a message
            # self.logger.debug('Ignoring %s, no dataset %s' % (delegate_info['command'],
            #                                                   delegate_info['dataset_id']))
            return

        # Adding host info to the delegate_info
        from .application import app
        delegate_info['peer'] = app.get_api_endpoint()  # socket.gethostname()

        # Inform other peers that we take care of the operation
        helpers.publish(self,
                        constants.DELEGATE_ACCEPTED_TOPIC,
                        operation_id=delegate_info['operation_id'],
                        peer=delegate_info['peer'])

        # self.logger.debug('Running the delegated command.')
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
        # self.logger.debug('A delegate for operation %s accepted by %s' %
        #                   (info['operation_id'], info['peer']))

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
            # self.logger.debug('Ignoring a pull request which is not for us')
            return
        self.logger.debug('Going to pull dataset %s' % info['dataset_id'])
        # ctx = zmq.Context()
        # pull_socket = ctx.socket(zmq.PULL)
        # pull_socket.connect(info['endpoint'])
        # array = helpers.recv_array(pull_socket)
        import zerorpc
        c = zerorpc.Client()
        c.connect(info['api_endpoint'])
        array = c.get_temp_dataset(info['dataset_id'])
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
        # self.logger.debug('Got operation news, going to update store %s' % operation_info)
        store = manager.get_operation_store()
        operation_id = operation_info.pop('operation_id')
        # Update the store but don't publish anything since we are not initiator
        store.update(operation_id, publish=False, **operation_info)

        # TODO: Move out the following code into some proper class

        # First of all wait to make sure related following setup messages are arrived
        #import gevent
        #gevent.sleep(1)

        # Check if we are the collector
        if helpers.is_result_collector(operation_id):
            self.logger.debug('I am collector, checking for sub-operations')
            # Check if all the sub-ops are done
            mother_op = helpers.get_mother_operation(operation_id)
            if not mother_op:
                self.logger.warning('Why we set mother to ourselves?')
                mother_op = store.get(operation_id)
            # print '*~#*~#*~#*~#*~#'
            # print mother_op
            # print '*~#*~#*~#*~#*~#'
            # Collect result dataset ids
            result_dataset_ids = []
            if len(mother_op['sub_operations']) != mother_op['sub_op_count']:
                self.logger.debug('Some sub-operations are not done yet, we take no action.')
                return
            for sub_op_id in mother_op['sub_operations']:
                sub_op = store.get(sub_op_id)
                if sub_op['state'] != constants.OperationState.done.value:
                    # Some operations are not done, leave it.
                    self.logger.debug('Some sub-operations are not done yet, we take no action.')
                    return
                if 'result_dataset_id' not in sub_op:
                    raise Exception('Sub-operation %s is labled as done but it has no result dataset.' % sub_op_id)
                result_dataset_ids.append(sub_op['result_dataset_id'])

            # Sub-operations are done, so we run the command over dataset_ids
            # Get command endpoint of the mother operation
            func = getattr(manager, mother_op['command'])

            self.logger.debug('Calling %s command with dataset ids' % mother_op['command'])
            # Call command with an extra flag to bypass decorators
            # The result will be stored by the function itself
            # print '*~#*~#*~#*~#*~#YOU SHOULD SEE THIS ONCE*~#*~#*~#*~#*~#*~#'
            func(result_dataset_ids, bypass=True, operation_id=mother_op['operation_id'])

            # Update the network about new state
            store.update(operation_id=mother_op['operation_id'],
                         state=constants.OperationState.done.value)