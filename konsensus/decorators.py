"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import uuid
import functools
import datetime

import gevent
import blinker

import helpers
import constants


def delegate(func):
    @functools.wraps(func)
    def new_func(self, dataset, result_ds_name, *args, **kwargs):
        # If the function has already been delegated don't circulate it again.
        if 'is_delegate' in kwargs:
            self.logger.debug('Ignoring delegation try for already delegated request')
            return func(self, dataset, result_ds_name, *args, **kwargs)

        if dataset not in self.local_datasets:
            self.logger.debug('Dataset %s is not available locally, trying to delegate.' % dataset)

            # Make an id
            operation_id = str(uuid.uuid4())

            helpers.publish(self,
                            constants.DELEGATE_TOPIC,
                            command=func.__name__,
                            dataset=dataset,
                            result_ds_name=result_ds_name,
                            delegate_id=operation_id)
            self.logger.debug('Request for dataset %s published, waiting for someone to answer' % dataset)

            global reply
            reply = None

            def accept_handler(sender, info=None):
                self.logger.debug('Delegate request for dataset %s accepted' % dataset)
                global reply
                reply = info

            accept_signal = blinker.signal(constants.PEER_ACCEPTED_DELEGATE_SIG)
            accept_signal.connect(accept_handler)

            gevent.sleep(3)

            def proxy_func(*args, **kwargs):
                global reply
                if reply:
                    return 'You request with id %s is delivered to peer %s' % (reply['delegate_id'], reply['peer'])
            return proxy_func()
            # return proxy or call remote and return the result

        else:
            return func(self, dataset, result_ds_name, *args, **kwargs)
    return new_func


def register(func):
    """
    Makes a new operation id and stores it in distributed operation store.
    :param func:
    :return:
    """
    # Get application instance to access manager

    @functools.wraps(func)
    def new_func(self, *args, **kwargs):
        from application import app
        store = app.manager.get_operation_store()
        operation_id = store.store(*args,
                                   func_name=func.__name__,

                                   **kwargs)
        result = func(self, *args, **kwargs)
        store.update(operation_id, state='running')
        return result

    return new_func