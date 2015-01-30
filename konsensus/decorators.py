"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import uuid
import functools

from . import helpers
from . import constants


def delegate(func):
    @functools.wraps(func)
    def new_func(self, dataset_id, *args, **kwargs):
        self.logger.debug('Entered delegate decorator function')
        # Extract operation_id
        operation_id = kwargs.get('operation_id', None)
        # Get store
        from application import app
        store = app.manager.get_operation_store()
        # Store if this is new operation, otherwise take no action
        if not operation_id:
            self.logger.debug('Creating new operation id in delegate decorator')
            operation_id = store.store(dataset_id,
                                       *args,
                                       command=func.__name__,
                                       **kwargs)

        # Ask others to continue if we don't have the required data
        if dataset_id in self.local_datasets:
            self.logger.debug('We have dataset locally in delegate decorator')
            # TODO: Remove this `is_delegate` stuff all together. Separate service from internals.
            # prevent `is_delegate` from propagating into store
            if 'is_delegate' in kwargs:
                kwargs.pop('is_delegate')
            # We have the data so we do the business
            # Furthermore we don't care about the result, its async, remember?
            try:
                self.logger.debug('Running real function inside delegate decorator')
                func(self, dataset_id, *args, **kwargs)
                state = constants.OperationState.done.value
                self.logger.debug('Operation %s entering %s state' % (operation_id, state))
                store.update(operation_id=operation_id,
                             state=state)
                # Not required, store will tell everybody
                # # Now inform others about operation state of finished
                # helpers.publish(self,
                #                 constants.OPERATION_NEWS_TOPIC,
                #                 operation_id=operation_id,
                #                 state=constants.OperationState.done.value)
            # TODO: Define proper exceptions
            except Exception, e:
                state = constants.OperationState.failed.value
                self.logger.debug('Operation %s entering %s state' % (operation_id, state))
                store.update(operation_id=operation_id,
                             state=state)
                # Not required, store will tell everybody
                # # Tell others it failed
                # helpers.publish(self,
                #                 constants.OPERATION_NEWS_TOPIC,
                #                 operation_id=operation_id,
                #                 state=constants.OperationState.failed.value)
                # Don't pass silently
                raise

            return operation_id
        # Delegate in case that this is not a delegate itself
        elif 'is_delegate' not in kwargs:
            self.logger.debug('Dataset %s is not available locally, trying to delegate.'
                              % dataset_id)
            # Inform others that there is an operation for some dataset
            # Whoever who has the data will take further actions
            helpers.publish(self,
                            constants.DELEGATE_TOPIC,
                            command=func.__name__,
                            dataset_id=dataset_id,
                            operation_id=operation_id)
            self.logger.debug('Request for dataset %s published' % dataset_id)

            # Return the operation id not to break the wrapped function return value
            return operation_id
        else:
            self.logger.debug('Passing silently in delegate decorator because this is a delegate or we do not have data')

    return new_func


# def register(func):
#     """
#     Makes a new operation id and stores it in distributed operation store.
#     :param func:
#     :return:
#     """
#     # Get application instance to access manager
#
#     @functools.wraps(func)
#     def new_func(self, *args, **kwargs):
#         from application import app
#         store = app.manager.get_operation_store()
#         operation_id = store.store(*args,
#                                    func_name=func.__name__,
#                                    **kwargs)
#         kwargs.update(operation_id=operation_id)
#         func(self, *args, **kwargs)
#         return operation_id
#
#     return new_func


def distribute_linear(target_function):
    """
    Breaks a linear operation into sub-operations and launches them in a collective manner.
    :param target_func: the function to be launched in sub-operations over each dataset
    :return:
    """
    def distribute_linear_decorator(func):
        @functools.wraps(func)
        def new_func(self, *args, **kwargs):
            dataset_ids = args
            # Store current operation
            from application import app
            store = app.manager.get_operation_store()
            mother_operation_id = store.store(dataset_ids,
                                              command=func.__name__,
                                              **kwargs)
            sub_operations = []

            # Randomly select who should be collector
            # We might also be the collector in case we have one of these datasets
            import random
            choice = random.choice(dataset_ids)

            # For each dataset launch a separate operation and keep it's operation_id
            for dataset_id in dataset_ids:
                operation_id = target_function(app.manager,
                                               dataset_id,
                                               is_collector=(choice == dataset_id),
                                               mother_operation_id=mother_operation_id)
                sub_operations.append(operation_id)
            store.update(mother_operation_id, sub_operations=sub_operations)
            return mother_operation_id
        return new_func

    return distribute_linear_decorator