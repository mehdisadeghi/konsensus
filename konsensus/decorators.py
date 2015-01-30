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
        # If the function has already been delegated don't circulate it again.
        # if 'is_delegate' in kwargs:
        #     self.logger.debug('Ignoring delegation try for already delegated request')
        #     # We don't need this flag anymore, otherwise it will be stored in the store.
        #     is_delegate = kwargs.pop('is_delegate')
        #     return func(self, dataset_id, *args, **kwargs)

        # Store the operation if its new, otherwise take no action
        if 'operation_id' not in kwargs:
            from application import app
            store = app.manager.get_operation_store()
            kwargs.update(operation_id=store.store(dataset_id,
                                                   *args,
                                                   func_name=func.__name__,
                                                   **kwargs))

        # Ask others to continue if we don't have the required data
        if dataset_id in self.local_datasets:
            # We have the data so we do the business
            return func(self, dataset_id, *args, **kwargs)
        # Delegate in case that this is not a delegate itself
        elif 'is_delegate' not in kwargs:
            self.logger.debug('Dataset %s is not available locally, trying to delegate.'
                              % dataset_id)

            # Extract operation_id and attach it to the publish message
            operation_id = kwargs.pop('operation_id')

            helpers.publish(self,
                            constants.DELEGATE_TOPIC,
                            command=func.__name__,
                            dataset_id=dataset_id,
                            operation_id=operation_id)
            self.logger.debug('Request for dataset %s published' % dataset_id)

            # Return the operation id not to break the wrapped function return value
            return operation_id
        elif 'is_delegate' in kwargs:
            self.logger.debug('Ignoring delegation try for already delegated request')
            # We don't need this flag anymore, otherwise it will be stored in the store.
            kwargs.pop('is_delegate')

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


# class OneTimeTrueDice(object):
#     """
#     This is a dice which will become True ONLY once
#     """
#     def __init__(self):
#         self._done = False
#
#     def throw(self):
#         import random
#         if not self._done:
#             choice = random.choice([True, False])
#             if choice:
#                 self._done = True
#             return choice
#         return False


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
                                              func_name=func.__name__,
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