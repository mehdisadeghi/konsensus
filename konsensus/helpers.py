"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import logging
import socket

import zmq.green as zmq
import blinker
import numpy

from . import constants

logger = logging.getLogger(__name__)


def publish(sender, topic, **kwargs):
    """
    Send a publish request
    :param sender:
    :param topic:
    :param kwargs:
    :return:
    """
    kwargs['topic'] = topic
    publish_signal = blinker.signal(constants.PUBLISH)
    publish_signal.send(sender, **kwargs)


def is_running_instance(config, ip, port):
    """
    Check if the given ip and port point to the currently running instance
    :param config:
    :param ip:
    :param port:
    :return:
    """
    local_ip = socket.gethostbyname(socket.gethostname())
    return ip in (local_ip, '127.0.0.1', 'localhost') and port == config.API_PORT


def send_array(socket, A, flags=0, copy=True, track=False):
    """send a numpy array with metadata"""
    md = dict(
        dtype=str(A.dtype),
        shape=A.shape,
    )
    socket.send_json(md, flags | zmq.SNDMORE)
    return socket.send(A, flags, copy=copy, track=track)


def recv_array(socket, flags=0, copy=True, track=False):
    """recv a numpy array"""
    md = socket.recv_json(flags=flags)
    msg = socket.recv(flags=flags, copy=copy, track=track)
    buf = buffer(msg)
    a = numpy.frombuffer(buf, dtype=md['dtype'])
    return a.reshape(md['shape'])


def whoami(config):
    logger.debug('I am konsensus instance at %s:%s' % (config.HOST, config.API_PORT))


def get_operation_store():
    """
    A shortcut method to return operation store
    :return:
    """
    from .application import app
    return app.manager.get_operation_store()


def is_result_collector(operation_id):
    """
    Check if we are the result collector for the given operation
    :return:
    """
    from .application import app
    # Check if this operation is sub-op
    mother = get_mother_operation(operation_id)
    if mother:
        # Check if the assigned collector id is same as our id
        return mother.get('collector_id') == app.get_id()
    else:
        return False


def get_mother_operation(operation_id):
    """
    Check if the given operation_id is a sub-operation and return its mother with key included
    :param operation_id:
    :return:
    """
    store = get_operation_store()
    operation = store.get(operation_id)
    mother_id = operation.get('mother_operation_id')
    if mother_id:
        mother = store.get(mother_id)
        mother['operation_id'] = mother_id
        return mother


def is_sub_operation(operation_id):
    """
    Check if the given operation_id is a sub-operation
    :param operation_id:
    :return:
    """
    return get_mother_operation() is not None