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