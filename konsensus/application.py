"""
    consensus
    ~~~~~~~~~

    This file is part of kansensus project.
"""
import logging
import zerorpc
import gevent
import zmq

from .api import KonsensusAPI
from .defaults import DefaultConfig


class KonsensusApp(object):
    def __init__(self, name, host="0.0.0.0", port=DefaultConfig.API_PORT):
        self.name = name
        self.host = host
        self.port = port
        logging.basicConfig(level=logging.DEBUG)

    def run(self):
        gevent.joinall([gevent.spawn(self._run_api_listener),
                        gevent.spawn(self._run_heartbeat_listener),
                        gevent.spawn(self._run_heartbeat_broadcast)])

    def _run_api_listener(self):
        logging.info("Starting server")
        s = zerorpc.Server(KonsensusAPI())
        conn_string = "tcp://{host}:{port}".format(host=self.host,
                                                   port=self.port)
        s.bind(conn_string)
        logging.info("Listening on {conn_string}".format(conn_string=conn_string))
        s.run()

    def _run_heartbeat_listener(self):
        pass

    def _run_heartbeat_broadcast(self):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://*:%s" % DefaultConfig.HEARTBEAT_PORT)

        while True:
            topic =
