"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import logging
import zmq.green as zmq
import zerorpc
import gevent

from blinker import signal

from api import KonsensusAPI
from defaults import DefaultConfig
from constants import NEW_OPERATION_TOPIC, HEARTBEAT_TOPIC
from manager import KonsensusManager


class KonsensusApp(object):
    def __init__(self, name, host="0.0.0.0", port=DefaultConfig.API_PORT):
        self.name = name
        self.host = host
        self.port = port
        self.api = None
        logging.basicConfig(level=logging.DEBUG)

    def run(self):
        gevent.joinall([gevent.spawn(self._run_api_listener),
                        gevent.spawn(self._run_publisher),
                        gevent.spawn(self._subscribe_to_peers)])

    def _run_api_listener(self):
        logging.info("Starting server")
        self.api = KonsensusAPI(KonsensusManager())
        s = zerorpc.Server(self.api)
        conn_string = "tcp://{host}:{port}".format(host=self.host,
                                                   port=self.port)
        s.bind(conn_string)
        logging.info("Listening on {conn_string}".format(conn_string=conn_string))
        s.run()

    def _run_publisher(self):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://*:%s" % DefaultConfig.PUB_PORT)

        def new_op_handler(sender, op_name=None):
            logging.debug("Got a signal for operation %s from %s" % (op_name, sender))
            socket.send("%s %s" % (NEW_OPERATION_TOPIC, op_name))

        logging.debug("Connecting to operation.new signal")
        sig = signal('operation.new')
        sig.connect(new_op_handler, weak=False)

        # Move to discovery and heartbeat monitoring module
        #while True:
        #    socket.send('%d %s' % (HEARTBEAT_TOPIC, 'hb'))
        #    gevent.sleep(1)

    def _subscribe_to_peers(self):
        """
        Run the required listeners
        :return:
        """
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.setsockopt(zmq.SUBSCRIBE, '')

        import socket as sk
        self_ip = sk.gethostbyname(sk.gethostname())
        
        for ip in DefaultConfig.PEERS:
            if ip != str(self_ip):
                address = '%s:%s' % (ip, DefaultConfig.PUB_PORT)
                logging.debug('Subscribing to peer at: %s' % address)
                socket.connect('tcp://%s' % address)

        def new_msg_handler(sender, msg=None):
            topic, message, = msg.split()
            logging.debug('News for topic %s, msg: %s' % (topic, message))
            self.api.handle_topic(topic, message)

        sig = signal('message.new')
        sig.connect(new_msg_handler, weak=False)
        logging.debug('signal receivers: %s' % sig.receivers)

        while True:
            msg = socket.recv()
            sig.send(self, msg=msg)
            gevent.sleep(.1)