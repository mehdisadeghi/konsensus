"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import logging
import zmq.green as zmq
import zerorpc
import gevent
import msgpack

from blinker import signal

import constants
from api import KonsensusAPI
from defaults import DefaultConfig
from manager import KonsensusManager


class KonsensusApp(object):
    def __init__(self, name, host="0.0.0.0", port=None, config=DefaultConfig()):
        self.config = config
        self.name = name
        self.host = host
        self.port = port or config.API_PORT
        self.manager = None
        self.logger = self._get_logger()
        logging.basicConfig(level=logging.DEBUG)

    def _get_logger(self):
        logger = logging.getLogger('%s.%s' % (__name__, self.config.PEER_ID))
        if logger.handlers:
            return logger
        logger.propagate = False
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('Peer %s:%s' % (self.config.PEER_ID, logging.BASIC_FORMAT))
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def _register_handlers(self, manager):
        """
        Dynamically add all topic handlers
        :param manager:
        :return:
        """
        import topic_handlers as th
        import inspect
        for name, class_type in inspect.getmembers(th, predicate=inspect.isclass):
            if class_type is th.ZMQTopicHandlerBase:
                continue
            instance = class_type()
            manager.register_handler(instance.get_topic(), instance)

    def run(self):
        gevent.joinall([gevent.spawn(self._run_api_listener),
                        gevent.spawn(self._run_publisher),
                        gevent.spawn(self._subscribe_to_peers)])

    def _return_loop_methods(self):
        return [self._run_api_listener,
                self._run_publisher,
                self._subscribe_to_peers]

    def _run_api_listener(self):
        conn_string = "tcp://{host}:{port}".format(host=self.host,
                                                   port=self.port)
        self.logger.info("Starting the server on %s" % conn_string)
        self.manager = KonsensusManager(self.config)
        self._register_handlers(self.manager)
        self.api = KonsensusAPI(self.manager)
        s = zerorpc.Server(self.api)
        s.bind(conn_string)
        #logging.info("Listening on {conn_string}".format(conn_string=conn_string))
        s.run()

    def _run_publisher(self):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        self.logger.info('Running publisher at tcp://*:%s' % self.config.PUB_PORT)
        socket.bind("tcp://%s:%s" % (self.host, self.config.PUB_PORT))

        def publish_handler(sender, topic=None, **kwargs):
            if not topic:
                raise Exception("No topic given. Won't publish anything without it.")
            self.logger.debug('Got a publish request with topic %s and keywords: %s' % (topic, kwargs))

            packed = msgpack.packb(kwargs)
            socket.send('%s %s' % (topic, packed))

        publish = signal(constants.PUBLISH)
        publish.connect(publish_handler, weak=False)

    def _is_self(self, ip, port):
        """Check if ip and port points to myself"""
        import socket as sk
        self_ip = sk.gethostbyname(sk.gethostname())
        self_port = self.config.API_PORT
        return str(self_ip) == ip and self_port == port

    def _subscribe_to_peers(self):
        """
        Run the required listeners
        :return:
        """
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.setsockopt(zmq.SUBSCRIBE, '')

        for ip, port in self.config.PEERS:
            if not self._is_self(ip, port):
                address = '%s:%s' % (ip, port)
                self.logger.debug('Subscribing to peer at: %s' % address)
                socket.connect('tcp://%s' % address)

        def new_msg_handler(sender, msg=None):
            #logging.debug('Received message: %s' % msg)
            topic, delimiter, packed = msg.partition(' ')
            topic = int(topic)
            message_dict = msgpack.unpackb(packed)
            self.logger.debug('News for topic %s, msg: %s' % (topic, message_dict))
            self.manager.handle_topic(topic, message_dict)

        sig = signal(constants.NEW_MESSAGE_TOPIC)
        sig.connect(new_msg_handler, weak=False)
        #logging.debug('signal receivers: %s' % sig.receivers)

        while True:
            msg = socket.recv()
            sig.send(self, msg=msg)
            gevent.sleep(.1)