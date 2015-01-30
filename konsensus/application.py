"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""
import logging
import gevent

import zmq.green as zmq
import zerorpc
# Monkey patching msgpack to support numpy arrays
import msgpack
import msgpack_numpy as mn
mn.patch()
from blinker import signal

from . import constants
from .api import KonsensusAPI
from .settings import DefaultSettings
from .manager import KonsensusManager

# An application level variable to access internals such as manager
app = None


class KonsensusApp(object):
    def __init__(self, name, host="0.0.0.0", port=None, config=None):
        if not config:
            config = DefaultSettings()
        self.config = config
        config['HOST'] = host
        self.name = name
        self.host = host
        self.port = port or config.API_PORT
        self._topic_handlers = {}
        self.manager = KonsensusManager(self.config)
        self.logger = logging.getLogger(__name__)
        global app
        app = self

    def register_handler(self, topic, handler):
        """
        Register a function to handle a zmq topic message
        :param topic:
        :param handler:
        :return:
        """
        if topic in self._topic_handlers:
            self._topic_handlers.append(handler)
        else:
            self._topic_handlers[topic] = [handler]

    def _register_handlers(self):
        """
        Dynamically add all topic handlers
        :param manager:
        :return:
        """
        import handlers as th
        import inspect
        for name, class_type in inspect.getmembers(th, predicate=inspect.isclass):
            if class_type is th.ZMQTopicHandlerBase:
                continue
            handler = class_type()
            topic = handler.get_topic()
            if topic in self._topic_handlers:
                self._topic_handlers.append(handler)
            else:
                self._topic_handlers[topic] = [handler]

    def _handle_topic(self, topic, messages):
        """
        Lookup topic handler and call them
        :param topic:
        :param messages:
        :return:
        """
        if topic not in self._topic_handlers:
            return self._default_handler(topic, messages)
        else:
            for handler in self._topic_handlers[topic]:
                handler.handle(self.manager, messages)

    def _default_handler(self, topic, messages):
        """
        Default handler for any message type
        :param topic:
        :param messages:
        :return:
        """
        self.logger.error('No handler assigned for topic %s' % topic)

    def run(self):
        gevent.joinall([gevent.spawn(self._run_api_listener),
                        gevent.spawn(self._run_publisher),
                        gevent.spawn(self._subscribe_to_peers)])

    def _run_api_listener(self):
        conn_string = "tcp://{host}:{port}".format(host=self.host,
                                                   port=self.port)
        self.logger.info("Starting the server on %s" % conn_string)
        self._register_handlers()
        self.api = KonsensusAPI(self.manager)
        s = zerorpc.Server(self.api)
        s.bind(conn_string)
        s.run()

    def get_api_endpoint(self):
        """
        Returns the api endpoint
        :return:
        """
        return "tcp://%s:%s" % (self.host, self.port)

    def get_id(self):
        """
        Returns applications unique id to be identified in the network
        :return:
        """
        return self.get_api_endpoint()

    def _run_publisher(self):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        self.logger.info('Running publisher at tcp://*:%s' % self.config.PUB_PORT)
        socket.bind("tcp://%s:%s" % (self.host, self.config.PUB_PORT))

        def publish_handler(sender, topic=None, **kwargs):
            if not topic:
                raise Exception("No topic given. Won't publish anything without it.")
            self.logger.debug('Got a publish request for topic %s:%s' %
                              (topic, constants.topics.get(topic)))

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

        for ip, pub_port, api_port in self.config.PEERS:
            if not self._is_self(ip, pub_port):
                address = '%s:%s' % (ip, pub_port)
                self.logger.debug('Subscribing to peer at: %s' % address)
                socket.connect('tcp://%s' % address)

        def new_msg_handler(sender, msg=None):
            topic, delimiter, packed = msg.partition(' ')
            topic = int(topic)
            message_dict = msgpack.unpackb(packed)
            self.logger.debug('News for topic %s:%s arrived' %
                              (topic, constants.topics.get(topic)))
            self._handle_topic(topic, message_dict)

        sig = signal(constants.NEW_MESSAGE_TOPIC)
        sig.connect(new_msg_handler, weak=False)

        while True:
            msg = socket.recv()
            sig.send(self, msg=msg)
            gevent.sleep(.1)