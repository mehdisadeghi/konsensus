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
        logging.basicConfig(level=logging.DEBUG)

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

    def _run_api_listener(self):
        logging.info("Starting server")
        self.manager = KonsensusManager(self.config)
        self._register_handlers(self.manager)
        self.api = KonsensusAPI(self.manager)
        s = zerorpc.Server(self.api)
        conn_string = "tcp://{host}:{port}".format(host=self.host,
                                                   port=self.port)
        s.bind(conn_string)
        logging.info("Listening on {conn_string}".format(conn_string=conn_string))
        s.run()

    def _run_publisher(self):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://*:%s" % self.config.PUB_PORT)

        # def new_op_handler(sender, op_name=None):
        #     logging.debug("Got a signal for operation %s from %s" % (op_name, sender))
        #     socket.send("%s %s" % (constants.NEW_OPERATION_TOPIC, op_name))
        #
        # logging.debug("Connecting to operation.new signal")
        # sig = signal(constants.NEW_OPERATION_SIG)
        # sig.connect(new_op_handler, weak=False)

        # def delegate_handler(sender, command=None, dataset=None):
        #     logging.debug('Got a delegate request for command %s on dataset %s' % (command, dataset))
        #     delegate_info = {'command': command,
        #                      'dataset': dataset}
        #     socket.send('%s %s' % (constants.DELEGATE_TOPIC, delegate_info))
        #
        # delegate = signal(constants.DELEGATE_SIG)
        # delegate.connect(delegate_handler, weak=False)

        def publish_handler(sender, topic=None, **kwargs):
            if not topic:
                raise Exception("No topic given. Won't publish anything without it.")
            logging.debug('Got a publish request with topic %s and keywords: %s' % (topic, kwargs))

            packed = msgpack.packb(kwargs)
            socket.send('%s %s' % (topic, packed))

        publish = signal(constants.PUBLISH)
        publish.connect(publish_handler, weak=False)

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
        
        for ip in self.config.PEERS:
            if ip != str(self_ip):
                address = '%s:%s' % (ip, self.config.PUB_PORT)
                logging.debug('Subscribing to peer at: %s' % address)
                socket.connect('tcp://%s' % address)

        def new_msg_handler(sender, msg=None):
            logging.debug('Received message: %s' % msg)
            topic, delimiter, packed = msg.partition(' ')
            topic = int(topic)
            message_dict = msgpack.unpackb(packed)
            logging.debug('News for topic %s, msg: %s' % (topic, message_dict))
            self.manager.handle_topic(topic, message_dict)

        sig = signal(constants.NEW_MESSAGE_TOPIC)
        sig.connect(new_msg_handler, weak=False)
        logging.debug('signal receivers: %s' % sig.receivers)

        while True:
            msg = socket.recv()
            sig.send(self, msg=msg)
            gevent.sleep(.1)