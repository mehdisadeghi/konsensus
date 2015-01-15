import logging
logging.basicConfig(level=logging.DEBUG)

from spyne.application import Application
from spyne.decorator import srpc
from spyne.service import ServiceBase
from spyne.model.primitive import Integer
from spyne.model.primitive import Unicode

from spyne.model.complex import Iterable

from spyne.protocol.msgpack import MessagePackRpc

from spyne.server.zeromq import ZeroMQServer

class HelloWorldService(ServiceBase):
    @srpc(Unicode, Integer, _returns=Iterable(Unicode))
    def say_hello(name, times):
        for i in range(times):
            yield 'Hello, %s' % name

application = Application([HelloWorldService],
    tns='spyne.examples.hello',
    in_protocol=MessagePackRpc(validator='soft'),
    out_protocol=MessagePackRpc()
)

if __name__ == '__main__':
    wsgi_app = ZeroMQServer(application,
                           'tcp://0.0.0.0:5555')
    wsgi_app.serve_forever()
