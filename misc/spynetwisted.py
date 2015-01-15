import logging
logging.basicConfig(level=logging.DEBUG)

from spyne.application import Application
from spyne.decorator import srpc
from spyne.service import ServiceBase
from spyne.model.primitive import Integer
from spyne.model.primitive import Unicode

from spyne.model.complex import Iterable

from spyne.protocol.msgpack import MessagePackRpc

from spyne.server.twisted import TwistedWebResource

from twisted.internet import reactor
from twisted.web.server import Site

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
    resource = TwistedWebResource(application)
    site = Site(resource)

    reactor.listenTCP(8000, site, interface='0.0.0.0')
    reactor.run()
