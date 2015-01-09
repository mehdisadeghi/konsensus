import gevent, time

from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver

def greencount():
    s = time.time()
    while 1:
        gevent.sleep(1)
        s0 = time.time()
        print s0-s-1
        s = s0

class TwistedFactory(Factory):
    class protocol(LineReceiver):
        delimiter = '\n'
        def connectionMade(self):
            print '+connection:',id(self)
            self.transport.write(self.factory.quote+'\r\n')
            def later(i):
                self.transport.write('%d from later()\r\n'%i)
            @gevent.Greenlet.spawn
            def ninja():
                gevent.sleep(1)
                self.transport.write('0 from ninja()\r\n')
                gevent.sleep(2)
                self.transport.write('2 from ninja()\r\n')
                reactor.callLater(1,later,3)
            reactor.callLater(2,later,1)
        def connectionLost(self,reason):
            print '-connection:',id(self),reason
        def lineReceived(self,data):
            self.stopcounter()
            data = data.strip()
            ldata = data.lower()
            if ldata == 'die':
                self.sendLine('stopping reactor')
                reactor.stop()
            elif ldata == 'quit':
                self.sendLine('quitting in 2 seconds')
                gevent.sleep(2)
                self.transport.loseConnection()
            elif ldata == 'delay':
                self.sendLine('waiting 10 seconds')
                gevent.sleep(10)
                self.sendLine('hi again')
            elif ldata == 'count':
                self.stopcounter()
                @gevent.Greenlet.spawn
                def count():
                    i = 0
                    while 1:
                        gevent.sleep(1)
                        self.sendLine('%d from count()'%i)
                        i += 1
                self.counter = count
            else:
                self.sendLine(data)
        def stopcounter(self):
            try:
                self.counter.kill()
                del self.counter
            except AttributeError:
                pass
    def __init__(self,quote=None):
        self.quote = quote or 'An apple a day keeps the doctor away'

import geventreactor; geventreactor.install()
from twisted.internet import reactor
gevent.Greenlet.spawn(greencount)
reactor.listenTCP(8007,TwistedFactory('Welcome to the geventreactor demo!\r\ncount:\tstart a counter\r\ndelay:\tblock your session for 10 seconds\r\nquit:\tterminate your session after 2 seconds\r\ndie:\tstop the reactor\r\notherwise, simply echo'))
reactor.run()
