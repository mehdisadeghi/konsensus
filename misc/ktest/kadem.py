from twisted.internet import reactor
from twisted.python import log
from kademlia.network import Server
import sys

# log to std out
log.startLogging(sys.stdout)

def quit(result):
    print "Key result:", result
    reactor.stop()

def get(result, server):
    return server.get("a key").addCallback(quit)

def done(found, server):
    log.msg("Found nodes: %s" % found)
    return server.set("a key", "a value").addCallback(get, server)

server = Server()
# next line, or use reactor.listenUDP(5678, server.protocol)
server.listen(5678)
server.bootstrap([('127.0.0.1', 8468)]).addCallback(done, server)

reactor.run()
