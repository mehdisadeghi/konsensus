from twisted.internet import reactor
from twisted.python import log
from kademlia.network import Server
import sys

key = "sample"

log.startLogging(sys.stdout)

value = None

def done(result):
    print "Key result:", result
    global value
    value = result
    reactor.stop()

def setDone(result, server):
    server.get(key).addCallback(done)

def bootstrapDone(found, server):
    server.set(key, "Hi from Kamedlia!").addCallback(setDone,
                                                     server)

server = Server()
server.listen(8469)
server.bootstrap([("127.0.0.1", 8468)]).addCallback(bootstrapDone,
                                                    server)

reactor.run()
