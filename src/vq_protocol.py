from twisted.internet import protocol, reactor
from vertica_wire_handler import VerticaWireHandler
from query_cache import QueryCache
from constants import HOST, TARGET_PORT, _END_PATTERN, _REQUEST_ORD,_END_JDBC_PATTERN, MAX_RESULT_SIZE

query_cache = QueryCache()

_MAX_RESULT_SIZE = MAX_RESULT_SIZE if MAX_RESULT_SIZE < 380000 else 380000

class ServerProtocol(protocol.Protocol):
    
    def __init__(self):
        self.buffer = None
        self.client = None
        self.sp_data = [None, None]

    def connectionMade(self):
        self.client_factory = protocol.ClientFactory()
        self.client_factory.protocol = ClientProtocol
        self.client_factory.server = self

        reactor.connectTCP(HOST, TARGET_PORT, self.client_factory)

    def dataReceived(self, data):
        if (self.client != None):
            if data[0] == _REQUEST_ORD:
                msg = VerticaWireHandler(data)
                self.sp_data = [msg, []]
                if msg.key in query_cache.cache_keys:
                    cached_data = query_cache.cache_access(msg.key)
                    if cached_data:
                        for message in cached_data:
                            self.transport.write(message) #data in cache
                    else:
                        self.client.write(data) #cached data was purged continue as normal
                else:
                    self.client.write(data) #data not in cache
            else:
                self.sp_data = [None, None]
                self.client.write(data)
        else:
            self.sp_data = [None, None]
            self.buffer = data

    def write(self, data):
        msg = self.sp_data[0]
        if msg:
            self.sp_data[1].append(data)
            if data[-2:] in (_END_PATTERN, _END_JDBC_PATTERN):  
                if len(str(self.sp_data[1]).encode()) < _MAX_RESULT_SIZE:
                    query_cache.write_to_cache(msg, self.sp_data[1])
                
        self.transport.write(data)
        
                
class ClientProtocol(protocol.Protocol):
    
    def connectionMade(self):
        self.factory.server.client = self
        self.write(self.factory.server.buffer)
        self.factory.server.buffer = ''
        
    def dataReceived(self, data):     
        self.factory.server.write(data)
            
    def write(self, data):
        self.transport.write(data)
  
