from twisted.internet import protocol, reactor
from vertica_wire_handler import VerticaWireHandler
from query_cache import QueryCache
from constants import HOST, TARGET_PORT, _END_PATTERN,_REQUEST_EXT_ORD, _REQUEST_ORD,_END_JDBC_PATTERN, MAX_RESULT_SIZE
import re
query_cache = QueryCache()

_MAX_RESULT_SIZE = MAX_RESULT_SIZE if MAX_RESULT_SIZE < 380000 else 380000

    
class VSQLServerProtocol(protocol.Protocol):
    
    def __init__(self):
        self.buffer = None
        self.client = None
        self.sp_data = [None, None]
        self._user = None

    def connectionMade(self):
        """ When client connection is established, open a connection to remote DB """
        self.client_factory = protocol.ClientFactory()
        self.client_factory.protocol = ClientProtocol
        self.client_factory.server = self

        reactor.connectTCP(HOST, TARGET_PORT, self.client_factory)


#see if we can bottle b'C with subsequent. IF subsquent can hash, proceed
        
    def dataReceived(self, data):
        """ Handle messages from Client to Server"""
        #if data[0] == 0 and not self._user:
        # self.auth(data)
        # if msg.key in query_cache.cache_keys:
        #     cached_data = query_cache.cache_access(msg.key)
        #     if cached_data:
        #         for message in cached_data:
        #             self.transport.write(message) #data in cache
        if self.client:
            if self.vsql_msg and self.vsql_msg.cached_header:
                if data[0] == 68:
                    for msg in self.vsql_msg.cached_header:
                        self.transport.write(msg)
                elif data[0] == 66:
                    for row in self.vsql_msg.cached_data:
                        self.transport.write(row)
                elif data[0]== 67:
                    self.vsql_msg = VerticaWireHandler(_REQUEST_EXT_ORD)
                    self.vsql_msg.set_extended_prep(data) 
                elif data[0] == 66:
                    pass
            elif data[0] == 67 and self.vsql_msg and self.vsql_msg.cachable:
                if len(str(self.vsql_msg.rows).encode()) < _MAX_RESULT_SIZE:
                    print("WRITTEN TO CACHE\n")
                    query_cache.write_to_cache(self.vsql_msg)
                    self.vsql_msg = None
            elif data[0] == _REQUEST_EXT_ORD and self.vsql_msg:
                self.vsql_msg.set_message(data)
                if self.vsql_msg.key in query_cache.cache_keys:
                    self.vsql_msg.acknowledgement, self.vsql_msg.cached_header, self.vsql_msg.cached_data = query_cache.cache_access(self.vsql_msg.key)
                    self.transport.write(self.vsql_msg.acknowledgement)
                else:
                    self.client.write(data)
            elif data[0] == 67:
               self.vsql_msg = VerticaWireHandler(_REQUEST_EXT_ORD)
               self.vsql_msg.set_extended_prep(data) 
            else:
                if data[0] not in [68, 66]:
                    self.vsql_msg = None
                self.client.write(data)
        else:
            self.vsql_msg = None
            self.buffer = data

    def write(self, data):
        """ Handle Messages from Server to Client"""
        if self.vsql_msg:
            if self.vsql_msg.streaming_header: #streaming series of header messages
                self.vsql_msg.append_extended_header_response(data)
                if data[-2:] in [b'\x05T', b'\x05I']:
                    self.vsql_msg.stop_streaming_header()
                    print("STOP STREAMING_HEADER")
            elif self.vsql_msg.streaming_message: #streaming series of body messages
                self.vsql_msg.add_row_response(data)
                if data[-2:] in [b'\x05T', b'\x05I']:
                    self.vsql_msg.stop_streaming_messages()
            elif data[0] == 116 :  #initial header response
                self.vsql_msg.append_extended_header_response(data) #initial body_response
                if data[-2:] not in [b'\x05T', b'\x05I']:
                    self.vsql_msg.stream_header()      
                    print("STREAM HEADER")
            elif data[0] in (51,49): #response to prepared statement
                self.vsql_msg.set_extended_acknowledgement(data)
            elif data[0] in (84, 50): #response to execute
                self.vsql_msg.add_row_response(data)
                if data[-2:] not in [b'\x05T', b'\x05I']:
                    self.vsql_msg.stream_message()             
                    
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
        
        
        