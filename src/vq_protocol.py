from twisted.internet import protocol, reactor
from vertica_wire_handler import VerticaWireHandler
from query_cache import QueryCache
from constants import (HOST, TARGET_PORT, _END_PATTERN,_REQUEST_EXT_ORD, _REQUEST_ORD,_END_JDBC_PATTERN, MAX_RESULT_SIZE,
_RESPONSE_TERMINIATIONS,_EXT_RESPONSE_ORD,_PREP_STMT_ACK_ORDS,_HEADER_RESP_ORD,
_PLAN_REQUEST_ORD,
_EXECUTE_REQUEST_ORD,
_BIND_REQUEST_ORD
)
import re
query_cache = QueryCache()

_MAX_RESULT_SIZE = MAX_RESULT_SIZE if MAX_RESULT_SIZE < 380000 else 380000

    
class VSQLServerProtocol(protocol.Protocol):
    
    def __init__(self):
        self.buffer = None
        self.client = None
        self.sp_data = [None, None]
        self._user = None
        self.test_mode = False

    def connectionMade(self):
        """ When client connection is established, open a connection to remote DB """
        self.client_factory = protocol.ClientFactory()
        self.client_factory.protocol = ClientProtocol
        self.client_factory.server = self

        reactor.connectTCP(HOST, TARGET_PORT, self.client_factory)

    def send_from_cache(self, data):
        """ From cache, respond directly back to client"""
        if data[0] == _BIND_REQUEST_ORD:
            for msg in self.vsql_msg.cached_header:
                self.transport.write(msg) #from client back to client
        elif data[0] == _EXECUTE_REQUEST_ORD:
            for row in self.vsql_msg.cached_data:
                self.transport.write(row) #from client back to client
        elif data[0]== _PLAN_REQUEST_ORD:
            self.vsql_msg = VerticaWireHandler(_REQUEST_EXT_ORD)
            self.vsql_msg.set_extended_prep(data) 
    
    def write_to_cache(self):
         """ Write data to cache and clear msg in memory"""
         if len(str(self.vsql_msg.rows).encode()) < _MAX_RESULT_SIZE:
            query_cache.write_to_cache(self.vsql_msg)
            print(query_cache.nosql_cache_table[self.vsql_msg.key])
            self.vsql_msg = None 
            
    def dataReceived(self, data):
        """ Handle messages from Client to Server"""
        # if data[0] == 112:
        #     print("\nr",b"p\x00\x00\x00")
        # else:
        #     print("\nr", data)
        
        if data[0] == 0: #Retrieve username from handshake
            self.auth(data)

        if self.client:
            if self.vsql_msg and self.vsql_msg.cached_header:
                self.send_from_cache(data)
            elif data[0] == _PLAN_REQUEST_ORD and self.vsql_msg and self.vsql_msg.cachable:
                self.write_to_cache()
            elif data[0] == _REQUEST_EXT_ORD and self.vsql_msg:
                self.vsql_msg.set_message(data)
                if self.vsql_msg.key in query_cache.cache_keys and self.validate_query_permissions:
                    self.vsql_msg.acknowledgement, self.vsql_msg.cached_header, self.vsql_msg.cached_data = query_cache.cache_access(self.vsql_msg.key)
                    self.transport.write(self.vsql_msg.acknowledgement) #from client back to client
                else:
                    self.client.write(data)
            elif data[0] == _PLAN_REQUEST_ORD:
               self.vsql_msg = VerticaWireHandler(_REQUEST_EXT_ORD)
               self.vsql_msg.set_extended_prep(data) 
            else:
                if data[0] not in [_BIND_REQUEST_ORD, _EXECUTE_REQUEST_ORD]:
                    self.vsql_msg = None
                self.client.write(data) #from client to Server
        else:
            self.vsql_msg = None
            self.buffer = data

    def write(self, data):
        """ Handle Messages from Server to Client"""
        print("\nw", data)
        if self.vsql_msg:
            if self.vsql_msg.streaming_header: #streaming series of header messages
                self.vsql_msg.append_extended_header_response(data)
                if data[-2:] in _RESPONSE_TERMINIATIONS:
                    self.vsql_msg.stop_streaming_header()
            elif self.vsql_msg.streaming_message: #streaming series of body messages
                self.vsql_msg.add_row_response(data)
                if data[-2:] in _RESPONSE_TERMINIATIONS:
                    self.vsql_msg.stop_streaming_messages()
            elif data[0] == _HEADER_RESP_ORD :  #initial header response
                self.vsql_msg.append_extended_header_response(data) #initial body_response
                if data[-2:] not in _RESPONSE_TERMINIATIONS:
                    self.vsql_msg.stream_header()      
            elif data[0] in _PREP_STMT_ACK_ORDS: #response to prepared statement
                self.vsql_msg.set_extended_acknowledgement(data)
            elif data[0] in _EXT_RESPONSE_ORD: #response to execute
                self.vsql_msg.add_row_response(data)
                if data[-2:] not in _RESPONSE_TERMINIATIONS:
                    self.vsql_msg.stream_message()             
                    
        self.transport.write(data)
        

    def auth(self, data): #get username. Ideally, this might be a able to be a bit cleaner TODO: Valdiate JDBC VS ODBC
        """ Grab user_name during initial handshake"""
        if not self.test_mode:
            if b"user" in data:
                self._user = re.search("user\x00(.+)\x00",data.decode('latin1')).group(1).split("\x00")[0]
                self.build_grants()
                print("BUILDING GRANTS")
        
    @property
    def validate_query_permissions(self):
        """ Check objects in cache, validate permissions are granted on all objects"""
        if self.test_mode:
            return True
        targets_requested = query_cache.cache_table[self.vsql_msg.key]
        access_granted = True
        for target in targets_requested:
            if target not in self._read_grants:
                access_granted = False
                break
        return access_granted
                
    def build_grants(self):
        """ Get a list of object-grants on session-user"""
        roles = list(self.priv_mgr.user_role_df.loc[self.priv_mgr.user_role_df.user_name == self._user]["role"].values)
        grants = list(self.priv_mgr.grant_user_df.loc[self.priv_mgr.grant_user_df.grantee == self._user].obj.values)
        for role in roles:
            grants += list(self.priv_mgr.grant_role_df.loc[self.priv_mgr.grant_role_df.grantee == role].obj.values)
        self._read_grants = grants
        print(self._read_grants[:5])
                
        
class ClientProtocol(protocol.Protocol):
    def connectionMade(self):
        self.factory.server.client = self
        self.write(self.factory.server.buffer)
        self.factory.server.buffer = ''
        
    def dataReceived(self, data): 
        self.factory.server.write(data)
            
    def write(self, data):
        self.transport.write(data)
        
        
        