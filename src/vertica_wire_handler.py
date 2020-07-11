from hashlib import md5
from constants import _REQUEST_EXT_ORD, _REQUEST_ORD
import re 

class VerticaWireHandler():
    
    """To build out as an actual handler when more is added--for now, string conversion. For now, pretty gratuituous """
    
    def __init__(self, proto_ord):
        self.proto_ord = proto_ord
        self.rows = []
        self.header_response = None
        self.cached_header = None
        self.acknowledgement = None
        self.streaming_header = False  
        self.streaming_message = False
    def set_message(self, data):
        self.data = data
    @property
    def message(self):
        return self.data[5:-1].decode().upper()
    
    @property
    def message_length_bytes(self):
        return self.data[1:5]
        
    @property
    def key(self):
        if self.proto_ord == _REQUEST_ORD:
            return md5(self.data[5:-1]).hexdigest()
        elif self.proto_ord == _REQUEST_EXT_ORD:
            return md5(re.findall("\x00(.+)\x00", self.data[4:-6].decode('latin1'))[0].encode('latin1')).hexdigest()

    @property
    def message_type(self):
        return self.data[0]
    
    @property 
    def cachable(self):
        if self.header_response and self.acknowledgement:
            return True
        else:
            return False
        
    def append_extended_header_response(self,s):
        if self.header_response:
            self.header_response.append(s)
        else:
            self.header_response = [s]

    def set_extended_acknowledgement(self,s):
        self.acknowledgement = s
        
    def set_extended_prep(self,s):
        self.prep = s
        
    def add_row_response(self, s):
        self.rows.append(s)

    def stop_streaming_header(self):
        self.streaming_header = False
        
    def stream_header(self):
        self.streaming_header = True
        
    def stream_message(self):
        self.streaming_message = True
        
    def stop_streaming_message(self):
        self.streaming_message = False


"""  if self.vsql_msg and self.vsql_msg.cached_header:
                if data[0] == 68:
                    print(self.vsql_msg.cached_header)
                    self.transport.write(self.vsql_msg.cached_header)
                    print("HEADER RESPONSE")
                elif data[0] == 66:
                    print("SENDING DATA")
                    for row in self.vsql_msg.cached_data:
                        self.transport.write(row)
                elif data[0]== 67:
                    self.vsql_msg.set_extended_prep(data)"""