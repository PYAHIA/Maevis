import sys

for path in ['', '/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python37.zip', '/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7', '/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/lib/python3.7/lib-dynload', '/usr/local/lib/python3.7/site-packages']:
    sys.path.append(path)

from twisted.internet import protocol, reactor
from constants import LOCAL_PORT
from vq_protocol import VSQLServerProtocol
from privilege_manager import PrivilegeManager

if __name__ == '__main__':
    factory = protocol.ServerFactory()
    factory.protocol = VSQLServerProtocol
    priv_mgr = None #PrivilegeManager()
    factory.protocol.priv_mgr = priv_mgr
    
    print("\nready")
    
    reactor.listenTCP(LOCAL_PORT, factory)
    reactor.run()
