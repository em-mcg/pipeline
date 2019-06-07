import socket

from sprocket.controlling.common.logger import get_logger
from sprocket.controlling.common.socket_nb import SocketNB
from OpenSSL import SSL

logger = get_logger(__file__.split('/')[-1])


###
#  figure out which sockets need to be selected
###
def get_arwsocks(sockets):

    # read_socks is all objects that we could select upon
    read_socks = [ s for s in sockets
                 if isinstance(s, socket.SocketType)
                 or isinstance(s, SSL.Connection)
                 or (isinstance(s, SocketNB) and s.sock is not None) ]

    # write_socks is all read_socks that indicate they want to be written
    write_socks = [ s for s in sockets if isinstance(s, SocketNB) and (s.ssl_write or s.want_write) ]
    return (sockets, read_socks, write_socks)
