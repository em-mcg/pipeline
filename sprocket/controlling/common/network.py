#!/usr/bin/python
import logging
import socket
import traceback

from OpenSSL import SSL, crypto
from OpenSSL._util import lib as _ssl_lib

import sprocket.controlling.common.defs
import sprocket.controlling.common.socket_nb


# format the base64 portion of an SSL cert into something libssl can use
def format_pem(ctype, cert):
    fmt_cert = "-----BEGIN %s-----\n" % ctype
    while len(cert) > 0:
        fmt_cert += cert[:64] + "\n"
        cert = cert[64:]
    fmt_cert += "-----END %s-----\n" % ctype

    return fmt_cert

format_ssl_cert = lambda c: format_pem("CERTIFICATE", c)
format_ssl_key = lambda c: format_pem("RSA PRIVATE KEY", c)

def format_ssl_cert_chain(chain):
    res = ""
    for c in chain.split(' '):
        res += format_ssl_cert(c)

    return res


###
#  connect a socket, maybe SSLizing
###
def connect_socket(addr, port, cacert, srvcrt, srvkey):
    # connect to the master for orders
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.connect((addr, port))

    # if we have a cacert, this means we should use SSL for this connection
    if cacert is not None:
        s = sslize(s, cacert, srvcrt, srvkey, True)
        if not isinstance(s, SSL.Connection):
            return "ERROR could not initialize SSL connection: %s\n" % str(s)

    # wrap in non-blocking socket reader/writer class
    s.setblocking(False)
    s = sprocket.controlling.common.socket_nb.SocketNB(s)
    s.do_handshake()

    return s

def ssl_context(cacert, srvcrt, srvkey):
    # general setup: TLSv1.2, no compression, paranoid ciphers
    sslctx = SSL.Context(SSL.TLSv1_2_METHOD)
    sslctx.set_verify_depth(9)
    sslctx.set_options(SSL.OP_NO_COMPRESSION)
    sslctx.set_mode(_ssl_lib.SSL_MODE_ENABLE_PARTIAL_WRITE | _ssl_lib.SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER)
    sslctx.set_cipher_list(sprocket.controlling.common.defs.Defs.cipher_list)
    sslctx.set_verify(SSL.VERIFY_PEER | SSL.VERIFY_FAIL_IF_NO_PEER_CERT, lambda _, __, ___, ____, ok: ok)

    # use CA cert provided during lambda invocation
    fmt_cert = format_ssl_cert(cacert)
    x509_cert = crypto.load_certificate(crypto.FILETYPE_PEM, fmt_cert)
    sslctx.get_cert_store().add_cert(x509_cert)

    # add my certificate chain
    has_cert = False
    for cert in srvcrt.split(' '):
        x509_cert = crypto.load_certificate(crypto.FILETYPE_PEM, format_ssl_cert(cert))
        if not has_cert:
            sslctx.use_certificate(x509_cert)
            has_cert = True
        else:
            sslctx.add_extra_chain_cert(x509_cert)

    # private key
    sslctx.use_privatekey(crypto.load_privatekey(crypto.FILETYPE_PEM, format_ssl_key(srvkey)))

    # check that all's well
    sslctx.check_privatekey()

    return sslctx

###
#  SSLize a connected socket, requiring a supplied cacert
###
def sslize(sock, cacert, srvcrt, srvkey, is_connect):
    sslconn = None
    try:
        sslctx = ssl_context(cacert, srvcrt, srvkey)
        sslconn = SSL.Connection(sslctx, sock)
        if is_connect:
            sslconn.set_connect_state()
        else:
            sslconn.set_accept_state()
    except:
        return traceback.format_exc()
    else:
        return sslconn

###
#  listen on a socket, maybe SSLizing
###
def listen_socket(addr, port, cacert, srvcrt, srvkey, nlisten=1):
    ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ls.bind((addr, port))
    ls.listen(nlisten)
    logging.debug("start listening")

    if cacert is not None and srvcrt is not None and srvkey is not None:
        logging.debug("start sslizing")
        ls = sslize(ls, cacert, srvcrt, srvkey, False)
        if not isinstance(ls, SSL.Connection):
            return "ERROR could not initialize SSL connection: %s\n" % str(ls)
    ls.setblocking(False)
    return ls

###
#  accept from a listening socket and hand back a SocketNB
###
def accept_socket(lsock):
    (ns, _) = lsock.accept()
    ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ns.setblocking(False)
    ns = sprocket.controlling.common.socket_nb.SocketNB(ns)
    ns.do_handshake()

    return ns

