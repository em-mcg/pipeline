#!/usr/bin/python

import collections
import socket
import traceback
import sys

from OpenSSL import SSL

from sprocket.controlling.common.defs import Defs
from sprocket.controlling.common.logger import get_logger

logger = get_logger(__file__.split('/')[-1])


# wrapper around socket-like objects to handle
# non-blocking reading and writing in correct format
# This class also works for non-blocking SSL sockets!
class SocketNB(object):
    def __init__(self, sock):
        if isinstance(sock, SocketNB):
            self.sock = sock.sock
            self.want_write = sock.want_write
            self.want_handle = sock.want_handle
            self.expectlen = sock.expectlen
            self.recv_queue = sock.recv_queue
            self.send_queue = sock.send_queue
            self.recv_buf = sock.recv_buf
            self.send_buf = sock.send_buf
            self.ssl_write = sock.ssl_write
            self.handshaking = sock.handshaking

        else:
            self.sock = sock
            self.want_write = False
            self.want_handle = False
            self.expectlen = None
            self.recv_queue = collections.deque()
            self.send_queue = collections.deque()
            self.recv_buf = ""
            self.send_buf = None
            self.ssl_write = None
            self.handshaking = False

        self._fileno = sock.fileno()

    def __str__(self):
        return str(self.fileno())

    def fileno(self):
        return self._fileno

    def getsockname(self):
        return self.getsockname()

    def getpeername(self):
        return self.sock.getpeername()

    @staticmethod
    def shutdown(*_):
        pass

    def close(self):
        traceback.print_stack(file=sys.stdout)
        #logger.debug("CLOSING SOCKET %s" % traceback.format_exc())
        if self.sock is None:
            return

        if Defs.debug:
            logger.debug("CLOSING SOCKET {} {}".format(self.fileno(), traceback.format_exc()))

        try:
            if isinstance(self.sock, SSL.Connection):
                self.sock.shutdown()
            else:
                self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except:
            pass

        self.sock = None

    def _fill_recv_buf(self):
        self.ssl_write = None
        start_len = len(self.recv_buf)
        while True:
            try:
                nbuf = self.sock.recv(16384)
                if len(nbuf) == 0:
                    logger.debug("Got empty string; socket must be closed")
                    break
                else:
                    self.recv_buf += nbuf
            except SSL.WantReadError:
                logger.debug("Read error")
                start_len = -1
                break
            except SSL.WantWriteError:
                logger.debug("Write error")
                self.ssl_write = True
                start_len = -1
                break
            except Exception as e:
                logger.debug("Unknown error {}".format(e))
                start_len = -1
                break

        if len(self.recv_buf) == start_len:
            # logger.debug("CLOSING SOCKET")
            self.close()

    def do_read(self):
        if self.sock is None:
            return

        if self.handshaking:
            return self.do_handshake()

        logger.debug("Have '{}' on recv_queue".format(self.recv_queue))
        self._fill_recv_buf()
        if len(self.recv_buf) == 0:
            return

        while True:
            if self.expectlen is None:
                if len(self.recv_buf) >= Defs.header_len:
                    # NOTE exception will bubble out to calling function!
                    self.expectlen = int(self.recv_buf[0:Defs.header_len])
                    self.recv_buf = self.recv_buf[Defs.header_len:]
                else:
                    break

            # expectlen indicates how much we want, so get it
            else:
                if len(self.recv_buf) >= self.expectlen:
                    self.recv_queue.append(self.recv_buf[:self.expectlen])
                    self.recv_buf = self.recv_buf[self.expectlen:]
                    self.expectlen = None
                else:
                    break

        self.update_flags()

    def do_peek(self):
        if self.sock is None:
            return

        if self.handshaking:
            return self.do_handshake()

        try:
            nbuf = self.sock.recv(16384, socket.MSG_PEEK)
            return len(nbuf) > 0
        except SSL.WantReadError:
            pass
        except SSL.WantWriteError:
            self.ssl_write = True
            pass
        except:
            pass


    def update_flags(self):
        """
        Update send/recv flags:
          - (want_write = true) indicates that we have data to send in the send_queue
          - (want_handle = true) indicates that the recv queue is non-empty
        """
        self.want_handle = len(self.recv_queue) > 0
        self.want_write = len(self.send_queue) > 0 or self.send_buf is not None
        # logger.debug("updating flags :: want_handle: {}, want_write: {}".format(self.want_handle, self.want_write))

    def enqueue(self, msg):
        self.send_queue.append(self.format_message(msg))
        self.update_flags()

    @staticmethod
    def format_message(msg):
        return Defs.header_fmt % (len(msg), msg)

    def outofband_msg(self, msg):
        self.recv_queue.append(msg)
        self.update_flags()

    def dequeue(self):
        if len(self.recv_queue) == 0:
            return None

        ret = self.recv_queue.popleft()
        self.update_flags()
        return ret

    def _fill_send_buf(self):
        self.send_buf = '' if self.send_buf is None else self.send_buf
        if self.ssl_write is True:
            return

        # if we have multiple messages enqueued, put them all in the buffer
        while len(self.send_queue) > 0:
            self.send_buf += self.send_queue.popleft()

        if len(self.send_buf) == 0:
            self.send_buf = None

    def _send_raw(self):
        while True:
            self.ssl_write = None
            slen = 0
            try:
                slen = self.sock.send(self.send_buf)
            except (socket.error, OSError, SSL.ZeroReturnError, SSL.SysCallError, SSL.WantReadError):
                break
            except SSL.WantWriteError:
                self.ssl_write = True

            self.send_buf = self.send_buf[slen:]
            if len(self.send_buf) < 1:
                self.send_buf = None
                break

    def do_write(self):
        """
        Write send buffer to lambda socket
        """
        if self.sock is None:
            return

        if self.handshaking:
            return self.do_handshake()

        logger.debug("Have '{}' on send_queue".format(self.send_queue))
        self._fill_send_buf()
        if self.send_buf is not None:
            logger.debug("Sending messages {}".format(self.send_buf))
            self._send_raw()

        self.update_flags()

    def do_handshake(self):
        self.update_flags()
        logger.debug("doing handshake:"+str(self))
        if not isinstance(self.sock, SSL.Connection):
            logger.debug("not SSL, returning:"+str(self))
            return

        self.handshaking = True
        self.ssl_write = None
        try:
            self.sock.do_handshake()
        except SSL.WantWriteError:
            self.ssl_write = True
            logger.debug("SSL.WantWriteError:"+str(self))
        except SSL.WantReadError:
            logger.debug("SSL.WantReadError:"+str(self))
            pass
        except SSL.Error:
            logger.error("SSL.Error:"+str(self))
            self.close()
        else:
            self.handshaking = False
            logger.info("handshaked: "+str(self))
