#!/usr/bin/python
#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

"""
Implements a proxy that relays status, results, etc. upstream to the rBuilder.

When a node is started we store a mapping from their IPv6 address to the
URL of the rBuilder that created the job that the node is running. Then any
requests that come in are checked first to see if they are addressed to a
feature of the jobmaster itself (e.g. the template generator). If not, the
request path is checked against a whitelist and forwarded to the originating
rBuilder.
"""

import asyncore
import BaseHTTPServer
import errno
import httplib
import logging
import re
import socket
import SocketServer
import sys
import threading
import urllib
import weakref
from conary.lib import util
from jobmaster.subprocutil import Subprocess
from jobmaster.util import setupLogging

log = logging.getLogger(__name__)


ALLOWED_PATHS = {
        'POST': [
            re.compile('^/api/products/[^/]+/images/\d+/buildLog$'),
            ],
        'PUT': [
            re.compile('^/uploadBuild/\d+/'),
            re.compile('^/api/products/[^/]+/images/\d+/files$'),
            re.compile('^/api/products/[^/]+/images/\d+/status$'),
            ],
        }


class ConnectionClosed(Exception):
    pass


class AsyncProxyServer(asyncore.dispatcher):
    def __init__(self, port=0, map=None):
        asyncore.dispatcher.__init__(self, None, map)
        self.create_socket(socket.AF_INET6, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.bind(('::', port))
        self.port = self.socket.getsockname()[1]
        self.listen(5)

        self.targetMap = {}
        self.lock = threading.Lock()

    def handle_accept(self):
        while True:
            try:
                sock, _ = self.socket.accept()
            except socket.error, err:
                if err.args[0] == errno.EAGAIN:
                    break
                raise
            else:
                ProxyClient(sock, self._map, self)

    def addTarget(self, address, target):
        self.lock.acquire()
        try:
            self.targetMap[address] = target
        finally:
            self.lock.release()

    def findTarget(self, address):
        return 'http://rbatest01.eng.rpath.com/'


STATE_HEADER, STATE_COPY_ALL, STATE_COPY_SIZE, STATE_COPY_CHUNKED = range(4)


class ProxyDispatcher(asyncore.dispatcher):
    chunk_size = 8192
    buffer_threshold = chunk_size * 8

    def __init__(self, sock, map, server):
        asyncore.dispatcher.__init__(self, sock, map)
        self._server = weakref.ref(server)
        self.in_buffer = self.out_buffer = ''
        self.state = STATE_HEADER
        self.copy_remaining = 0L
        self._pair = None

    @property
    def pair(self):
        return self._pair and self._pair()

    @staticmethod
    def _parse_header(header):
        lines = header.rstrip().split('\r\n')
        firstline = lines.pop(0)

        headers = {}
        for line in lines:
            if ':' not in line:
                continue
            key, value = line.split(':', 1)
            value = value.lstrip()
            headers[key.lower()] = value

        return firstline, headers

    # Sending machinery
    def send(self, data):
        """
        Send C{data} as soon as possible, without blocking.
        """
        self.out_buffer += data
        self._do_send()

    def _do_send(self):
        """
        Try to send the current contents of the send queue.
        """
        if not self.connected:
            return
        while self.out_buffer:
            try:
                sent = self.socket.send(self.out_buffer)
            except socket.error, err:
                if err.args[0] == errno.EAGAIN:
                    # OS send queue is full; save the rest for later.
                    break
                else:
                    log.debug("Closing socket due to write error %s", str(err))
                    raise ConnectionClosed
            else:
                self.out_buffer = self.out_buffer[sent:]

    def handle_write(self):
        self._do_send()

    def writable(self):
        return (not self.connected) or len(self.out_buffer)

    # Receiving machinery
    def handle_read(self):
        while len(self.in_buffer) < self.buffer_threshold:
            try:
                data = self.socket.recv(self.chunk_size)
            except socket.error, err:
                if err.args[0] == errno.EAGAIN:
                    # OS recv queue is empty.
                    break
                else:
                    log.debug("Closing socket due to read error %s", str(err))
                    raise ConnectionClosed

            if not data:
                if self.in_buffer:
                    # The connection is closed, but we still have to process
                    # the data we received.
                    self._do_recv()
                raise ConnectionClosed

            self.in_buffer += data
        self._do_recv()

    def _do_recv(self):
        """
        Try to process the contents of the input queue.
        """
        while self.in_buffer:
            if self.state == STATE_HEADER:
                end = self.in_buffer.find('\r\n\r\n')
                if end > -1:
                    end += 4
                    header, self.in_buffer = (self.in_buffer[:end],
                            self.in_buffer[end:])
                    self.handle_header(header)
                    continue
                elif len(self.in_buffer) > self.buffer_threshold:
                    log.warning("Dropping connection due to excessively large "
                            "header.")
                    raise ConnectionClosed
                else:
                    break
            else:
                self.handle_copy()

    def readable(self):
        # Read data if we're processing headers (not copying), or we're copying
        # and the pair socket is not full.
        return ((not self.connected) or self.state == STATE_HEADER
                or self.pair_copyable())

    def handle_header(self, header):
        raise NotImplementedError

    # Copying machinery
    def copyable(self):
        """
        Return C{True} if the output buffer can accept more bytes.
        """
        return len(self.out_buffer) < self.buffer_threshold

    def pair_copyable(self):
        return self.pair and self.pair.copyable()

    def start_copy(self, headers):
        assert self.state == STATE_HEADER
        if 'content-length' in headers:
            self.copy_remaining = long(headers['content-length'])
            self.state = STATE_COPY_SIZE
        else:
            self.state = STATE_COPY_ALL

    def handle_copy(self):
        if not self.pair:
            # Copy to whom?
            raise ConnectionClosed

        if self.state == STATE_COPY_ALL:
            copyBytes = len(self.in_buffer)
        elif self.state == STATE_COPY_SIZE:
            copyBytes = min(len(self.in_buffer), self.copy_remaining)
        else:
            assert False

        buffer, self.in_buffer = (
                self.in_buffer[:copyBytes], self.in_buffer[copyBytes:])
        self.pair.send(buffer)

        if self.state == STATE_COPY_SIZE:
            self.copy_remaining -= copyBytes
            if not self.copy_remaining:
                # Done sending the entity; back to waiting for headers.
                self.state == STATE_HEADER

    # Cleanup machinery
    def handle_close(self):
        raise ConnectionClosed

    def handle_error(self):
        e_class, e_value, e_tb = sys.exc_info()
        if e_class is ConnectionClosed:
            self.close()
        else:
            raise #XXX


class ProxyClient(ProxyDispatcher):
    upstream = None

    def close(self):
        ProxyDispatcher.close(self)
        if self.upstream:
            self.upstream.close()
            self.upstream = None

    def handle_header(self, request):
        """
        Parse a request from the client and direct it where it needs to go.
        """
        requestline, headers = self._parse_header(request)

        words = requestline.split()
        if len(words) != 3:
            log.error("Dropping client with unsupported request line: %s",
                    requestline)
            raise ConnectionClosed

        self.method, self.path, version = words
        if version != 'HTTP/1.1':
            log.error("Dropping client with unsupported HTTP version %s",
                    version)
            raise ConnectionClosed

        return self.do_proxy(request, headers)


    def send_response(self, response, headers, body=''):
        headers.append('Content-Length: %s' % len(body))
        self.send('HTTP/1.1 %s\r\n%s\r\n\r\n%s' % (response,
            '\r\n'.join(headers), body))

    def do_proxy(self, request, headers):
        ok = False
        paths = ALLOWED_PATHS.get(self.method)
        if paths:
            for pattern in paths:
                if pattern.match(self.path):
                    ok = True
                    break
        if not ok:
            return self.send_response('403 Forbidden',
                    ['Content-Type: text/plain'], 'Proxying not permitted\r\n')

        if self.pair:
            self.pair.send(request)
        else:
            self.upstream = ProxyUpstream(None, self._map, self._server())
            self.upstream._pair = weakref.ref(self)
            self.upstream.send(request)
            self.upstream.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.upstream.connect(('172.16.160.151', 80))
            self._pair = weakref.ref(self.upstream)

        self.start_copy(headers)


class ProxyUpstream(ProxyDispatcher):
    def handle_connect(self):
        if not self.pair:
            raise ConnectionClosed
        self._do_send()

    def handle_header(self, response):
        if not self.pair:
            raise ConnectionClosed
        responseline, headers = self._parse_header(response)
        self.start_copy(headers)
        self.pair.send(response)


def test():
    import epdb, signal, os
    print os.getpid()
    def hdlr(signum, tb):
        epdb.serve()
    signal.signal(signal.SIGUSR1, hdlr)

    setupLogging(logging.DEBUG)
    s = AsyncProxyServer(1138)
    try:
        asyncore.loop(use_poll=True)
    except KeyboardInterrupt:
        print


if __name__ == '__main__':
    test()
