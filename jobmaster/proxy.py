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
import base64
import cgi
import cPickle
import errno
import logging
import os
import re
import socket
import sys
import threading
import urllib
import urlparse
import weakref
from conary.lib.log import setupLogging
from jobmaster.templategen import TemplateGenerator

log = logging.getLogger(__name__)


ALLOWED_PATHS = {
        'POST': [
            re.compile('^/api/v1/images/\d+/build_log$'),
            ],
        'PUT': [
            re.compile('^/uploadBuild/\d+/'),
            re.compile('^/api/v1/images/\d+/?$'),
            re.compile('^/api/v1/images/\d+/build_files$'),
            ],
        }


class ConnectionClosed(Exception):
    pass


class ProxyServer(asyncore.dispatcher):
    def __init__(self, port=0, _map=None, jobmaster=None):
        asyncore.dispatcher.__init__(self, None, _map)
        self.jobmaster = jobmaster and weakref.ref(jobmaster)

        self.create_socket(socket.AF_INET6, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.bind(('::', port))
        self.port = self.socket.getsockname()[1]
        self.listen(5)

        self.lock = threading.Lock()
        self.targetMap = {}

    def serve_forever(self):
        asyncore.loop(use_poll=True, map=self._map)

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

    def addTarget(self, address, targetUrl):
        if not isinstance(address, basestring):
            address = address.format(useMask=False)
        self.lock.acquire()
        try:
            (existing, refs) = self.targetMap.get(address, (None, 0))
            if existing and existing != targetUrl:
                raise RuntimeError("You must use network containers when "
                        "sharing a jobmaster between head nodes")
            refs += 1
            self.targetMap[address] = (targetUrl, refs)
        finally:
            self.lock.release()

    def removeTarget(self, address):
        if not isinstance(address, basestring):
            address = address.format(useMask=False)
        self.lock.acquire()
        try:
            (targetUrl, refs) = self.targetMap[address]
            assert refs > 0
            refs -= 1
            if refs:
                self.targetMap[address] = (targetUrl, refs)
            else:
                del self.targetMap[address]
        finally:
            self.lock.release()

    def findTarget(self, address):
        self.lock.acquire()
        try:
            target = self.targetMap.get(address)
            if target is None:
                return None
            (targetUrl, refs) = target
            assert refs > 0
            return targetUrl
        finally:
            self.lock.release()


(STATE_HEADER, STATE_COPY_ALL, STATE_COPY_SIZE, STATE_COPY_CHUNKED,
        STATE_COPY_TRAILER, STATE_CLOSING) = range(6)


class ProxyDispatcher(asyncore.dispatcher):
    """asyncore handler for the jobmaster proxy server"""
    chunk_size = 8192
    buffer_threshold = chunk_size * 8

    def __init__(self, sock, _map, server, pair=None):
        asyncore.dispatcher.__init__(self, sock, _map)
        self._server = weakref.ref(server)
        self.in_buffer = self.out_buffer = ''
        self.state = STATE_HEADER
        self.copy_remaining = 0L
        self._remote = None
        self._pair = pair and weakref.ref(pair) or None

    @property
    def server(self):
        return self._server()

    @property
    def pair(self):
        return self._pair and self._pair()

    @property
    def name(self):
        if not self.socket:
            return ''
        if self._remote is None:
            try:
                peer = self.socket.getpeername()
            except:
                self._remote = ''
            else:
                self._remote = '[%s]:%s' % peer[:2]
        return self._remote

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
                    if err.args[0] not in (errno.ECONNRESET, errno.EPIPE):
                        log.debug("Closing socket due to write error %s",
                                str(err))
                    raise ConnectionClosed
            else:
                self.out_buffer = self.out_buffer[sent:]

        if self.state == STATE_CLOSING:
            # Write buffer is flushed; close it now.
            raise ConnectionClosed

    def handle_write(self):
        self._do_send()

    def writable(self):
        return (not self.connected) or len(self.out_buffer)

    # Receiving machinery
    def handle_read(self):
        try:
            data = self.socket.recv(self.chunk_size)
        except socket.error, err:
            if err.args[0] == errno.EAGAIN:
                # OS recv queue is empty.
                return
            else:
                if err.args[0] not in (errno.ECONNRESET, errno.EPIPE):
                    log.debug("Closing socket due to read error %s", str(err))
                raise ConnectionClosed

        if True or self.state != STATE_CLOSING:
            self.in_buffer += data
            self._do_recv()

        if not data:
            raise ConnectionClosed

    def _do_recv(self):
        """
        Try to process the contents of the input queue.
        """
        last = None
        while self.in_buffer:
            # Keep processing until the input buffer stops shrinking.
            if (self.state, len(self.in_buffer)) == last:
                break
            last = self.state, len(self.in_buffer)

            if self.state == STATE_HEADER:
                end = self.in_buffer.find('\r\n\r\n')
                if end > -1:
                    end += 4
                    header, self.in_buffer = (self.in_buffer[:end],
                            self.in_buffer[end:])
                    self.handle_header(header)
                elif len(self.in_buffer) > self.buffer_threshold:
                    log.warning("Dropping connection due to excessively large "
                            "header.")
                    raise ConnectionClosed
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
        """Return True if the output buffer can accept more bytes."""
        return len(self.out_buffer) < self.buffer_threshold

    def pair_copyable(self):
        """Return True if there is a pair socket and it is copyable."""
        return self.pair and self.pair.copyable()

    def start_copy(self, headers):
        """Set copy mode based on the info in the given headers."""
        assert self.state == STATE_HEADER
        if 'transfer-encoding' in headers:
            if headers['transfer-encoding'] != 'chunked':
                log.error("Don't know how to copy transfer encoding %r",
                        headers['transfer-encoding'])
                raise ConnectionClosed
            self.copy_remaining = 0L
            self.state = STATE_COPY_CHUNKED
        elif 'content-length' in headers:
            self.copy_remaining = long(headers['content-length'])
            self.state = STATE_COPY_SIZE
        else:
            self.state = STATE_COPY_ALL

    def handle_copy(self):
        """Handle input while in copy mode."""
        if not self.pair:
            # Copy to whom?
            raise ConnectionClosed

        if self.state == STATE_COPY_ALL:
            copyBytes = len(self.in_buffer)

        elif self.state == STATE_COPY_SIZE:
            copyBytes = min(len(self.in_buffer), self.copy_remaining)
            if not copyBytes:
                # Done copying fixed-length entity; back to reading headers.
                self.state = STATE_HEADER
                return

        elif self.state == STATE_COPY_CHUNKED:
            if not self.copy_remaining:
                # Read the size of the next chunk.
                end = self.in_buffer.find('\r\n')
                if end < 0:
                    if len(self.in_buffer) > self.buffer_threshold:
                        log.warning("Very large chunk header; "
                                "closing connection.")
                        raise ConnectionClosed
                    # No chunk header yet.
                    return

                header = self.in_buffer[:end].split(';')[0]
                try:
                    next_size = int(header, 16)
                except ValueError:
                    log.error("Bad chunk header; closing connection.")
                    raise ConnectionClosed

                self.copy_remaining = end + 2 + next_size
                if next_size:
                    # Copy the CRLF after the chunk data.
                    self.copy_remaining += 2
                else:
                    # Last chunk. Switch to trailer mode.
                    self.state = STATE_COPY_TRAILER

            copyBytes = min(len(self.in_buffer), self.copy_remaining)

        elif self.state == STATE_COPY_TRAILER:
            if len(self.in_buffer) < 2:
                # Not enough bytes to determine whether there is a trailer.
                return
            elif self.in_buffer[:2] == '\r\n':
                # No trailer.
                copyBytes = 2
            else:
                end = self.in_buffer.find('\r\n\r\n')
                if end < 0:
                    return

                # Trailer found.
                copyBytes = end + 4

            self.state = STATE_HEADER

        else:
            assert False

        buf = self.in_buffer[:copyBytes]
        self.in_buffer = self.in_buffer[copyBytes:]
        self.pair.send(buf)

        if self.state in (STATE_COPY_SIZE, STATE_COPY_CHUNKED):
            self.copy_remaining -= copyBytes

    # Cleanup machinery
    def handle_close(self):
        """Handle an asyncore close event."""
        # Throw to make sure further events don't get called, and so there is
        # only one place that close events get handled.
        raise ConnectionClosed

    def handle_error(self):
        """Handle an asyncore error event."""
        e_class = sys.exc_info()[0]
        if e_class is not ConnectionClosed:
            log.exception("Unhandled exception in proxy handler; "
                    "closing connection:")
        self.close()

    def close(self):
        """Close the dispatcher object and its socket."""
        asyncore.dispatcher.close(self)
        pair, self._pair = self.pair, None
        if pair:
            pair.pair_closed()

    def pair_closed(self):
        """Handle the paired socket having closed."""


class ProxyClient(ProxyDispatcher):
    upstream = None

    def pair_closed(self):
        """Handle the upstream socket closing by changing to CLOSED state."""
        if self.out_buffer:
            # Don't close the connection until the send queue is empty.
            self.state = STATE_CLOSING
        else:
            self.close()

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

        method, path, version = words
        if version != 'HTTP/1.1':
            log.error("Dropping client with unsupported HTTP version %s",
                    version)
            raise ConnectionClosed

        log.debug('%s "%s"', self.name, requestline)

        if path.startswith('/templates/'):
            return self.do_templates(method, path, headers)
        else:
            return self.do_proxy(request, method, path, headers)

    def send_response(self, response, headers, body=''):
        """Send a simple HTTP response."""
        headers.append('Content-Length: %s' % len(body))
        self.send('HTTP/1.1 %s\r\n%s\r\n\r\n%s' % (response,
            '\r\n'.join(headers), body))

    def send_text(self, response, body):
        """Send a simple HTTP response with text/plain content."""
        self.send_response(response, ['Content-Type: text/plain'], body)

    def do_proxy(self, request, method, path, headers):
        """Attempt to proxy a request upstream."""
        proxyOK = False
        paths = ALLOWED_PATHS.get(method)
        if paths:
            for pattern in paths:
                if pattern.match(path):
                    proxyOK = True
                    break
        if not proxyOK:
            return self.send_text('403 Forbidden',
                    'Proxying not permitted\r\n')

        if self.pair:
            self.pair.send(request)
        else:
            # Note that we don't need to keep a strong reference to the paired
            # connection because one is kept in the asyncore poll map.
            upstream = ProxyUpstream(None, self._map, self._server(), self)
            upstream._pair = weakref.ref(self)
            upstream.send(request)
            self._connect(upstream)

        self.start_copy(headers)

    def _connect(self, upstream):
        # Figure out who we're proxying to.
        peer = self.socket.getpeername()[0]
        url = self.server.findTarget(peer)
        if not url:
            return self.send_text('403 Forbidden', 'Peer not recognized\r\n')

        # Split the URL to get the hostname.
        scheme, url = urllib.splittype(url)
        if scheme != 'http':
            return self.send_text('504 Gateway Timeout',
                    'Invalid target URL\r\n')
        host, port = _split_hostport(urllib.splithost(url)[0])

        # Resolve the hostname to an address.
        try:
            addresses = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
        except socket.gaierror, err:
            log.error("Error resolving target URL: %s", err)
            addresses = []
        if not addresses:
            return self.send_text('504 Gateway Timeout',
                    'Unknown target URL\r\n')

        # Create the right socket type and initiate the connection (which may
        # not complete immediately).
        family, socktype, _, _, address = addresses[0]
        upstream.create_socket(family, socktype)
        upstream.connect(address)

        self._pair = weakref.ref(upstream)

    def do_templates(self, method, path, headers):
        """Handle a request to get anaconda templates."""
        clength = headers.get('content-length', 0)
        if clength != 0:
            # Implementing this would add a lot of complexity for something we
            # can handle entirely in URI parameters.
            return self.send_text('400 Bad Request',
                    'Request body not allowed here.\r\n')

        # Figure out who we're proxying to.
        peer = self.socket.getpeername()[0]
        url = self.server.findTarget(peer)
        if not url:
            return self.send_text('403 Forbidden', 'Peer not recognized\r\n')

        # Figure out what they want.
        path, _, query = urlparse.urlparse(path)[2:5]
        assert path.startswith('/templates/')
        path = path[11:]
        query = cgi.parse_qs(query)

        if method == 'GET' and path == 'getTemplate':
            try:
                blob = base64.urlsafe_b64decode(query['p'][0])
                params = cPickle.loads(blob)
            except:
                log.warning("Bad getTemplate request:", exc_info=True)
                return self.send_text('400 Bad Request',
                        'Bad arguments for getTemplate\r\n')

            start = not query.get('nostart', 0)
            if start:
                log.debug("Client requested template %s=%s[%s]",
                        *params['templateTup'][0])
            jobmaster = self.server.jobmaster()
            assert jobmaster
            conaryCfg = jobmaster.getConaryConfig(url)
            workDir = jobmaster.cfg.getTemplateCache()
            generator = TemplateGenerator(params['templateTup'],
                    params['kernelTup'], conaryCfg, workDir)

            status, path = generator.getTemplate(start)
            path = os.path.basename(path)
            if generator.pid:
                # Make sure the main event loop will reap the generator when it
                # quits.
                jobmaster.subprocesses.append(generator)
            return self.send_text('200 OK', '%s\r\n%s\r\n' % (
                generator.Status.values[status], path))

        else:
            return self.send_text('404 Not Found', 'Unknown function\r\n')


class ProxyUpstream(ProxyDispatcher):
    def pair_closed(self):
        self.close()

    def handle_connect(self):
        if not self.pair:
            raise ConnectionClosed
        self._do_send()

    def handle_read(self):
        if not self.pair:
            raise ConnectionClosed
        ProxyDispatcher.handle_read(self)

    def handle_header(self, response):
        responseline, headers = self._parse_header(response)
        code = responseline.split(' ', 2)[1]
        code = int(code)
        if 100 <= code < 200:
            # 1xx codes don't have an entity.
            pass
        else:
            self.start_copy(headers)
        self.pair.send(response)


def _split_hostport(host):
    i = host.rfind(':')
    j = host.rfind(']')
    if i > j:
        port = int(host[i+1:])
        host = host[:i]
    else:
        port = 80
    if host and host[0] == '[' and host[-1] == ']':
        host = host[1:-1]
    return host, port


def test():
    import epdb, signal
    print os.getpid()
    def hdlr(signum, sigtb):
        epdb.serve()
    signal.signal(signal.SIGUSR1, hdlr)

    setupLogging(consoleLevel=logging.DEBUG, consoleFormat='file')
    s = ProxyServer(7770)
    try:
        asyncore.loop(use_poll=True)
    except KeyboardInterrupt:
        print


if __name__ == '__main__':
    test()
