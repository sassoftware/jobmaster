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

import BaseHTTPServer
import httplib
import logging
import re
import socket
import SocketServer
import threading
import urllib
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


class ProxyServer(SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer,
        Subprocess):
    address_family = socket.AF_INET6

    def __init__(self, server_address):
        BaseHTTPServer.HTTPServer.__init__(self, server_address, ProxyHandler)
        self.targetMap = {}
        self._lock = threading.Lock()

    def run(self):
        try:
            self.serve_forever()
        except KeyboardInterrupt:
            pass

    def handle_error(self, request, client_address):
        log.exception("Unhandled exception in thread serving request for %s:",
                client_address)

    def addTarget(self, address, target):
        self._lock.acquire()
        try:
            self.targetMap[address] = target
        finally:
            self._lock.release()

    def removeTarget(self, address):
        self._lock.acquire()
        try:
            del self.targetMap[address]
        finally:
            self._lock.release()

    def getTarget(self, address):
        return 'http://rbatest01.eng.rpath.com/'
        self._lock.acquire()
        try:
            return self.targetMap.get(address)
        finally:
            self._lock.release()


class ProxyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_proxy(self):
        client = self.client_address[0]
        # Strip IPv4-in-IPv6 address notation.
        if client.startswith('::ffff:'):
            client = client[7:]

        # Which rBuilder is this client working for?
        target = self.server.getTarget(client)
        if not target:
            self.send_error(403, "Unknown client")
            return

        # Is this request allowed?
        allowed = ALLOWED_PATHS.get(self.command)
        if not allowed:
            self.send_error(403, "Request not allowed")
            return
        for pattern in allowed:
            if pattern.match(self.path):
                self.forward_request(target)
                break
        else:
            self.send_error(403, "Request not allowed")

    def forward_request(self, target):
        # Determine and connect to the target host.
        scheme, url = urllib.splittype(target)
        if scheme != 'http':
            log.error("Can't forward to target %r", target)
            self.send_error(500, "Unable to forward request")
            return

        host, url = urllib.splithost(url)
        host, port = urllib.splitport(host)

        conn = httplib.HTTPConnection(host, port)
        conn.connect()

        # Forward the request and its entity (body) to the target.
        self.headers['Connection'] = 'close'
        if 'Expect' in self.headers:
            del self.headers['Expect']
        conn.putrequest(self.command, self.path, skip_host=True,
                skip_accept_encoding=True)
        for header in self.headers.headers:
            conn.putheader(*header.rstrip().split(':', 1))
        conn.endheaders()
        length = long(self.headers.get('Content-Length', 0))
        if length:
            util.copyfileobj(self.rfile, conn, sizeLimit=length)

        # Get the response and forward it back to the caller.
        resp = httplib.HTTPResponse(conn.sock, method=self.command)
        resp.begin()
        assert resp.version == 11
        assert not resp.chunked
        self.wfile.write('HTTP/1.1 %s %s\r\n%s\r\n'
                % (resp.status, resp.reason, str(resp.msg)))
        if resp.length:
            util.copyfileobj(resp.fp, self.wfile, sizeLimit=resp.length)

        resp.close()
        conn.close()

    # Entry points from BaseHTTPRequestHandler
    do_DELETE = do_proxy
    do_GET = do_proxy
    do_HEAD = do_proxy
    do_POST = do_proxy
    do_PUT = do_proxy

    def log_error(self, format, *args):
        self._log(logging.ERROR, format, *args)
    def log_message(self, format, *args):
        self._log(logging.ERROR, format, *args)

    def _log(self, level, format, *args):
        message = '[%s] %s' % (self.address_string(), format)
        log.log(level, message, *args)


def test():
    setupLogging(logging.DEBUG)
    s = ProxyServer(('', 1138))
    s.start()
    try:
        s.wait()
    except KeyboardInterrupt:
        s.kill()


if __name__ == '__main__':
    test()
