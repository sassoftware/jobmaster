#!/usr/bin/python2.4
#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()

import cPickle
import os
import socket
import tempfile
import time

from jobmaster import templateserver

from conary.lib import util

class TemplateServerTest(testsuite.TestCase):
    def setUp(self):
        self.__class__.__base__.setUp(self)
        self.tmpDir = tempfile.mkdtemp()
        self.templateRoot = tempfile.mkdtemp()
        self.TIMEOUT = templateserver.TIMEOUT
        templateserver.TIMEOUT = 0
        self.srv = templateserver.getServer(self.templateRoot,
                tmpDir = self.tmpDir)

    def tearDown(self):
        self.__class__.__base__.tearDown(self)
        util.rmtree(self.tmpDir)
        util.rmtree(self.templateRoot)
        templateserver.TIMEOUT = self.TIMEOUT

    def testStop(self):
        self.started = False
        class FakeSocket(object):
            def accept(x):
                self.started = True
                raise socket.timeout
        self.srv.socket = FakeSocket()
        self.srv.start()
        started = False
        while not started:
            # use the templateserver's lock, to prevent race conditions
            self.srv.lock.acquire()
            started = self.started
            self.srv.lock.release()
        self.srv.stop()

    def testRunAfterStopped(self):
        self.srv.running = False
        self.srv.run()

    def testInlineRun(self):
        class FakeSocket(object):
            def accept(*args, **kwargs):
                self.srv.running = False
                raise socket.timeout
        self.srv.socket = FakeSocket()
        self.srv.run()

    def testSocketError(self):
        class FakeServer(object):
            def __init__(x, *args, **kwargs):
                exc = socket.error(101)
                raise exc
        TemplateServer = templateserver.TemplateServer
        try:
            templateserver.TemplateServer = FakeServer
            self.assertRaises(socket.error, templateserver.getServer, '/tmp')
        finally:
            templateserver.TemplateServer = TemplateServer

class StubHandler(templateserver.TemplateServerHandler):
    def __init__(self, path, templateRoot = None, *args, **kwargs):
        self.templateRoot = templateRoot
        self.path = path
        self.errors = []
        self.headers = []
        self.responses = []
        self.hostname = 'local.test'
        self.port = '8100'

    def send_error(self, code, msg = ''):
        self.errors.append((code, msg))

    def send_response(self, code):
        self.responses.append(code)

    def send_header(self, key, val):
        self.headers.append((key, val))

    def end_headers(self):
        pass

class TemplateServerHandlerTest(testsuite.TestCase):
    bases = {}
    def setUp(self):
        self.__class__.__base__.setUp(self)

    def tearDown(self):
        self.__class__.__base__.tearDown(self)

    def touch(self, path, contents = ''):
        util.mkdirChain(os.path.dirname(path))
        if not os.path.exists(path):
            f = open(path, 'w')
            f.write(contents)
            f.close()

    def testDo_POST(self):
        hdlr = StubHandler('/makeTemplate')
        hdlr.makeTemplate = lambda *args, **kwargs: None
        hdlr.do_POST()
        self.failIf(hdlr.errors)

    def testBadPost(self):
        hdlr = StubHandler('/bogus_path')
        hdlr.makeTemplate = lambda *args, **kwargs: None
        hdlr.do_POST()
        # bad request
        self.assertEquals(hdlr.errors, [(400, '')])

    def testBadStatus(self):
        hdlr = StubHandler('/bogus')
        hdlr.status()
        # bad request
        self.assertEquals(hdlr.errors, [(400, '')])

    def testStatusMissing(self):
        tmpDir = tempfile.mkdtemp()
        try:
            hdlr = StubHandler('/stuff?h=hash', templateRoot = tmpDir)
            hdlr.status()
            # no such template
            self.assertEquals(hdlr.errors, [(404, 'No such template')])
        finally:
            util.rmtree(tmpDir)

    def testStatusForward(self):
        tmpDir = tempfile.mkdtemp()
        try:
            self.touch(os.path.join(tmpDir, 'hash.tar'))
            self.touch(os.path.join(tmpDir, '.hash.metadata'))
            hdlr = StubHandler('/stuff?h=hash', templateRoot = tmpDir)
            hdlr.status()
            self.assertEquals(hdlr.responses, [303])
            self.assertEquals(hdlr.headers, \
                    [('Location', 'http://local.test:8100/hash')])
        finally:
            util.rmtree(tmpDir)

    def testStatusText(self):
        tmpDir = tempfile.mkdtemp()
        try:
            self.touch(os.path.join(tmpDir, '.hash.status'), contents = 'stuff')
            hdlr = StubHandler('/stuff?h=hash', templateRoot = tmpDir)
            hdlr.status()
            self.assertEquals(hdlr.responses, [200])
            self.assertEquals(hdlr.headers,
                    [('Content-Type', 'text/plain'), ('Content-Length', '5')])
        finally:
            util.rmtree(tmpDir)

    def testSendHeadStatus(self):
        tmpDir = tempfile.mkdtemp()
        try:
            self.touch(os.path.join(tmpDir, '.hash.status'), contents = 'stuff')
            hdlr = StubHandler('/status?h=hash', templateRoot = tmpDir)
            hdlr.send_head()
            self.assertEquals(hdlr.responses, [200])
            self.assertEquals(hdlr.headers,
                    [('Content-Type', 'text/plain'), ('Content-Length', '5')])
        finally:
            util.rmtree(tmpDir)

    def testSendHeadMissing(self):
        tmpDir = tempfile.mkdtemp()
        try:
            hdlr = StubHandler('/hash', templateRoot = tmpDir)
            hdlr.send_head()
            self.assertEquals(hdlr.errors, [(404, 'Template not found')])
        finally:
            util.rmtree(tmpDir)

    def testSendHeadTarball(self):
        tmpDir = tempfile.mkdtemp()
        try:
            metadata = cPickle.dumps({'custom_header': 'stuff'})
            self.touch(os.path.join(tmpDir, 'hash.tar'), contents = 'splatter')
            self.touch(os.path.join(tmpDir, '.hash.metadata'),
                    contents = metadata)
            hdlr = StubHandler('/hash', templateRoot = tmpDir)
            hdlr.command = 'head'
            hdlr.send_head()
            self.assertEquals(hdlr.responses, [204])
            ref = [('Content-Type', 'application/x-tar'),
                    ('Content-Length', '8'),
                    ('X-custom-header', 'stuff')]
            self.assertEquals(hdlr.headers, ref)
        finally:
            util.rmtree(tmpDir)

    def testSendHeadCommand(self):
        tmpDir = tempfile.mkdtemp()
        try:
            metadata = cPickle.dumps({'custom_header': 'stuff'})
            self.touch(os.path.join(tmpDir, 'hash.tar'), contents = 'splatter')
            self.touch(os.path.join(tmpDir, '.hash.metadata'),
                    contents = metadata)
            hdlr = StubHandler('/hash', templateRoot = tmpDir)
            hdlr.command = 'something'
            hdlr.send_head()
            self.assertEquals(hdlr.responses, [200])
            ref = [('Content-Type', 'application/x-tar'),
                    ('Content-Length', '8'),
                    ('X-custom-header', 'stuff')]
            self.assertEquals(hdlr.headers, ref)
        finally:
            util.rmtree(tmpDir)


if __name__ == "__main__":
    testsuite.main()
