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
import StringIO
import tempfile
import time

from jobmaster import templateserver
from jobmaster import templategen

from conary.lib import util
from conary.errors import TroveNotFound

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

    def testCleanStaleLocks(self):
        '''
        Ensure that stale lockfiles are deleted on server start.
        Tests RBL-2155
        '''
        def listdir(dir):
            self.assertEquals(dir, self.templateRoot)
            return ['.00112233445566778899aabbccddeeff.status',
                     '112233445566778899aabbccddeeff00.tar']
        def unlink(path):
            self.assertEquals(path, os.path.join(self.templateRoot,
                '.00112233445566778899aabbccddeeff.status'),
                'Attempted to delete the wrong file')
            templateserver._file_deleted = True

        class FakeSocket(object):
            def accept(*args, **kwargs):
                self.srv.running = False
                raise socket.timeout

        _listdir = os.listdir
        _unlink = os.unlink
        try:
            templateserver.os.listdir = listdir
            templateserver.os.unlink = unlink
            templateserver._file_deleted = False

            self.srv.socket = FakeSocket()
            self.srv.run()
        finally:
            templateserver.os.listdir = _listdir
            templateserver.os.unlink = _unlink

        self.assertEquals(templateserver._file_deleted, True,
            'Template building lock was not deleted')

class StubHeaders(object):
    def __init__(self):
        self.headers = {'content-length': '100'}
    def getheader(self, key):
        return self.headers.get(key)
    def setheader(self, key, val):
        self.headers[key] = val

class StubHandler(templateserver.TemplateServerHandler):
    def __init__(self, path, templateRoot = None, *args, **kwargs):
        self.templateRoot = templateRoot
        self.path = path
        self.headers = StubHeaders()
        self.errors = []
        self.responses = []
        self.responseHeaders = []
        self.hostname = 'local.test'
        self.port = '8100'
        self.rfile = StringIO.StringIO()

    def send_error(self, code, msg = ''):
        self.errors.append((code, msg))

    def send_response(self, code):
        self.responses.append(code)

    def send_header(self, key, val):
        self.responseHeaders.append((key, val))

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
            self.assertEquals(hdlr.responseHeaders, \
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
            self.assertEquals(hdlr.responseHeaders,
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
            self.assertEquals(hdlr.responseHeaders,
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
            self.assertEquals(hdlr.responseHeaders, ref)
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
            self.assertEquals(hdlr.responseHeaders, ref)
        finally:
            util.rmtree(tmpDir)

    def testMakeTemplateVersions(self):
        tmpDir = tempfile.mkdtemp()
        try:
            hdlr = StubHandler('/hash', templateRoot = tmpDir)
            hdlr.makeTemplate()
            self.assertEquals(hdlr.errors, [(400, '')])
        finally:
            util.rmtree(tmpDir)

    def testMakeTemplate404(self):
        class DummyAnacondaTemplate(object):
            def __init__(self, *args, **kwargs):
                raise TroveNotFound
        tmpDir = tempfile.mkdtemp()
        AnacondaTemplate = templategen.AnacondaTemplate
        try:
            templategen.AnacondaTemplate = DummyAnacondaTemplate
            hdlr = StubHandler('/hash', templateRoot = tmpDir)
            hdlr.rfile = StringIO.StringIO('v=/test.rpath.local@rpl:1/1-1-1&f=[is: x86]')
            hdlr.makeTemplate()
            self.assertEquals(hdlr.errors, [(404, '')])
        finally:
            templategen.AnacondaTemplate = AnacondaTemplate
            util.rmtree(tmpDir)

    def testMakeTemplateDone(self):
        class DummyAnacondaTemplate(object):
            __init__ = lambda *args, **kwargs: None
            getFullTroveSpecHash = lambda *args, **kwargs: '12345'
            status = lambda *args, **kwargs: (False, '')
            exists = lambda *args, **kwargs: True
        tmpDir = tempfile.mkdtemp()
        AnacondaTemplate = templategen.AnacondaTemplate
        try:
            templategen.AnacondaTemplate = DummyAnacondaTemplate
            hdlr = StubHandler('/hash', templateRoot = tmpDir)
            hdlr.rfile = StringIO.StringIO('v=/test.rpath.local@rpl:1/1-1-1&f=[is: x86]')
            hdlr.makeTemplate()
            self.assertEquals(hdlr.responses, [303])
            self.assertEquals(hdlr.responseHeaders,
                    [('Location', 'http://local.test:8100/12345')])
        finally:
            templategen.AnacondaTemplate = AnacondaTemplate
            util.rmtree(tmpDir)

    def testMakeTemplateRunning(self):
        class DummyAnacondaTemplate(object):
            __init__ = lambda *args, **kwargs: None
            getFullTroveSpecHash = lambda *args, **kwargs: '12345'
            status = lambda *args, **kwargs: (True, 'status')
            exists = lambda *args, **kwargs: False
        tmpDir = tempfile.mkdtemp()
        AnacondaTemplate = templategen.AnacondaTemplate
        try:
            templategen.AnacondaTemplate = DummyAnacondaTemplate
            hdlr = StubHandler('/hash', templateRoot = tmpDir)
            hdlr.rfile = StringIO.StringIO('v=/test.rpath.local@rpl:1/1-1-1&f=[is: x86]')
            hdlr.makeTemplate()
            self.assertEquals(hdlr.responses, [303])
            self.assertEquals(hdlr.responseHeaders,
                    [('Location', 'http://local.test:8100/status?h=12345')])
        finally:
            templategen.AnacondaTemplate = AnacondaTemplate
            util.rmtree(tmpDir)

    def testMakeTemplate(self):
        class DummyAnacondaTemplate(object):
            __init__ = lambda *args, **kwargs: None
            getFullTroveSpecHash = lambda *args, **kwargs: '12345'
            getFullTroveSpec = lambda *args, **kwargs: '12345'
            status = lambda *args, **kwargs: (False, '')
            exists = lambda *args, **kwargs: False
            generate = lambda *args, **kwargs: None
        tmpDir = tempfile.mkdtemp()
        AnacondaTemplate = templategen.AnacondaTemplate
        fork = os.fork
        _exit = os._exit
        close = os.close
        try:
            os.fork = lambda *args, **kwargs: None
            os._exit = lambda *args, **kwargs: None
            os.close = lambda *args, **kwargs: None
            templategen.AnacondaTemplate = DummyAnacondaTemplate
            hdlr = StubHandler('/hash', templateRoot = tmpDir)
            hdlr.rfile = StringIO.StringIO('v=/test.rpath.local@rpl:1/1-1-1&f=[is: x86]')
            hdlr.rfile.fileno = lambda *args, **kwargs: 0
            hdlr.wfile = StringIO.StringIO()
            hdlr.makeTemplate()
            self.assertEquals(hdlr.responses, [202])
            self.assertEquals(hdlr.responseHeaders,
                    [('X-Full-Trove-Spec-Hash', '12345'),
                        ('Location', 'http://local.test:8100/status?h=12345'),
                        ('Content-Type', 'text/html'),
                        ('Content-Length', 217)])
        finally:
            os.close = close
            os._exit = _exit
            os.fork = fork
            templategen.AnacondaTemplate = AnacondaTemplate
            util.rmtree(tmpDir)

    def testMakeTemplateError(self):
        class DummyAnacondaTemplate(object):
            __init__ = lambda *args, **kwargs: None
            getFullTroveSpecHash = lambda *args, **kwargs: '12345'
            getFullTroveSpec = lambda *args, **kwargs: '12345'
            status = lambda *args, **kwargs: (False, '')
            exists = lambda *args, **kwargs: False
            def generate(*args, **kwargs):
                raise RuntimeError, "testing a codepath"
        def fake_exit(code):
            raise RuntimeError, "error code: %d" % code
        tmpDir = tempfile.mkdtemp()
        AnacondaTemplate = templategen.AnacondaTemplate
        fork = os.fork
        _exit = os._exit
        close = os.close
        try:
            os.fork = lambda *args, **kwargs: None
            os._exit = fake_exit
            os.close = lambda *args, **kwargs: None
            templategen.AnacondaTemplate = DummyAnacondaTemplate
            hdlr = StubHandler('/hash', templateRoot = tmpDir)
            hdlr.rfile = StringIO.StringIO('v=/test.rpath.local@rpl:1/1-1-1&f=[is: x86]')
            hdlr.rfile.fileno = lambda *args, **kwargs: 0
            hdlr.makeTemplate()
            self.assertEquals(hdlr.errors, [(500, 'error code: 1')])
        finally:
            os.close = close
            os._exit = _exit
            os.fork = fork
            templategen.AnacondaTemplate = AnacondaTemplate
            util.rmtree(tmpDir)


if __name__ == "__main__":
    testsuite.main()
