#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import os
import subprocess
import signal
import tempfile
import testsuite
import testhelp
import threading

from conary import versions
from conary.lib import util
from cStringIO import StringIO

from jobmaster import master
from jobmaster import templateserver
from jobmaster import imagecache


class DummyConnection(object):
    def __init__(self, *args, **kwargs):
        self.sent = []
        self.listeners = []
        self.subscriptions = []
        self.unsubscriptions = []
        self.acks = []

    def send(self, dest, message):
        self.sent.insert(0, (dest, message))

    def receive(self, message):
        for listener in self.listeners:
            listener.receive(message)

    def subscribe(self, dest, ack = 'auto'):
        if dest.startswith('/queue/'):
            assert ack == 'client', 'Queue will not be able to refuse a message'
        self.subscriptions.insert(0, dest)

    def unsubscribe(self, dest):
        self.unsubscriptions.insert(0, dest)

    def addlistener(self, listener):
        if listener not in self.listeners:
            self.listeners.append(listener)

    def dellistener(self, listener):
        if listener in self.listeners:
            self.listeners.remove(listener)

    def start(self):
        pass

    def ack(self, messageId):
        self.acks.append(messageId)

    def insertMessage(self, message):
        message = 'message-id: dummy-message\n\n\n' + message
        self.receive(message)

    def disconnect(self):
        pass

class DummyQueue(object):
    type = 'queue'

    def __init__(self, host, port, dest, namespace = 'test', timeOut = 600,
                 queueLimit = None, autoSubscribe = True):
        self.connectionName = '/' + '/'.join((self.type, 'test', dest))
        self.incoming = []
        self.outgoing = []
        self.messageCount = 0

    def send(self, message):
        assert type(message) in (str, unicode), \
            "Can't put non-strings in a queue"
        message = 'message-id: message-%d\n\n\n' % self.messageCount + message
        self.messageCount += 1
        self.outgoing.insert(0, message)

    def read(self):
        return self.incoming and self.incoming.pop() or None

    def disconnect(self):
        pass

class DummyMultiplexedQueue(DummyQueue):
    def __init__(self, host, port, dest = [], namespace = 'test',
                 timeOut = 600, queueLimit = None, autoSubscribe = True):
        self.incoming = []
        self.outgoing = []
        self.messageCount = 0

    def send(self, dest, message):
        assert type(message) in (str, unicode), \
            "Can't put non-strings in a queue"
        message = 'message-id: message-%d\n\n\n' % self.messageCount + message
        self.messageCount += 1
        self.outgoing.insert(0, (dest, message))

    def addDest(self, dest):
        pass

class DummyTopic(DummyQueue):
    type = 'topic'

class DummyMultiplexedTopic(DummyMultiplexedQueue):
    type = 'topic'

class DummyTemplateServer(object):
    __init__ = lambda *args, **kwargs: None
    start = lambda *args, **kwargs: None
    stop = lambda *args, **kwargs: None

class ThreadedJobMaster(master.JobMaster, threading.Thread):
    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self)
        master.getRunningKernel = FakeGetRunningKernel
        TemplateServer = templateserver.TemplateServer
        try:
            templateserver.TemplateServer = DummyTemplateServer
            master.JobMaster.__init__(self, *args, **kwargs)
        finally:
            templateserver.TemplateServer = TemplateServer


    def resolveTroveSpec(self, troveSpec):
        return troveSpec

    def getMaxSlaves(self):
        # needed for test suite purposes
        return 99999

    def realSlaveLimit(self):
        return 99999

class JobMasterHelper(testhelp.TestCase):
    def DummySystem(self, command):
        self.callLog.append(command)

    def setUp(self):
        class FakePopen:
            def __init__(self2, cmd, *args, **kwargs):
                self.callLog.append(cmd)
                self2.stderr = StringIO()
                self2.stdout = StringIO()
                self2.returncode = 0

            def poll(self2):
                return True

            def wait(self2):
                return 0

            def communicate(self2):
                # used in the templategen generate function
                return '', ''

        self.basePath = tempfile.mkdtemp()
        os.mkdir(os.path.join(self.basePath, 'imageCache'))
        os.mkdir(os.path.join(self.basePath, 'logs'))
        os.mkdir(os.path.join(self.basePath, 'config.d'))
        os.mkdir(os.path.join(self.basePath, 'tmp'))

        master.CONFIG_PATH = os.path.join(self.basePath, 'config.d', 'runtime')

        testhelp.TestCase.setUp(self)
        self.cfg = master.MasterConfig()
        self.cfg.nodeName = 'testMaster'
        self.cfg.nameSpace = 'test'
        self.cfg.basePath = self.basePath
        self.cfg.logFile = os.path.join(self.basePath, 'logs', 'jobmaster.log')
        self.jobMaster = ThreadedJobMaster(self.cfg)
        # ensure bootup messages don't interfere with tests
        self.jobMaster.response.response.connection.sent = []
        self.oldSubprocessPopen = subprocess.Popen
        self.oldOsSystem = os.system

        self.callLog = []
        os.system = self.DummySystem
        subprocess.Popen = FakePopen

        # Don't spend all day creating 256MB swap images
        imagecache.SWAP_SIZE = 1048576

    def tearDown(self):
        # Make sure logfiles get closed
        import logging
        log = logging.getLogger('')
        for handler in log.handlers:
            handler.close()
            log.removeHandler(handler)

        util.rmtree(self.cfg.basePath)
        #self.jobMaster.command.connection
        testhelp.TestCase.tearDown(self)
        os.system = self.oldOsSystem
        subprocess.Popen = self.oldSubprocessPopen

        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def assertLogContent(self, content):
        f = open(self.cfg.logFile)
        assert content in f.read(), "'%s' did not appear in log" % content

    def touch(self, fn, contents = ''):
       if not os.path.exists(fn):
           util.mkdirChain(os.path.split(fn)[0])
           f = open(fn, 'w')
           f.write(contents)
           f.close()

_uname = '2.6.porkchops-0.0.1.smp.vcplusplus.sparc'
kernelData = dict(uname=_uname,
            kernel='/boot/' + _uname, initrd='/boot/' + _uname + '.img',
            trove=('bean:cup','/conary.example.com/1.2.3-4-5', None))
def FakeGetRunningKernel(): return kernelData
