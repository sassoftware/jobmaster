#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import testsuite
import testhelp

from jobmaster import master

import tempfile
import threading

from conary.lib import util

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


class ThreadedJobMaster(master.JobMaster, threading.Thread):
    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self)
        master.JobMaster.__init__(self, *args, **kwargs)

class JobMasterHelper(testhelp.TestCase):
    def setUp(self):
        testhelp.TestCase.setUp(self)
        self.cfg = master.MasterConfig()
        self.cfg.nodeName = 'testMaster'
        self.cfg.nameSpace = 'test'
        self.cfg.cachePath = tempfile.mkdtemp()
        self.jobMaster = master.JobMaster(self.cfg)
        # ensure bootup messages don't interfere with tests
        self.jobMaster.response.response.connection.sent = []

    def tearDown(self):
        util.rmtree(self.cfg.cachePath)
        testhelp.TestCase.tearDown(self)
