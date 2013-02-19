#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()

import os
import time
import json
import signal
import StringIO
import threading
import tempfile
import weakref

import jobmaster_helper

from jobmaster import master
from jobmaster import xenmac
from jobmaster import xenip

from mcp import slavestatus

from conary.lib import util

class DummyHandler(master.SlaveHandler):
    count = 0
    jobQueueName = 'job3.0.0-1-1:x86'
    def __init__(self, master, troveSpec, *others):
        # So I don't have to change every. single. instance. below here
        if len(others) == 2:
            kernelData, data = others
        elif len(others) == 1:
            kernelData = jobmaster_helper.kernelData,
            data, = others
        self.master = weakref.ref(master)
        self.troveSpec = troveSpec
        self.kernelData = kernelData
        self.lock = threading.RLock()
        self.offline = False
        threading.Thread.__init__(self)

    def start(self):
        self.slaveName = "slave%d" % DummyHandler.count
        DummyHandler.count += 1
        self.slaveStatus('building')
        threading.Thread.start(self)
        return self.slaveName

    def run(self):
        pass

    def stop(self):
        self.slaveStatus('stopped')

    def isOnline(self):
        return not self.offline

class MasterTest(jobmaster_helper.JobMasterHelper):
    def setUp(self):
        jobmaster_helper.JobMasterHelper.setUp(self)
        DummyHandler.count = 0

    def assertResponse(self, responseSent = None, **kwargs):
        if not responseSent:
            responseSent = self.jobMaster.response.response.connection.sent
        self.failIf(not responseSent,
                    "Expected response. No response was sent.")
        addr, dataStr = responseSent.pop(0)
        assert addr == '/topic/mcp/response', "Last sent was not a response"
        data = json.loads(dataStr)
        for key, val in kwargs.iteritems():
            assert key in data, "Expected %s in response" % key
            assert data[key] == val, "expected %s of %s but got %s" % \
                (key, val, data[key])

    def insertControl(self, controlTopic = None, **kwargs):
        if not controlTopic:
            controlTopic = self.jobMaster.controlTopic
        kwargs.setdefault('node', self.cfg.nodeName)
        dataStr = json.dumps(kwargs)
        controlTopic.inbound.insert(0, dataStr)

    def testBasicAttributes(self):
        assert self.jobMaster.cfg.slaveLimit == 1
        assert self.jobMaster.arch == os.uname()[-1]
        assert self.jobMaster.slaves == {}
        assert self.jobMaster.handlers == {}

    def testInitialStatus(self):
        # test that the MCP reports status during init phase
        jobMaster = master.JobMaster(self.cfg)
        savedTime = time.time
        try:
            jobMaster.checkSlaveCount = lambda: None
            time.time = lambda: 300
            jobMaster.heartbeat()
        finally:
            time.time = savedTime

        self.assertResponse(responseSent = \
                                jobMaster.response.response.connection.sent,
                            node = self.cfg.nodeName,
                            limit = 1,
                            slaves = [],
                            event = "masterStatus")
        self.failIf(jobMaster.lastHeartbeat != 300,
                "timestamp of heartbeat was not saved")

    def testStatus(self):
        self.jobMaster.status(protocolVersion = 1)
        assert self.jobMaster.response.response.connection.sent
        dataStr = self.jobMaster.response.response.connection.sent.pop(0)[1]
        data = json.loads(dataStr)
        self.failIf(data['event'] != 'masterStatus')

    def testDownStatus(self):
        troveSpec = 'test=test.rpath.local@rpl:1[is: x86]'
        self.jobMaster.handlers['testSlave'] = \
                DummyHandler(self.jobMaster, troveSpec, {})
        self.jobMaster.handlers['testSlave'].offline = True
        self.jobMaster.handlers['testSlave2'] = \
                DummyHandler(self.jobMaster, troveSpec, {})
        self.jobMaster.status(protocolVersion = 1)
        assert self.jobMaster.response.response.connection.sent
        dataStr = self.jobMaster.response.response.connection.sent.pop(0)[1]
        data = json.loads(dataStr)
        self.failIf(data['event'] != 'masterStatus')
        refSlaves = ['testMaster:testSlave2']
        self.failIf(data['slaves'] != refSlaves, \
                "Expected %s, but found %s" % \
                (str(data['slaves']), str(refSlaves)))

    def testClearImageCache(self):
        cachePath = os.path.join(self.cfg.basePath, 'imageCache')
        assert not os.listdir(cachePath)
        f = open(os.path.join(cachePath, 'test'), 'w')
        f.close()
        assert os.listdir(cachePath) == ['test']
        self.jobMaster.clearImageCache(protocolVersion = 1)
        self.failIf(os.listdir(cachePath),
                    "clearImageCache did not delete images")

    def testBasicSetSlaveLimit(self):
        limit = self.jobMaster.cfg.slaveLimit
        self.jobMaster.slaveLimit(protocolVersion = 1, limit = limit + 1)
        self.failIf(self.jobMaster.cfg.slaveLimit != limit + 1,
                    "Slave limit was not adjusted from %d to %d" % \
                        (limit, limit + 1))
        assert self.jobMaster.jobQueue.queueLimit == (limit + 1)

    def testSetSlaveLimitError(self):
        # this code ends up testing two things, that the slave limit is
        # honored, and also that checkSlaveLimit will reduce the demand count
        # as well as increase it.
        limit = self.jobMaster.cfg.slaveLimit
        getMaxSlaves = self.jobMaster.getMaxSlaves
        try:
            self.jobMaster.getMaxSlaves = lambda *args, **kwargs: 2
            self.jobMaster.slaveLimit(protocolVersion = 1, limit = 0)
        finally:
            self.jobMaster.getMaxSlaves = getMaxSlaves
        self.failIf(self.jobMaster.cfg.slaveLimit,
                "Expected a slaveLimit of 0, but got %d" % \
                        self.jobMaster.cfg.slaveLimit)
        assert self.jobMaster.jobQueue.queueLimit == 0

    def testSetSlaveLimitExceeded(self):
        # this code ends up testing two things, that the slave limit is
        # honored, and also that checkSlaveLimit will reduce the demand count
        # as well as increase it.
        limit = self.jobMaster.cfg.slaveLimit
        getMaxSlaves = self.jobMaster.getMaxSlaves
        try:
            self.jobMaster.getMaxSlaves = lambda *args, **kwargs: 1
            self.jobMaster.slaveLimit(protocolVersion = 1, limit = 2)
        finally:
            self.jobMaster.getMaxSlaves = getMaxSlaves
        self.failIf(self.jobMaster.cfg.slaveLimit != 1,
                "Expected a slaveLimit of 1, but got %d" % \
                        self.jobMaster.cfg.slaveLimit)
        assert self.jobMaster.jobQueue.queueLimit == 1

    def testRunningSetSlaveLimit(self):
        # this is illegal, but not harmful when test case was written
        self.jobMaster.slaves['testSlave'] = None
        self.jobMaster.slaveLimit(protocolVersion = 1, limit = 2)
        self.failIf(self.jobMaster.cfg.slaveLimit != 2,
                    "Slave limit was not set to 2")
        assert self.jobMaster.jobQueue.queueLimit == 1, \
            "setting slave limit did not account for running slaves"

    def testBuildingSetSlaveLimit(self):
        # this is illegal, but not harmful when test case was written
        troveSpec = 'test=test.rpath.local@rpl:1[is: x86]'
        self.jobMaster.handlers['testSlave'] = \
                DummyHandler(self.jobMaster, troveSpec, {})
        self.jobMaster.slaveLimit(protocolVersion = 1, limit = 2)
        self.failIf(self.jobMaster.cfg.slaveLimit != 2,
                    "Slave limit was not set to 2")
        assert self.jobMaster.jobQueue.queueLimit == 1, \
            "setting slave limit did not account for slaves being built"

    def testAllSetSlaveLimit(self):
        # this is illegal, but not harmful when test case was written
        self.jobMaster.slaves['testSlave'] = None
        troveSpec = 'test=test.rpath.local@rpl:1[is: x86]'
        self.jobMaster.handlers['testSlave'] = \
                DummyHandler(self.jobMaster, troveSpec, {})
        self.jobMaster.slaveLimit(protocolVersion = 1, limit = 3)
        self.failIf(self.jobMaster.cfg.slaveLimit != 3,
                    "Slave limit was not set to 3")
        assert self.jobMaster.jobQueue.queueLimit == 1, \
            "setting slave limit did not account for existing slaves"

    def testSlaveLimitEdge(self):
        self.jobMaster.slaves['testSlave'] = None
        troveSpec = 'test=test.rpath.local@rpl:1[is: x86]'
        self.jobMaster.handlers['testSlave'] = \
                DummyHandler(self.jobMaster, troveSpec, {})
        self.jobMaster.slaveLimit(protocolVersion = 1, limit = 1)
        self.failIf(self.jobMaster.cfg.slaveLimit != 1,
                    "Slave limit was not set to 1")

    def testNegativeSlaveLimit(self):
        self.jobMaster.slaveLimit(protocolVersion = 1, limit = -1)
        self.failIf(self.jobMaster.cfg.slaveLimit != 0,
                    "Slave limit of -1 was not adjusted to 0")

    def testCheckSlaveCount(self):
        # test that slave count checks account for slaves currently being
        # built when determining the proper number to ask for.
        self.jobMaster.slaves['test1'] = None
        self.jobMaster.slaves['test2'] = None
        self.jobMaster.handlers['test3'] = None
        realSlaveLimit = self.jobMaster.realSlaveLimit
        try:
            self.jobMaster.realSlaveLimit = lambda: 2
            self.jobMaster.jobQueue.setLimit(0)
            self.jobMaster.cfg.slaveLimit = 5
            self.jobMaster.checkSlaveCount()
            self.failIf(self.jobMaster.jobQueue.queueLimit != 1,
                    "Expected slave limit to be corrected to 1")
        finally:
            self.jobMaster.realSlaveLimit = realSlaveLimit

    def testBestProtocol(self):
        assert self.jobMaster.getBestProtocol(protocols = []) == 0
        assert self.jobMaster.getBestProtocol(protocols = [0]) == 0
        assert self.jobMaster.getBestProtocol(protocols = [1]) == 1
        saveProtocols = master.PROTOCOL_VERSIONS
        try:
            master.PROTOCOL_VERSIONS = set(range(6))
            assert self.jobMaster.getBestProtocol(protocols = range(3,16)) == 5
        finally:
            master.PROTOCOL_VERSIONS = saveProtocols

    def testCheckVersion(self):
        self.jobMaster.checkVersion(protocols = [])
        self.assertResponse(event = 'protocol', protocolVersion = 0)
        self.jobMaster.checkVersion(protocols = [0])
        self.assertResponse(event = 'protocol', protocolVersion = 0)
        self.jobMaster.checkVersion(protocols = [1]) == 1
        self.assertResponse(event = 'protocol', protocolVersion = 1)

    def testMasterAddressing(self):
        # fixme. figure out a way to test that nothing happened
        self.jobMaster.checkControlTopic()
        data = {}
        data['node'] = 'masters'
        data['action'] = 'checkVersion'
        data['protocols'] = [1]
        dataStr = json.dumps(data)
        self.jobMaster.controlTopic.inbound = [dataStr]
        self.jobMaster.checkControlTopic()
        self.assertResponse(event = 'protocol', protocolVersion = 1)

        data['node'] = self.cfg.nodeName
        dataStr = json.dumps(data)
        self.jobMaster.controlTopic.inbound = [dataStr]
        self.jobMaster.checkControlTopic()

        self.assertResponse(event = 'protocol', protocolVersion = 1)


    # OBVIOUSLY NEED MORE HANDLER TESTS
    # need a test for the weakref in SlaveHandler

    def testStoppingIncrement(self):
        # test that stopping a slave when the limit has been exceeded doesn't
        # trigger a request for another.
        dummyHandler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0.0-1-1', {})
        self.jobMaster.handlers[dummyHandler.start()] = dummyHandler

        self.jobMaster.jobQueue.queueLimit = 0
        self.jobMaster.handleSlaveStop( \
            self.cfg.nodeName + ':' + dummyHandler.slaveName)
        assert self.jobMaster.jobQueue.queueLimit == 1

    def testHandlerStopIncrement(self):
        dummyHandler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0.0-1-1', {})
        self.jobMaster.handlers[dummyHandler.start()] = dummyHandler

        dummyHandler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0.0-1-1', {})
        self.jobMaster.handlers[dummyHandler.start()] = dummyHandler

        self.jobMaster.jobQueue.queueLimit = 0

        self.jobMaster.handleSlaveStop( \
            self.cfg.nodeName + ':' + dummyHandler.slaveName)
        assert self.jobMaster.jobQueue.queueLimit == 0

    def testSlaveStopIncrement(self):
        dummyHandler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0.0-1-1', {})
        self.jobMaster.slaves[dummyHandler.start()] = dummyHandler

        dummyHandler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0.0-1-1', {})
        self.jobMaster.slaves[dummyHandler.start()] = dummyHandler

        self.jobMaster.jobQueue.queueLimit = 0
        self.jobMaster.cfg.slaveLimit = 1

        self.jobMaster.handleSlaveStop( \
            self.cfg.nodeName + ':' + dummyHandler.slaveName)
        assert self.jobMaster.jobQueue.queueLimit == 0

        self.jobMaster.handleSlaveStop( \
            self.cfg.nodeName + ':' + self.jobMaster.slaves.keys()[0])
        assert self.jobMaster.jobQueue.queueLimit == 1

    def testStartSlave(self):
        origSlaveHandler = master.SlaveHandler
        try:
            master.SlaveHandler = DummyHandler
            self.jobMaster.handleSlaveStart( \
                {'jobSlaveNVF' : 'trash=/test.rpath.local@rpl:1/1.0-1-1'})
        finally:
            master.SlaveHandler = origSlaveHandler

        assert not self.jobMaster.slaves
        assert self.jobMaster.handlers.keys() == ['slave0']

    def testCheckHandlers(self):
        handler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0-1-1', {})
        self.jobMaster.handlers[handler.start()] = handler

        handler.join()

        self.jobMaster.checkHandlers()
        assert not self.jobMaster.handlers
        assert self.jobMaster.slaves.keys() == ['slave0']

    def testStopHandler(self):
        handler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0-1-1', {})
        self.jobMaster.handlers[handler.start()] = handler
        self.jobMaster.jobQueue.queueLimit = 0
        self.jobMaster.response.response.connection.sent = []
        slaveId = self.cfg.nodeName + ':' + handler.slaveName

        handler.join()

        self.jobMaster.handleSlaveStop(slaveId)

        assert not self.jobMaster.handlers, "handler was not removed"

        self.assertResponse(status = 'stopped', node = self.cfg.nodeName,
                            slaveId = slaveId, event = 'slaveStatus')

    def testStopSlave(self):
        handler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0-1-1', {})
        self.jobMaster.slaves[handler.start()] = handler
        self.jobMaster.jobQueue.queueLimit = 0
        self.jobMaster.response.response.connection.sent = []
        slaveId = self.cfg.nodeName + ':' + handler.slaveName
        handler.join()

        self.jobMaster.handleSlaveStop(slaveId)

        assert not self.jobMaster.slaves, "slave was not removed"

        self.assertResponse(status = 'stopped', node = self.cfg.nodeName,
                            slaveId = slaveId, event = 'slaveStatus')

    def testStopMessage(self):
        handler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0-1-1', {})
        self.jobMaster.slaves[handler.start()] = handler
        handler.join()
        self.jobMaster.response.response.connection.sent = []
        slaveId = self.cfg.nodeName + ':' + handler.slaveName
        self.insertControl(action = 'stopSlave', slaveId = slaveId,
                           protocolVersion = 1)

        self.jobMaster.checkControlTopic()

        assert not self.jobMaster.slaves, "slave was not removed"

        self.assertResponse(status = 'stopped', node = self.cfg.nodeName,
                            slaveId = slaveId, event = 'slaveStatus')

    def testStatusMessage(self):
        self.insertControl(action = 'status', protocolVersion = 1)
        self.jobMaster.checkControlTopic()

        self.assertResponse(node = self.cfg.nodeName,
                            limit = self.cfg.slaveLimit,
                            slaves = [],
                            event = "masterStatus")

        handler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0-1-1', {})
        self.jobMaster.slaves[handler.start()] = handler
        handler.join()
        slaveId = self.cfg.nodeName + ':' + handler.slaveName
        self.jobMaster.response.response.connection.sent = []

        self.insertControl(action = 'status', protocolVersion = 1)
        self.jobMaster.checkControlTopic()

        self.assertResponse(node = self.cfg.nodeName,
                            limit = self.cfg.slaveLimit,
                            slaves = [slaveId],
                            event = "masterStatus")

    def testCacheMessage(self):
        cachePath = os.path.join(self.cfg.basePath, 'imageCache')
        f = open(os.path.join(cachePath, 'fakeImage'), 'w')
        f.write('')
        f.close()

        self.insertControl(action = 'clearImageCache', protocolVersion = 1)
        self.jobMaster.checkControlTopic()

        assert not os.listdir(cachePath), \
            "test image was not removed by clearImageCache"

    def testCheckVersionMessage(self):
        self.insertControl(action = "checkVersion", protocols = [])
        self.jobMaster.checkControlTopic()
        self.assertResponse(protocolVersion = 0)

    def testInvalidMessage(self):
        self.insertControl(node = self.cfg.nodeName, action = 'notARealMessage')
        self.jobMaster.checkControlTopic()
        self.assertLogContent("Control method 'notARealMessage' does not exist")

    def testUnimplementedMessage(self):
        # insert an action which really is a method of the class, but not
        # flagged as callable
        self.insertControl(node = self.cfg.nodeName,
                           action = 'checkControlTopic')
        self.jobMaster.checkControlTopic()
        self.assertLogContent(\
            "Action 'checkControlTopic' is not a control method")

    def testBadNode(self):
        dataStr = json.dumps({'action' : 'leftOutNode'})
        self.jobMaster.controlTopic.inbound.insert(0, dataStr)

        # test that this control command is ignored.
        self.jobMaster.checkControlTopic()
        assert not self.jobMaster.controlTopic.inbound

    def testMissingParams(self):
        self.insertControl(action = "checkVersion")
        self.jobMaster.checkControlTopic()
        self.assertLogContent( \
            'checkVersion() takes exactly 2 arguments (1 given)')

    def testOfflineMessage(self):
        jobMaster = jobmaster_helper.ThreadedJobMaster(self.cfg)
        jobMaster.start()
        jobMaster.response.response.connection.sent = []
        assert jobMaster.isAlive()
        sent = jobMaster.response.response.connection.sent
        while 'running' not in jobMaster.__dict__:
            time.sleep(0.1)
        jobMaster.running = False
        jobMaster.join()
        assert sent, "no response was sent"
        data = json.loads(sent.pop()[1]) # We want the last message sent
        refData = {"node": "testMaster", "event": "masterOffline"}
        for key, val in refData.iteritems():
            assert key in data
            self.assertEquals( data[key], val)

    def pipeSlaves(self, memory, func = master.JobMaster.getMaxSlaves):
        self.memList = memory[:]
        def DummyPipe(*args, **kwargs):
            return StringIO.StringIO(self.memList.pop())
        popen = os.popen
        try:
            os.popen = DummyPipe
            return func(self.jobMaster)
        finally:
            os.popen = popen

    def testGetNoSlaves(self):
        slaves = self.pipeSlaves(['512', '512'])
        self.failIf(slaves != 0, "expected no slaves, got: %d" % slaves)

    def testGetOneSlave(self):
        slaves = self.pipeSlaves(['512', '1024'])
        self.failIf(slaves != 1, "expected 1 slave, got: %d" % slaves)

    def testGetMaxSlaves(self):
        self.jobMaster.cfg.maxSlaveLimit = 2
        slaves = self.pipeSlaves(['512', '4096'])
        self.assertEquals(slaves, 2)

        self.jobMaster.cfg.maxSlaveLimit = 0
        slaves = self.pipeSlaves(['512', '4095'])
        self.assertEquals(slaves, 6)

    def testGetSlavesTurnover(self):
        slaves = self.pipeSlaves(['512', '1280'])
        self.failIf(slaves != 1, "expected 1 slaves, got: %d" % slaves)

    def testGetSlavesBadPipe(self):
        slaves = self.pipeSlaves(['NAN', 'NAN'])
        self.failIf(slaves != 0, "expected 0 slaves, got: %d" % slaves)

    def testGetRealNoSlaves(self):
        slaves = self.pipeSlaves(['512', '511'], func = master.JobMaster.realSlaveLimit)
        self.failIf(slaves != 0, "expected no slaves, got: %d" % slaves)

    def testGetRealOneSlave(self):
        slaves = self.pipeSlaves(['512', '1088'],
                func = master.JobMaster.realSlaveLimit)
        self.failIf(slaves != 1, "expected 1 slave, got: %d" % slaves)

    def testGetRealMaxSlaves(self):
        self.jobMaster.cfg.maxSlaveLimit = 2
        slaves = self.pipeSlaves(['512', '4096'],
                func = master.JobMaster.realSlaveLimit)
        self.assertEquals(slaves, 2)

        self.jobMaster.cfg.maxSlaveLimit = 0
        slaves = self.pipeSlaves(['512', '4096'],
                func = master.JobMaster.realSlaveLimit)
        self.assertEquals(slaves, 6)

    def testGetRealSlavesTurnover(self):
        slaves = self.pipeSlaves(['512', '1600'],
                func = master.JobMaster.realSlaveLimit)
        self.failIf(slaves != 2, "expected 2 slaves, got: %d" % slaves)

    def testGetRealSlavesBadPipe(self):
        slaves = self.pipeSlaves(['NAN', 'NAN'],
                func = master.JobMaster.realSlaveLimit)
        self.failIf(slaves != 0, "expected 0 slaves, got: %d" % slaves)

    def testMissingSlave(self):
        def DummyPipe(*args, **kwargs):
            return StringIO.StringIO('slave name does not appear in output')

        popen = os.popen
        try:
            os.popen = DummyPipe
            # just enough to cause the jobMaster to check it's slave list
            self.insertControl(node = self.cfg.nodeName + ":voodoo")

            self.jobMaster.checkControlTopic()
            self.assertLogContent('Detected missing slave')
        finally:
            os.popen = popen

    def testNodeName(self):
        nodeName = self.cfg.nodeName
        try:
            self.cfg.nodeName = None
            master.JobMaster(self.cfg)
            self.failIf(self.cfg.nodeName is None,
                        "Master did not set it's nodeName")
        finally:
            self.cfg.nodeName = nodeName

    def testStopAllSlaves(self):
        dummyHandler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0.0-1-1', {})
        self.jobMaster.handlers[dummyHandler.start()] = dummyHandler
        dummyHandler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0.0-1-1', {})
        self.jobMaster.slaves[dummyHandler.start()] = dummyHandler

        self.jobMaster.stopAllSlaves()
        self.failIf(self.jobMaster.handlers,
                "handlers were not cleared upon jobmaster stop")
        self.failIf(self.jobMaster.slaves,
                "slaves were not cleared upon jobmaster stop")

    def testCheckSlaves(self):
        dummyHandler = DummyHandler(self.jobMaster,
            'trash=/test.rpath.local@rpl:1/1.0.0-1-1', {})
        self.jobMaster.slaves[dummyHandler.start()] = dummyHandler
        slaves = self.jobMaster.slaves.keys()
        class MockPipe(object):
            def readlines(x):
                return []
        def mockStop(slaveId):
            self.stoppedSlaves.append(slaveId)
        self.stoppedSlaves = []
        popen = os.popen
        handleSlaveStop = self.jobMaster.handleSlaveStop
        try:
            os.popen = lambda x: MockPipe()
            self.jobMaster.handleSlaveStop = mockStop
            self.jobMaster.checkSlaves()
        finally:
            os.popen = popen
            self.jobMaster.handleSlaveStop = handleSlaveStop
        self.failIf(self.stoppedSlaves != slaves,
                'expected slaves to be stopped: %s' % str(slaves))

    def testWaitForSlave(self):
        slaveId = 'slave00'
        LVM_PATH = master.LVM_PATH
        NEW_LVM_PATH = tempfile.mkdtemp()
        popen = os.popen
        class DummyPipe(object):
            def __init__(x, retval):
                x.retval = retval
            def read(x):
                return x.retval
        def MockPopen(cmd):
            self.count += 1
            return DummyPipe((self.count % 2) and \
                '/dev/vg00/slave00:vg00:3:1:-1:1:4784128:73:-1:0:0:253:4' or '')
        self.count = 0
        try:
            master.LVM_PATH = NEW_LVM_PATH
            f = open(os.path.join(NEW_LVM_PATH, 'vg00-%s' % slaveId), 'w')
            f.write('')
            f.close()
            os.popen = MockPopen
            master.waitForSlave(slaveId)
            self.failIf(self.count != 2, "expected popen to be called twice")
        finally:
            util.rmtree(NEW_LVM_PATH)
            LVM_PATH = LVM_PATH
            os.popen = popen

    def testBadStatusReport(self):
        raise testsuite.SkipTestException("Test isn't needed as the respawn functionality is unused")
        def FakeHeartbeat(*args, **kwargs):
            # set up running to be a simple countdown
            if self.jobMaster.running is True:
                self.jobMaster.running = 2
            self.jobMaster.running -= 1
            return self.jobMaster._heartbeat(*args, **kwargs)

        def FakeHandlerRun(x):
            x.slaveStatus(slavestatus.OFFLINE)

        jobData = json.dumps({'protocolVersion': 1,
            'UUID' : 'test.rpath.local-build-42-3',
            'jobSlaveNVF' : 'jobslave=test.rpath.local@rpl:1[is: x86]'})
        sleep = master.time.sleep
        getJobQueueName = master.SlaveHandler.getJobQueueName
        genMac = xenmac.genMac
        genIP = xenip.genIP
        handlerRun = master.SlaveHandler.run
        try:
            master.SlaveHandler.run = FakeHandlerRun
            xenip.genIP = lambda *args, **kwargs: '10.5.6.1'
            xenmac.genMac = lambda *args, **kwargs: '00:16:3e:00:01:01'
            master.SlaveHandler.getJobQueueName = lambda *args, **kwargs: \
                    "job4.0.0:x86"
            self.jobMaster._heartbeat = self.jobMaster.heartbeat
            self.jobMaster.heartbeat = FakeHeartbeat
            time.sleep = lambda *args, **kwargs: None
            resp = self.jobMaster.response
            self.jobMaster.jobQueue.inbound = [jobData]
            self.captureOutput(self.jobMaster.run)
            state = 'testStart'
            respawnCount = 0
            for message in [x[1] for x in \
                    reversed(resp.response.connection.sent)]:
                data = json.loads(message)
                if data['event'] == 'slaveStatus':
                    if data['status'] != slavestatus.OFFLINE:
                        state = 'up'
                    else:
                        if state == 'up':
                            respawnCount += 1
                        state = 'down'
                elif data['event'] == 'masterOffline':
                    if state == 'up':
                        respawnCount += 1
                    state = 'down'
                elif data['event'] == 'masterStatus':
                    if 'testMaster:slave01' in data['slaves']:
                        state = 'up'
                    else:
                        if state == 'up':
                            respawnCount += 1
                        state = 'down'
            self.failIf(respawnCount != 1, \
                    'expected 1 respawn, but observed: %d' % respawnCount)
        finally:
            master.SlaveHandler.run = handlerRun
            xenip.genIP = genIP
            xenmac.genMac = genMac
            master.SlaveHandler.getJobQueueName = getJobQueueName 
            master.time.sleep = sleep

    def testHandlerOnline(self):
        class FakeHandler(master.SlaveHandler):
            def __init__(x, offline):
                x.offline = offline
                x.lock = threading.RLock()

        self.assertEquals(FakeHandler(True).isOnline(), False)
        self.assertEquals(FakeHandler(False).isOnline(), True)

    def testFailedHandlerRun(self):
        class FakeHandler(master.SlaveHandler):
            def __init__(x):
                x.offline = False
                x.lock = threading.RLock()
            def slaveStatus(x, status):
                x.lastStatus = status

        hdlr = FakeHandler()
        fork = os.fork
        _exit = os._exit
        setpgid = os.setpgid
        waitpid = os.waitpid
        try:
            os.setpgid = lambda *args: None
            os.fork = lambda: 0
            os._exit = lambda *args, **kwargs: None
            os.waitpid = lambda *args, **kwargs: None
            hdlr.run()
            self.assertEquals(hdlr.lastStatus, slavestatus.OFFLINE)
            self.assertEquals(hdlr.offline, True)
            self.failIf(hdlr.pid is not None, "expected None, but got: '%s'" \
                    % str(hdlr.pid))
        finally:
            os.waitpid = waitpid
            os.setpgid = setpgid
            os._exit = _exit
            os.fork = fork

    def testFlushJobs(self):
        jobData = json.dumps({'protocolVersion': 1,
            'UUID' : 'test.rpath.local-build-88-0',
            'jobSlaveNVF' : 'jobslave=test.rpath.local@rpl:1[is: x86]'})
        self.jobMaster.jobQueue.inbound = [jobData]
        self.jobMaster.flushJobs()
        for status in (slavestatus.BUILDING, slavestatus.OFFLINE):
            addy, event = \
                    self.jobMaster.response.response.connection.sent.pop(0)
            data = json.loads(event)
            self.assertEquals(data.get('event'), 'slaveStatus')
            self.assertEquals(data.get('jobId'), 'test.rpath.local-build-88-0')
            self.assertEquals(data.get('slaveId'), 'testMaster:deadSlave0')
            self.assertEquals(data.get('status'), status)

    def testFlushJobProtocols(self):
        jobData = json.dumps({'protocolVersion': -1,
            'UUID' : 'test.rpath.local-build-88-4',
            'jobSlaveNVF' : 'jobslave=test.rpath.local@rpl:1[is: x86]'})
        self.jobMaster.jobQueue.inbound = [jobData]
        self.jobMaster.flushJobs()
        self.assertEquals(self.jobMaster.response.response.connection.sent, [])

    def testCatchSignal(self):
        self.jobMaster.catchSignal(signal.SIGTERM, None)
        self.assertEquals(self.jobMaster.running, False)

    def testEstimateScratchSize(self):
        class ScratchHandler(master.SlaveHandler):
            def __init__(self, data, master):
                self.data = data
                self.master = weakref.ref(master)

        jobData = {'type': 'build', 'protocolVersion': 1,
                'data' : {'freespace' : 750, 'swapSize' : 250}}
        hdlr = ScratchHandler(jobData, self.jobMaster)
        hdlr.getTroveSize = lambda: 1024 * 1024 * 1024
        self.assertEquals(hdlr.estimateScratchSize(), 9398)
        jobData['type'] = 'cook'
        self.assertEquals(hdlr.estimateScratchSize(), 1024)

    def testEstimateMinScratchSize(self):
        class ScratchHandler(master.SlaveHandler):
            def __init__(self, data, master):
                self.data = data
                self.master = weakref.ref(master)

        jobData = {'type': 'build', 'protocolVersion': 1,
                'data' : {'freespace' : 0, 'swapSize' : 0}}
        hdlr = ScratchHandler(jobData, self.jobMaster)
        hdlr.getTroveSize = lambda: 5 * 1024 * 1024
        self.assertEquals(hdlr.estimateScratchSize(), 1024)


    def testEstimateScratchSizeLive(self):
        raise testsuite.SkipTestException("This test uses external repositories, and is used as a sanity check (see RBL-3599)")
        class ScratchHandler(master.SlaveHandler):
            def __init__(self, data):
                self.data = data

        jobData = {'type': 'build', 'protocolVersion': 1,
                'data' : {'freespace' : 256, 'swapSize' : 128}, 'project': {'conaryCfg': ''}}
        jobData['troveName'] = 'group-weasel-appliance'
        jobData['troveVersion'] = '/weasel.rpath.org@wgl:weasel-2.0-devel/0:2.0.1-13-1'
        jobData['troveFlavor'] = '1#x86:i486:i586:i686:sse:sse2|1#x86_64|5#use:~!dom0:~!domU:~!vmware:~!xen'
        hdlr = ScratchHandler(jobData)
        size = hdlr.estimateScratchSize()
        self.assertEquals(size, 29480)

if __name__ == "__main__":
    testsuite.main()
