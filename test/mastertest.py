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
import simplejson
import StringIO
import threading
import weakref

import jobmaster_helper

from jobmaster import master
from jobmaster import constants

class DummyHandler(master.SlaveHandler):
    count = 0
    jobQueueName = 'job3.0.0-1-1:x86'
    def __init__(self, master, troveSpec):
        self.master = weakref.ref(master)
        self.troveSpec = troveSpec
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

class MasterTest(jobmaster_helper.JobMasterHelper):
    def setUp(self):
        jobmaster_helper.JobMasterHelper.setUp(self)
        DummyHandler.count = 0

    def assertResponse(self, responseSent = None, **kwargs):
        if not responseSent:
            responseSent = self.jobMaster.response.response.connection.sent
        self.failIf(not responseSent,
                    "Expected response. No response was sent.")
        addr, dataStr = responseSent.pop()
        assert addr == '/topic/mcp/response', "Last sent was not a response"
        data = simplejson.loads(dataStr)
        for key, val in kwargs.iteritems():
            assert key in data, "Expected %s in response" % key
            assert data[key] == val, "expected %s of %s but got %s" % \
                (key, val, data[key])

    def insertControl(self, controlTopic = None, **kwargs):
        if not controlTopic:
            controlTopic = self.jobMaster.controlTopic
        kwargs.setdefault('node', self.cfg.nodeName)
        dataStr = simplejson.dumps(kwargs)
        controlTopic.inbound.insert(0, dataStr)

    def testBasicAttributes(self):
        assert self.jobMaster.cfg.slaveLimit == 1
        assert self.jobMaster.arch == os.uname()[-1]
        assert self.jobMaster.slaves == {}
        assert self.jobMaster.handlers == {}

    def testInitialStatus(self):
        # test that the MCP reports status during init phase
        jobMaster = master.JobMaster(self.cfg)

        self.assertResponse(responseSent = \
                                jobMaster.response.response.connection.sent,
                            node = self.cfg.nodeName,
                            limit = 1,
                            slaves = [],
                            event = "masterStatus")

    def testStatus(self):
        self.jobMaster.status(protocolVersion = 1)
        assert self.jobMaster.response.response.connection.sent

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
        assert self.jobMaster.demandQueue.queueLimit == (limit + 1)

    def testRunningSetSlaveLimit(self):
        # this is illegal, but not harmful when test case was written
        self.jobMaster.slaves['testSlave'] = None
        self.jobMaster.slaveLimit(protocolVersion = 1, limit = 2)
        self.failIf(self.jobMaster.cfg.slaveLimit != 2,
                    "Slave limit was not set to 2")
        assert self.jobMaster.demandQueue.queueLimit == 1, \
            "setting slave limit did not account for running slaves"

    def testBuildingSetSlaveLimit(self):
        # this is illegal, but not harmful when test case was written
        self.jobMaster.handlers['testSlave'] = None
        self.jobMaster.slaveLimit(protocolVersion = 1, limit = 2)
        self.failIf(self.jobMaster.cfg.slaveLimit != 2,
                    "Slave limit was not set to 2")
        assert self.jobMaster.demandQueue.queueLimit == 1, \
            "setting slave limit did not account for slaves being built"

    def testAllSetSlaveLimit(self):
        # this is illegal, but not harmful when test case was written
        self.jobMaster.slaves['testSlave'] = None
        self.jobMaster.handlers['testSlave'] = None
        self.jobMaster.slaveLimit(protocolVersion = 1, limit = 3)
        self.failIf(self.jobMaster.cfg.slaveLimit != 3,
                    "Slave limit was not set to 3")
        assert self.jobMaster.demandQueue.queueLimit == 1, \
            "setting slave limit did not account for existing slaves"

    def testSlaveLimitEdge(self):
        self.jobMaster.slaves['testSlave'] = None
        self.jobMaster.handlers['testSlave'] = None
        self.jobMaster.slaveLimit(protocolVersion = 1, limit = 1)
        self.failIf(self.jobMaster.cfg.slaveLimit != 1,
                    "Slave limit was not set to 1")
        assert self.jobMaster.demandQueue.queueLimit == 0, \
            "setting slave limit allowed negative value"

    def testNegativeSlaveLimit(self):
        self.jobMaster.slaveLimit(protocolVersion = 1, limit = -1)
        self.failIf(self.jobMaster.cfg.slaveLimit != 0,
                    "Slave limit of -1 was not adjusted to 0")

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
        dataStr = simplejson.dumps(data)
        self.jobMaster.controlTopic.inbound = [dataStr]
        self.jobMaster.checkControlTopic()
        self.assertResponse(event = 'protocol', protocolVersion = 1)

        data['node'] = self.cfg.nodeName
        dataStr = simplejson.dumps(data)
        self.jobMaster.controlTopic.inbound = [dataStr]
        self.jobMaster.checkControlTopic()

        self.assertResponse(event = 'protocol', protocolVersion = 1)


    # OBVIOUSLY NEED MORE HANDLER TESTS
    # need a test for the weakref in SlaveHandler

    def testStoppingIncrement(self):
        # test that stopping a slave when the limit has been exceeded doesn't
        # trigger a request for another.
        dummyHandler = DummyHandler(self.jobMaster,
                                    'trash=/test.rpath.local@rpl:1/1.0.0-1-1')
        self.jobMaster.handlers[dummyHandler.start()] = dummyHandler

        self.jobMaster.demandQueue.queueLimit = 0
        self.jobMaster.handleSlaveStop( \
            self.cfg.nodeName + ':' + dummyHandler.slaveName)
        assert self.jobMaster.demandQueue.queueLimit == 1

    def testHandlerStopIncrement(self):
        dummyHandler = DummyHandler(self.jobMaster,
                                    'trash=/test.rpath.local@rpl:1/1.0.0-1-1')
        self.jobMaster.handlers[dummyHandler.start()] = dummyHandler

        dummyHandler = DummyHandler(self.jobMaster,
                                    'trash=/test.rpath.local@rpl:1/1.0.0-1-1')
        self.jobMaster.handlers[dummyHandler.start()] = dummyHandler

        self.jobMaster.demandQueue.queueLimit = 0

        self.jobMaster.handleSlaveStop( \
            self.cfg.nodeName + ':' + dummyHandler.slaveName)
        assert self.jobMaster.demandQueue.queueLimit == 0

    def testSlaveStopIncrement(self):
        dummyHandler = DummyHandler(self.jobMaster,
                                    'trash=/test.rpath.local@rpl:1/1.0.0-1-1')
        self.jobMaster.slaves[dummyHandler.start()] = dummyHandler

        dummyHandler = DummyHandler(self.jobMaster,
                                    'trash=/test.rpath.local@rpl:1/1.0.0-1-1')
        self.jobMaster.slaves[dummyHandler.start()] = dummyHandler

        self.jobMaster.demandQueue.queueLimit = 0
        self.jobMaster.cfg.slaveLimit = 1

        self.jobMaster.handleSlaveStop( \
            self.cfg.nodeName + ':' + dummyHandler.slaveName)
        assert self.jobMaster.demandQueue.queueLimit == 0

        self.jobMaster.handleSlaveStop( \
            self.cfg.nodeName + ':' + self.jobMaster.slaves.keys()[0])
        assert self.jobMaster.demandQueue.queueLimit == 1

    def testStartSlave(self):
        origSlaveHandler = master.SlaveHandler
        try:
            master.SlaveHandler = DummyHandler
            self.jobMaster.handleSlaveStart( \
                'trash=/test.rpath.local@rpl:1/1.0-1-1')
        finally:
            master.SlaveHandler = origSlaveHandler

        assert not self.jobMaster.slaves
        assert self.jobMaster.handlers.keys() == ['slave0']

    def testCheckHandlers(self):
        handler = DummyHandler(self.jobMaster,
                               'trash=/test.rpath.local@rpl:1/1.0-1-1')
        self.jobMaster.handlers[handler.start()] = handler

        handler.join()

        self.jobMaster.checkHandlers()
        assert not self.jobMaster.handlers
        assert self.jobMaster.slaves.keys() == ['slave0']

    def testStopHandler(self):
        handler = DummyHandler(self.jobMaster,
                               'trash=/test.rpath.local@rpl:1/1.0-1-1')
        self.jobMaster.handlers[handler.start()] = handler
        self.jobMaster.demandQueue.queueLimit = 0
        self.jobMaster.response.response.connection.sent = []
        slaveId = self.cfg.nodeName + ':' + handler.slaveName

        handler.join()

        self.jobMaster.handleSlaveStop(slaveId)

        assert not self.jobMaster.handlers, "handler was not removed"

        self.assertResponse(status = 'stopped', node = self.cfg.nodeName,
                            slaveId = slaveId, event = 'slaveStatus')

    def testStopSlave(self):
        handler = DummyHandler(self.jobMaster,
                               'trash=/test.rpath.local@rpl:1/1.0-1-1')
        self.jobMaster.slaves[handler.start()] = handler
        self.jobMaster.demandQueue.queueLimit = 0
        self.jobMaster.response.response.connection.sent = []
        slaveId = self.cfg.nodeName + ':' + handler.slaveName
        handler.join()

        self.jobMaster.handleSlaveStop(slaveId)

        assert not self.jobMaster.slaves, "slave was not removed"

        self.assertResponse(status = 'stopped', node = self.cfg.nodeName,
                            slaveId = slaveId, event = 'slaveStatus')

    def testStopMessage(self):
        handler = DummyHandler(self.jobMaster,
                               'trash=/test.rpath.local@rpl:1/1.0-1-1')
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
                               'trash=/test.rpath.local@rpl:1/1.0-1-1')
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
        dataStr = simplejson.dumps({'action' : 'leftOutNode'})
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
        data = simplejson.loads(sent[0][1])
        refData = {"node": "testMaster", "event": "masterOffline"}
        for key, val in refData.iteritems():
            assert key in data
            assert data[key] == val

    def pipeSlaves(self, memory):
        def DummyPipe(*args, **kwargs):
            return StringIO.StringIO(memory)
        popen = os.popen
        try:
            os.popen = DummyPipe
            return master.JobMaster.getMaxSlaves(self.jobMaster)
        finally:
            os.popen = popen

    def testGetNoSlaves(self):
        slaves = self.pipeSlaves('767')
        self.failIf(slaves != 0, "expected no slaves, got: %d" % slaves)

    def testGetOneSlave(self):
        slaves = self.pipeSlaves('768')
        self.failIf(slaves != 1, "expected 1 slave, got: %d" % slaves)

    def testGetSlavesTurnover(self):
        slaves = self.pipeSlaves('1280')
        self.failIf(slaves != 2, "expected 2 slaves, got: %d" % slaves)

    def testGetSlavesBadPipe(self):
        slaves = self.pipeSlaves('NAN')
        self.failIf(slaves != 1, "expected 1 slave, got: %d" % slaves)

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


if __name__ == "__main__":
    testsuite.main()
