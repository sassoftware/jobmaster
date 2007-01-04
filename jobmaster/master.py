#!/usr/bin/python
#
# Copyright (c) 2005-2006 rPath, Inc.
#
# All rights reserved
#


import os
import time

import simplejson
import tempfile
import threading
import weakref

from jobmaster import master_error
from jobmaster import imagecache
from jobmaster import xencfg

from mcp import queue
from mcp import response
from mcp import client

from conary import conarycfg
from conary.lib import cfgtypes
from conary.conaryclient import cmdline
from conary import versions

def getAvailableArchs(arch):
    if arch in ('i686', 'i586', 'i486', 'i386'):
        return ('x86',)
    elif arch == 'x86_64':
        return ('x86', 'x86_64')

def getIP():
    p = os.popen("""ifconfig `route | grep "^default" | sed "s/.* //"` | grep "inet addr" | awk -F: '{print $2}' | sed 's/ .*//'""")
    data = p.read().strip()
    p.close()
    return data

def controlMethod(func):
    func._controlMethod = True
    return func

PROTOCOL_VERSIONS = set([1])

filterArgs = lambda d, *args: dict([x for x in d.iteritems() \
                                        if x[0] not in args])

getJsversion = lambda troveSpec: str(versions.VersionFromString( \
        cmdline.parseTroveSpec(troveSpec)[1]).trailingRevision())

def protocols(protocolList):
    if type(protocolList) in (int, long):
        protocolList = (protocolList,)
    def deco(func):
        def wrapper(self, *args, **kwargs):
            if kwargs.get('protocolVersion') in protocolList:
                return func(self, *args,
                            **(filterArgs(kwargs, 'protocolVersion')))
            else:
                raise master_error.ProtocolError(\
                    'Unsupported ProtocolVersion: %s' % \
                        str(kwargs.get('protocolVersion')))
        return wrapper
    return deco

class MasterConfig(client.MCPClientConfig):
    cachePath = os.path.join(os.path.sep, 'srv', 'rbuilder', 'master',
                             'imageCache')
    slaveLimit = (cfgtypes.CfgInt, 1)
    nodeName = (cfgtypes.CfgString, None)

class SlaveHandler(threading.Thread):
    # A slave handler is tied to a specific slave instance. do not re-use.
    def __init__(self, master, troveSpec):
        self.master = weakref.ref(master)
        self.imageCache = weakref.ref(master.imageCache)
        self.cfgPath = ''
        self.slaveName = None
        self.troveSpec = troveSpec
        self.lock = threading.RLock()
        self.killed = False
        self.started = False
        threading.Thread.__init__(self)

    def slaveStatus(self, status):
        self.master().slaveStatus(self.slaveName, status,
                                  getJsversion(self.troveSpec))

    def start(self):
        imagePath = self.imageCache().imagePath(self.troveSpec)
        xenCfg = xencfg.XenCfg(imagePath, {'memory' : 512})
        self.slaveName = xenCfg.cfg['name']
        if not self.imageCache().haveImage(self.troveSpec):
            self.slaveStatus('building')
        fd, self.cfgPath = tempfile.mkstemp()
        os.close(fd)
        f = open(self.cfgPath, 'w')
        xenCfg.write(f)
        f.close()
        threading.Thread.start(self)
        return xenCfg.cfg['name']

    def stop(self):
        self.lock.acquire()
        try:
            self.killed = True
            # tracked started state simply prevents emitting spurious shell
            # calls to stop that which isn't running.
            if self.started:
                # fixme: use qcow image and change this call to be "destroy"
                os.system('xm shutdown %s' % self.slaveName)
        finally:
            self.lock.release()
        self.slaveStatus('stopped')

    def run(self):
        self.imageCache().getImage(self.troveSpec)
        self.lock.acquire()
        try:
            if not self.killed:
                self.started = True
                os.system('xm create %s' % self.cfgPath)
        finally:
            self.lock.release()

class JobMaster(object):
    def __init__(self, cfg):
        if cfg.nodeName is None:
            cfg.nodeName = getIP() or '127.0.0.1'
        self.cfg = cfg
        self.demandQueue = queue.MultiplexedQueue(cfg.queueHost, cfg.queuePort,
                                       namespace = cfg.namespace,
                                       timeOut = 0, queueLimit = cfg.slaveLimit)
        self.arch = os.uname()[-1]
        archs = getAvailableArchs(self.arch)
        assert archs, "Unknown machine architecture."
        for arch in archs:
            self.demandQueue.addDest('demand:' + arch)
        self.controlTopic = queue.Topic(cfg.queueHost, cfg.queuePort,
                                       'control', namespace = cfg.namespace,
                                       timeOut = 0)
        self.response = response.MCPResponse(self.cfg.nodeName, cfg)
        self.imageCache = imagecache.ImageCache(self.cfg.cachePath)
        self.slaves = {}
        self.handlers = {}
        self.sendStatus()

    # FIXME: decorate with a catchall exception logger
    def checkControlTopic(self):
        dataStr = self.controlTopic.read()
        while dataStr:
            data = simplejson.loads(dataStr)
            if data.get('node') in ('masters', self.cfg.nodeName):
                action = data['action']
                kwargs = dict([(str(x[0]), x[1]) for x in data.iteritems() \
                                   if x[0] not in ('node', 'action')])
                if action in self.__class__.__dict__:
                    func = self.__class__.__dict__[action]
                    if '_controlMethod' in func.__dict__:
                        return func(self, **kwargs)
                    else:
                        raise master_error.ProtocolError( \
                            'Control method %s is not valid' % action)
                else:
                    raise master_error.ProtocolError( \
                        "Control method %s does not exist" % action)
            dataStr = self.controlTopic.read()

    def handleSlaveStart(self, troveSpec):
        handler = SlaveHandler(self, troveSpec)
        self.handlers[handler.start()] = handler

    def handleSlaveStop(self, slaveId):
        # FIXME: this doesn't handle slaves being started...
        slaveName = slaveId.split(':')[1]
        handler = None
        if slaveName in self.slaves:
            handler = self.slaves[slaveName]
            del self.slaves[slaveName]
        elif slaveName in self.handlers:
            handler = self.handlers[slaveName]
            del self.handlers[slaveName]
        if handler:
            handler.stop()
            if (len(self.slaves) + len(self.handlers)) < self.cfg.slaveLimit:
                self.demandQueue.incrementLimit()

    # FIXME: decorate with a catchall exception logger
    def checkDemandQueue(self):
        dataStr = self.demandQueue.read()
        if dataStr:
            data = simplejson.loads(dataStr)
            if data['protocolVersion'] == 1:
                self.handleSlaveStart(data['troveSpec'])
            else:
                # FIXME: protocol problem
                # should implement some sort of error feedback to MCP
                pass

    # FIXME: decorate with a catchall exception logger
    def checkHandlers(self):
        """Move slaves from 'being started' to 'active'

        Handlers are used to start slaves. Once a handler is done starting a
        slave, we know it's active."""
        for slaveName in [x[0] for x in self.handlers.iteritems() \
                              if not x[1].isAlive()]:
            self.slaves[slaveName] = self.handlers[slaveName]
            self.slaves[slaveName].slaveStatus('running')

            del self.handlers[slaveName]

    def run(self):
        self.running = True
        try:
            while self.running:
                self.checkHandlers()
                self.checkDemandQueue()
                self.checkControlTopic()
                time.sleep(0.1)
        finally:
            self.response.masterOffline()
            self.disconnect()

    def disconnect(self):
        self.running = False
        self.demandQueue.disconnect()
        self.controlTopic.disconnect()
        del self.response

    def sendStatus(self):
        self.response.masterStatus( \
            arch = self.arch, limit = self.cfg.slaveLimit,
            slaveIds = ['%s:%s' % (self.cfg.nodeName, x) for x in self.slaves])

    def slaveStatus(self, slaveName, status, jsversion):
        self.response.slaveStatus(self.cfg.nodeName + ':' + slaveName,
                                  status, jsversion)

    def getBestProtocol(self, protocols):
        common = PROTOCOL_VERSIONS.intersection(protocols)
        return common and max(common) or 0

    @controlMethod
    def checkVersion(self, protocols):
        self.response.protocol(self.getBestProtocol(protocols))

    @controlMethod
    @protocols((1,))
    def slaveLimit(self, limit):
        # please note this is a volatile change. rebooting the master resets it
        # FIXME: find a way to save this value.
        self.cfg.slaveLimit = max(limit, 0)
        limit = max(limit - len(self.slaves) - len(self.handlers), 0)
        self.demandQueue.setLimit(limit)

    @controlMethod
    @protocols((1,))
    def clearImageCache(self):
        self.imageCache.deleteAllImages()

    @controlMethod
    @protocols((1,))
    def status(self):
        self.sendStatus()

    @controlMethod
    @protocols((1,))
    def stopSlave(self, slaveId):
        self.handleSlaveStop(slaveId)


if __name__ == '__main__':
    cfg = MasterConfig()
    cfg.nodeName = 'testMaster'
    jobMaster = JobMaster(cfg)
    jobMaster.run()
