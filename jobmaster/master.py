#!/usr/bin/python
#
# Copyright (c) 2005-2006 rPath, Inc.
#
# All rights reserved
#


import os, sys
import math
import simplejson
import tempfile
import threading
import time
import signal
import weakref

from jobmaster import master_error
from jobmaster import imagecache
from jobmaster import xencfg

from mcp import queue
from mcp import response
from mcp import client

from conary.lib import cfgtypes, util
from conary import conaryclient
from conary.conaryclient import cmdline
from conary.deps import deps
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
    cachePath = os.path.join(os.path.sep, 'srv', 'rbuilder', 'jobmaster',
                             'imageCache')
    slaveLimit = (cfgtypes.CfgInt, 1)
    nodeName = (cfgtypes.CfgString, None)
    slaveMemory = (cfgtypes.CfgInt, 512) # memory in MB

class SlaveHandler(threading.Thread):
    # A slave handler is tied to a specific slave instance. do not re-use.
    def __init__(self, master, troveSpec):
        self.master = weakref.ref(master)
        self.imageCache = weakref.ref(master.imageCache)
        self.cfgPath = ''
        self.slaveName = None
        self.troveSpec = troveSpec
        self.lock = threading.RLock()
        threading.Thread.__init__(self)
        self.pid = None

    def slaveStatus(self, status):
        self.master().slaveStatus(self.slaveName, status,
                                  getJsversion(self.troveSpec))

    def start(self):
        fd, self.imagePath = tempfile.mkstemp()
        os.close(fd)
        xenCfg = xencfg.XenCfg(self.imagePath,
                               {'memory' : self.master().cfg.slaveMemory})
        self.slaveName = xenCfg.cfg['name']
        self.slaveStatus('building')
        fd, self.cfgPath = tempfile.mkstemp()
        os.close(fd)
        f = open(self.cfgPath, 'w')
        xenCfg.write(f)
        f.close()
        threading.Thread.start(self)
        return xenCfg.cfg['name']

    def stop(self):
        pid = self.pid
        if pid:
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError, e:
                # ignore race condition where child died right after we recorded
                # it's pid
                if errno != 3:
                    raise
        os.system('xm destroy %s' % self.slaveName)
        if os.path.exists(self.imagePath):
            util.rmtree(self.imagePath, ignore_errors = True)
        self.slaveStatus('stopped')
        self.join()

    def getJobQueueName(self):
        name, verStr, flv = cmdline.parseTroveSpec(self.troveSpec)
        ver = versions.VersionFromString(verStr)
        jsVersion = str(ver.trailingRevision())

        arch = 'unknown'
        for refFlv, refArch in (('1#x86_64', 'x86_64'), ('1#x86', 'x86')):
            if flv.satisfies(deps.ThawFlavor(refFlv)):
                arch = refArch
                break
        return 'job%s:%s' % (jsVersion, arch)

    def run(self):
        self.pid = os.fork()
        if not self.pid:
            os.setsid()
            try:
                # don't use original. make a backup
                cachedImage = self.imageCache().getImage(self.troveSpec)
                util.copyfile(cachedImage, self.imagePath)
                # now add per-instance settings. such as path to MCP
                mntPoint = tempfile.mkdtemp()
                try:
                    os.system('mount -o loop %s %s' % (self.imagePath, mntPoint))
                    cfg = self.master().cfg
                    cfgPath = os.path.join(mntPoint, 'srv', 'jobslave', 'config.d',
                                          'runtime')
                    util.mkdirChain(os.path.split(cfgPath)[0])
                    f = open(cfgPath, 'w')
                    f.write('queueHost %s\n' % cfg.queueHost)
                    f.write('queuePort %s\n' % str(cfg.queuePort))
                    f.write('nodeName %s\n' % ':'.join((cfg.nodeName,
                                                        self.slaveName)))
                    f.write('jobQueueName %s\n' % self.getJobQueueName())
                    f.close()
                    util.copytree(os.path.join(os.path.sep, 'srv', 'rbuilder',
                                               'entitlements'),
                                  os.path.join(mntPoint, 'srv', 'jobslave'))
                finally:
                    os.system('umount %s' % mntPoint)
                    util.rmtree(mntPoint, ignore_errors = True)

                os.system('xm create %s' % self.cfgPath)
            except:
                sys.exit(1)
            else:
                sys.exit(0)
        os.waitpid(self.pid, 0)
        self.pid = None

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

    def getMaxSlaves(self):
        # this function is desgined for xen. if we extend to remote slaves
        # such as EC2 it will need reworking.
        p = os.popen('xm info | grep total_memory | sed "s/.* //"')
        mem = p.read()
        if mem:
            mem = int(mem)
        else:
            return 1
        #p = os.popen("free -b | grep '^Mem:' | awk '{print $2;}'")
        #mem = int(p.read()) / (1024 * 1024)
        p.close()
        count = mem / self.cfg.slaveMemory
        # Enforce that the master has at least as much memory as half a slave.
        if (mem % self.cfg.slaveMemory) < (self.cfg.slaveMemory / 2):
            count -= 1
        return count

    def resolveTroveSpec(self, troveSpec):
        # this function is designed to ensure a partial NVF can be resolved to
        # a full NVF for caching and creation purposes.
        cc = conaryclient.ConaryClient()
        n, v, f = cmdline.parseTroveSpec(troveSpec)
        troves = cc.repos.findTrove( \
            None, (n, v, f))
        troves = [x for x in troves \
                      if x[2].stronglySatisfies(deps.parseFlavor('xen,domU'))]
        if troves[0][2] is None:
            troves[0][2] == ''
        return '%s=%s[%s]' % troves[0]

    def handleSlaveStart(self, troveSpec):
        troveSpec = self.resolveTroveSpec(troveSpec)
        handler = SlaveHandler(self, troveSpec)
        self.handlers[handler.start()] = handler

    def handleSlaveStop(self, slaveId):
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
        self.sendStatus()

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
            slaveIds = ['%s:%s' % (self.cfg.nodeName, x) for x in \
                            self.slaves.keys() + self.handlers.keys()])

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
        # ensure we don't exceed environmental constraints
        limit = min(limit, self.getMaxSlaves())
        self.cfg.slaveLimit = max(limit, 0)
        limit = max(limit - len(self.slaves) - len(self.handlers), 0)

        f = os.open(os.path.join(os.path.sep, 'srv', 'rbuilder', 'jobmaster', 'config.d', 'runtime'), 'w')
        f.write('slaveLimit %d' % limit)
        f.close()
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


def main():
    cfg = MasterConfig()
    cfg.read(os.path.join(os.path.sep, 'srv', 'rbuilder', 'jobmaster',
                          'config'))
    jobMaster = JobMaster(cfg)
    jobMaster.run()

def runDaemon():
    return main()
    pidFile = os.path.join(os.path.sep, 'var', 'run', 'jobmaster.pid')
    if os.path.exists(pidFile):
        f = open(pidFile)
        pid = f.read()
        f.close()
        statPath = os.path.join(os.path.sep, 'proc', pid, 'stat')
        if os.path.exists(statPath):
            f = open(statPath)
            name = f.read().split()[1][1:-1]
            if name == 'jobmaster':
                print >> sys.stderr, "Job Master already running as: %s" % pid
                sys.stderr.flush()
                sys.exit(-1)
            else:
                # pidfile doesn't point to a job master
                os.unlink(pidFile)
        else:
            # pidfile is stale
            os.unlink(pidFile)
    pid = os.fork()
    if not pid:
        os.setsid()
        devNull = os.open(os.devnull, os.O_RDWR)
        os.dup2(devNull, sys.stdout.fileno())
        os.dup2(devNull, sys.stderr.fileno())
        os.dup2(devNull, sys.stdin.fileno())
        os.close(devNull)
        pid = os.fork()
        if not pid:
            f = open(pidFile, 'w')
            f.write(str(os.getpid()))
            f.close()
            main()
            os.unlink(pidFile)
