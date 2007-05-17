#!/usr/bin/python
#
# Copyright (c) 2005-2006 rPath, Inc.
#
# All rights reserved
#

import os, sys
import inspect
import logging
log = logging
import math
import simplejson
import tempfile
import threading
import time
import traceback
import shutil
import signal
import weakref

from jobmaster import master_error
from jobmaster import imagecache
from jobmaster import xencfg, xenmac

from mcp import queue
from mcp import response
from mcp import client
from mcp import slavestatus

from conary.lib import cfgtypes, util
from conary import conarycfg
from conary import conaryclient
from conary.conaryclient import cmdline
from conary.deps import deps
from conary import versions

CONFIG_PATH = os.path.join(os.path.sep, 'srv', 'rbuilder', 'jobmaster',
                           'config.d', 'runtime')

def getAvailableArchs(arch):
    if arch in ('i686', 'i586', 'i486', 'i386'):
        return ('x86',)
    elif arch == 'x86_64':
        return ('x86', 'x86_64')

def getIP():
    p = os.popen("""/sbin/ifconfig `/sbin/route | grep "^default" | sed "s/.* //"` | grep "inet addr" | awk -F: '{print $2}' | sed 's/ .*//'""")
    data = p.read().strip()
    p.close()
    return data

def controlMethod(func):
    func._controlMethod = True
    return func

def getBootPaths():
    p = os.popen('uname -r')
    runningKernel = p.read().strip()
    p.close()

    files = [x for x in os.listdir('/boot') if runningKernel in x]
    kernel = [x for x in files if x.startswith('vmlinuz')][0]
    kernel = os.path.join(os.path.sep, 'boot', kernel)

    initrd = [x for x in files if x.startswith('initrd')][0]
    initrd = os.path.join(os.path.sep, 'boot', initrd)
    return kernel, initrd


def rewriteFile(template, target, data):
    f = open(template, 'r')
    templateData = f.read()
    f.close()
    f = open(target, 'w')
    f.write(templateData % data)
    f.close()
    os.unlink(template)


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

def catchErrors(func):
    def wrapper(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except:
            exc, e, bt = sys.exc_info()
            try:
                log.error(''.join(traceback.format_tb(bt)))
                log.error(e)
            except:
                print >> sys.stderr, "couldn't log error", e
    return wrapper

class MasterConfig(client.MCPClientConfig):
    basePath = os.path.join(os.path.sep, 'srv', 'rbuilder', 'jobmaster')
    slaveLimit = (cfgtypes.CfgInt, 1)
    nodeName = (cfgtypes.CfgString, None)
    slaveMemory = (cfgtypes.CfgInt, 512) # memory in MB
    proxy = None

class SlaveHandler(threading.Thread):
    # A slave handler is tied to a specific slave instance. do not re-use.
    def __init__(self, master, troveSpec):
        self.master = weakref.ref(master)
        self.imageCache = weakref.ref(master.imageCache)
        self.cfgPath = ''
        self.slaveName = None
        self.troveSpec = troveSpec
        self.jobQueueName = self.getJobQueueName()
        self.lock = threading.RLock()
        threading.Thread.__init__(self)
        self.pid = None

    def slaveStatus(self, status):
        self.master().slaveStatus(self.slaveName, status,
                                  self.jobQueueName.replace('job', ''))

    def start(self):
        fd, self.imagePath = tempfile.mkstemp()
        os.close(fd)

        kernel, initrd = getBootPaths()

        xenCfg = xencfg.XenCfg(self.imagePath,
                               {'memory' : self.master().cfg.slaveMemory,
                                'kernel': kernel,
                                'initrd': initrd,
                                'root': '/dev/xvda1 ro'})
        self.slaveName = xenCfg.cfg['name']
        self.ip = xenCfg.ip
        self.slaveStatus(slavestatus.BUILDING)
        fd, self.cfgPath = tempfile.mkstemp()
        os.close(fd)
        f = open(self.cfgPath, 'w')
        xenCfg.write(f)
        f.close()
        threading.Thread.start(self)
        log.info('starting slave: %s' % self.slaveName)
        return xenCfg.cfg['name']

    def stop(self):
        log.info('stopping slave %s' % self.slaveName)
        pid = self.pid
        if pid:
            try:
                os.kill(-pid, signal.SIGTERM)
            except OSError, e:
                # ignore race condition where child died right after we recorded
                # it's pid
                if errno != 3:
                    raise
        os.system('xm destroy %s' % self.slaveName)
        if os.path.exists(self.imagePath):
            util.rmtree(self.imagePath, ignore_errors = True)
        self.slaveStatus(slavestatus.OFFLINE)
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
            os.setpgid(0, 0)
            try:
                # don't use original. make a backup
                log.info('Getting slave image: %s' % self.troveSpec)
                cachedImage = self.imageCache().getImage(self.troveSpec)
                log.info("Making runtime copy of cached image at: %s" % \
                             self.imagePath)
                shutil.copyfile(cachedImage, self.imagePath)
                log.info("making mount point")
                # now add per-instance settings. such as path to MCP
                mntPoint = tempfile.mkdtemp()
                f = None
                try:
                    log.info('inserting runtime settings into slave')
                    os.system('mount -o loop %s %s' % (self.imagePath, mntPoint))
                    cfg = self.master().cfg

                    # write python SlaveConfig
                    cfgPath = os.path.join(mntPoint, 'srv', 'jobslave', 'config.d',
                                          'runtime')
                    util.mkdirChain(os.path.split(cfgPath)[0])
                    f = open(cfgPath, 'w')

                    # It never makes sense to direct a remote machine to
                    # 127.0.0.1
                    f.write('queueHost %s\n' % ((cfg.queueHost != '127.0.0.1') \
                                                    and cfg.queueHost \
                                                    or getIP()))

                    f.write('queuePort %s\n' % str(cfg.queuePort))
                    f.write('nodeName %s\n' % ':'.join((cfg.nodeName,
                                                        self.slaveName)))
                    f.write('jobQueueName %s\n' % self.jobQueueName)
                    if cfg.proxy:
                        f.write('proxy %s' % cfg.proxy)
                    f.close()

                    # write init script settings
                    initSettings = os.path.join(mntPoint, 'etc', 'sysconfig',
                                                'slave_runtime')
                    util.mkdirChain(os.path.split(initSettings)[0])
                    f = open(initSettings, 'w')

                    # the host IP address is domU IP address + 127 of the last quad
                    quads = [int(x) for x in self.ip.split(".")]
                    masterIP = ".".join(str(x) for x in quads[:3] + [quads[3]+127])

                    f.write('MASTER_IP=%s' % masterIP)
                    f.close()
                    entitlementsDir = os.path.join(os.path.sep, 'srv',
                                                   'rbuilder', 'entitlements')
                    if os.path.exists(entitlementsDir):
                        util.copytree(entitlementsDir,
                                      os.path.join(mntPoint, 'srv', 'jobslave'))

                    # set up networking inside domU
                    ifcfg = os.path.join(mntPoint, 'etc', 'sysconfig', 'network-scripts', 'ifcfg-eth0')
                    rewriteFile(ifcfg + ".template", ifcfg, dict(masterip = masterIP, ipaddr = self.ip))

                    resolv = os.path.join(mntPoint, 'etc', 'resolv.conf')
                    f = open(resolv, 'w')
                    f.write("nameserver %s\n" % self.ip)
                    f.close()

                finally:
                    if f:
                        f.close()
                    os.system('umount %s' % mntPoint)
                    util.rmtree(mntPoint, ignore_errors = True)

                log.info('booting slave: %s' % self.slaveName)
                os.system('xm create %s' % self.cfgPath)
            except:
                exc, e, tb = sys.exc_info()
                log.error(''.join(traceback.format_tb(tb)))
                log.error(e)
                try:
                    self.slaveStatus(slavestatus.OFFLINE)
                except Exception, innerException:
                    # this process must exit regardless of failure to log.
                    log.error("Error setting slave status to OFFLINE: " + str(innerException))
                # forcibly exit *now* sys.exit raises a SystemExit exception
                os._exit(1)
            else:
                self.slaveStatus(slavestatus.STARTED)
                os._exit(0)
        os.waitpid(self.pid, 0)
        self.pid = None

class JobMaster(object):
    def __init__(self, cfg):
        logging.basicConfig(level=logging.DEBUG,
            format ='%(asctime)s %(levelname)s %(message)s',
            filename = os.path.join(cfg.basePath, 'logs', 'jobmaster.log'),
            filemode='a')

        if cfg.nodeName is None:
            cfg.nodeName = getIP() or '127.0.0.1'
        self.cfg = cfg
        xenmac.setMaxSeq(self.cfg.slaveLimit)
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
        self.imageCache = imagecache.ImageCache(os.path.join(self.cfg.basePath,
                                                             'imageCache'))
        self.slaves = {}
        self.handlers = {}
        self.sendStatus()

        signal.signal(signal.SIGTERM, self.catchSignal)
        signal.signal(signal.SIGINT, self.catchSignal)

        log.info('started jobmaster: %s' % self.cfg.nodeName)

    @catchErrors
    def checkControlTopic(self):
        dataStr = self.controlTopic.read()
        while dataStr:
            data = simplejson.loads(dataStr)
            node = data.get('node', '')
            if node in ('masters', self.cfg.nodeName):
                action = data['action']
                kwargs = dict([(str(x[0]), x[1]) for x in data.iteritems() \
                                   if x[0] not in ('node', 'action')])
                memberDict = dict([ \
                        x for x in inspect.getmembers( \
                            self, lambda x: callable(x))])
                if action in memberDict:
                    func = memberDict[action]
                    if '_controlMethod' in func.__dict__:
                        return func(**kwargs)
                    else:
                        raise master_error.ProtocolError( \
                            "Action '%s' is not a control method" % action)
                else:
                    raise master_error.ProtocolError( \
                        "Control method '%s' does not exist" % action)
            elif node.split(':')[0] == self.cfg.nodeName:
                #check list of slaves and ensure it's really up
                slaveName = node.split(':')[1]
                p = os.popen("xm list| awk '{print $1;}'")
                if slaveName not in p.read():
                    log.info("Detected missing slave.")
                    self.sendStatus()
                p.close()
            dataStr = self.controlTopic.read()

    def getMaxSlaves(self):
        # this function is desgined for xen. if we extend to remote slaves
        # such as EC2 it will need reworking.
        p = os.popen('xm info | grep total_memory | sed "s/.* //"')
        mem = p.read().strip()
        if mem.isdigit():
            mem = int(mem)
        else:
            return 1
        p.close()
        count = mem / self.cfg.slaveMemory
        # Enforce that the master has at least as much memory as half a slave.
        if (mem % self.cfg.slaveMemory) < (self.cfg.slaveMemory / 2):
            count -= 1
        return count

    def resolveTroveSpec(self, troveSpec):
        # this function is designed to ensure a partial NVF can be resolved to
        # a full NVF for caching and creation purposes.
        cfg = conarycfg.ConaryConfiguration(True)
        cfg.initializeFlavors()
        cc = conaryclient.ConaryClient(cfg)
        n, v, f = cmdline.parseTroveSpec(troveSpec)
        troves = cc.repos.findTrove(None, (n, v, None))
        refXen = deps.parseFlavor('xen, domU')
        troves = [x for x in troves if x[2].stronglySatisfies(refXen) \
                      and x[2].stronglySatisfies(f)]
        if not troves:
            log.warning("Found no troves when looking for slaves. "
                        "This is almost certainly unwanted behavior. "
                        "Falling back to: %s" % troveSpec)
            return troveSpec
        if len(troves) > 1:
            # compare each pair of troves. take the difference between them
            # and see if the result satisfies f. this roughly translates to
            # a concept of "narrowest match" because an exact arch will be
            # preferred over a multi-arch trove.
            refTrove = troves[0]
            for trv in troves:
                if trv != refTrove:
                    if f.satisfies(trv[2].difference(refTrove[2])):
                        refTrove = trv
            troves = [refTrove]
        res = '%s=%s[%s]' % troves[0]
        log.info("Using %s to satisfy %s for slave" % (res, troveSpec))
        return res

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
                log.info('Setting limit of demand queue to: %s' % str(self.demandQueue.queueLimit))
        self.sendStatus()

    @catchErrors
    def checkDemandQueue(self):
        dataStr = self.demandQueue.read()
        if dataStr:
            data = simplejson.loads(dataStr)
            if data['protocolVersion'] == 1:
                self.handleSlaveStart(data['troveSpec'])
            else:
                log.error('Invalid Protocol Version %d' % \
                              data['protocolVersion'])
                # FIXME: protocol problem
                # should implement some sort of error feedback to MCP

    @catchErrors
    def checkHandlers(self):
        """Move slaves from 'being started' to 'active'

        Handlers are used to start slaves. Once a handler is done starting a
        slave, we know it's active."""
        for slaveName in [x[0] for x in self.handlers.iteritems() \
                              if not x[1].isAlive()]:
            self.slaves[slaveName] = self.handlers[slaveName]
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

    def catchSignal(self, sig, frame):
        log.info('caught signal: %d' % sig)
        self.running = False

    def disconnect(self):
        log.info('stopping jobmaster')
        for handler in self.handlers.values():
            handler.stop()
        self.running = False
        self.demandQueue.disconnect()
        self.controlTopic.disconnect()
        del self.response

    def sendStatus(self):
        log.info('sending master status')
        self.response.masterStatus( \
            arch = self.arch, limit = self.cfg.slaveLimit,
            slaveIds = ['%s:%s' % (self.cfg.nodeName, x) for x in \
                            self.slaves.keys() + self.handlers.keys()])

    def slaveStatus(self, slaveName, status, slaveType):
        log.info('sending slave status: %s %s %s' % \
                     (self.cfg.nodeName + ':' + slaveName, status, slaveType))
        self.response.slaveStatus(self.cfg.nodeName + ':' + slaveName,
                                  status, slaveType)

    def getBestProtocol(self, protocols):
        common = PROTOCOL_VERSIONS.intersection(protocols)
        return common and max(common) or 0

    @controlMethod
    def checkVersion(self, protocols):
        log.info('asked for protocol compatibility: %s' % str(protocols))
        self.response.protocol(self.getBestProtocol(protocols))

    @controlMethod
    @protocols((1,))
    def slaveLimit(self, limit):
        log.info('asked to set slave limit to %d' % limit)
        # ensure we don't exceed environmental constraints

        limit = min(limit, self.getMaxSlaves())
        self.cfg.slaveLimit = max(limit, 0)

        f = open(CONFIG_PATH, 'w')
        f.write('slaveLimit %d\n' % limit)
        f.close()
        xenmac.setMaxSeq(self.cfg.slaveLimit)
        self.sendStatus()

        limit = max(limit - len(self.slaves) - len(self.handlers), 0)
        self.demandQueue.setLimit(limit)

    @controlMethod
    @protocols((1,))
    def clearImageCache(self):
        log.info('clearing image cache')
        self.imageCache.deleteAllImages()

    @controlMethod
    @protocols((1,))
    def status(self):
        log.info('status requested')
        self.sendStatus()

    @controlMethod
    @protocols((1,))
    def stopSlave(self, slaveId):
        log.info('stopping slave: %s' % slaveId)
        self.handleSlaveStop(slaveId)


def main():
    cfg = MasterConfig()
    cfg.read(os.path.join(os.path.sep, 'srv', 'rbuilder', 'jobmaster',
                          'config'))
    jobMaster = JobMaster(cfg)
    jobMaster.run()

def runDaemon():
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
        devNull = os.open(os.devnull, os.O_RDWR)
        os.dup2(devNull, sys.stdout.fileno())
        os.dup2(devNull, sys.stderr.fileno())
        os.dup2(devNull, sys.stdin.fileno())
        os.close(devNull)
        pid = os.fork()
        if not pid:
            os.setpgid(0, 0)
            f = open(pidFile, 'w')
            f.write(str(os.getpid()))
            f.close()
            main()
            os.unlink(pidFile)
