#/usr/bin/python
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
import stat
import weakref

from jobmaster import master_error
from jobmaster import imagecache
from jobmaster import templateserver
from jobmaster import xencfg, xenmac
from jobmaster.util import rewriteFile, logCall, getIP

from mcp import mcp_log
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

LVM_PATH = os.path.join(os.path.sep, 'dev', 'mapper')

def getAvailableArchs(arch):
    if arch in ('i686', 'i586', 'i486', 'i386'):
        return ('x86',)
    elif arch == 'x86_64':
        return ('x86', 'x86_64')

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

    logFile = os.path.join(os.path.sep, 'var', 'log', 'rbuilder', 'jobmaster.log')
    logLevel = (mcp_log.CfgLogLevel, 'INFO')

    slaveLimit = (cfgtypes.CfgInt, 1)
    # maxSlaveLimit == 0 means unlimited
    maxSlaveLimit = (cfgtypes.CfgInt, 0)
    nodeName = (cfgtypes.CfgString, None)
    slaveMemory = (cfgtypes.CfgInt, 512) # memory in MB
    conaryProxy = 'self' # "self" for same machine, otherwise a URL
    templateCache = os.path.join(basePath, 'anaconda-templates')
    lvmVolumeName = 'vg00'
    debugMode = (cfgtypes.CfgBool, False)

def waitForSlave(slaveName):
    done = False
    while not done:
        p = os.popen("lvdisplay -c")
        data = p.read()
        c = [int(x.split(':')[5]) for x in data.splitlines() if slaveName in x]
        done = not (c and max(c))


class SlaveHandler(threading.Thread):
    # A slave handler is tied to a specific slave instance. do not re-use.
    def __init__(self, master, troveSpec, data):
        self.master = weakref.ref(master)
        self.imageCache = weakref.ref(master.imageCache)
        self.cfgPath = ''
        self.slaveName = None
        self.data = data
        self.troveSpec = troveSpec
        self.jobQueueName = self.getJobQueueName()
        self.lock = threading.RLock()
        threading.Thread.__init__(self)
        self.pid = None
        self.offline = False

    def slaveStatus(self, status, jobId = None):
        self.master().slaveStatus(self.slaveName, status,
                                  self.jobQueueName.replace('job', ''), jobId)

    def start(self):
        kernel, initrd = getBootPaths()

        xenCfg = xencfg.XenCfg(os.path.join(os.path.sep,
            'dev', self.master().cfg.lvmVolumeName),
                               {'memory' : self.master().cfg.slaveMemory,
                                'kernel': kernel,
                                'initrd': initrd,
                                'root': '/dev/xvda1 ro'},
                                extraDiskTemplate = '/dev/%s/%%s' % (self.master().cfg.lvmVolumeName))
        self.slaveName = xenCfg.cfg['name']
        self.imagePath = '/dev/%s/%s-base' % (self.master().cfg.lvmVolumeName, self.slaveName)
        self.ip = xenCfg.ip
        self.slaveStatus(slavestatus.BUILDING, jobId = self.data['UUID'])
        fd, self.cfgPath = tempfile.mkstemp( \
            dir = os.path.join(self.master().cfg.basePath, 'tmp'))
        os.close(fd)
        f = open(self.cfgPath, 'w')
        xenCfg.write(f)
        f.close()
        threading.Thread.start(self)
        log.info('requesting slave: %s' % self.slaveName)
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
                if e.errno != 3:
                    raise

        log.info("destroying slave")
        logCall('xm destroy %s' % self.slaveName, ignoreErrors = True)

        waitForSlave(self.slaveName)

        log.info("destroying scratch space")
        logCall("lvremove -f /dev/%s/%s-scratch" % (self.master().cfg.lvmVolumeName, self.slaveName), ignoreErrors = True)
        logCall("lvremove -f /dev/%s/%s-base" % (self.master().cfg.lvmVolumeName, self.slaveName), ignoreErrors = True)

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

    def writeSlaveConfig(self, cfgPath, cfg):
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
        if cfg.conaryProxy:
            if cfg.conaryProxy == 'self':
                f.write('conaryProxy http://%s/\n' % getIP())
            else:
                f.write('conaryProxy %s\n' % cfg.conaryProxy)
        f.write('debugMode %s\n' % str(cfg.debugMode))
        f.close()

    def getTroveSize(self):
        protocolVersion = self.data.get('protocolVersion')
        assert protocolVersion in (1,), "Unknown protocol version %s" % \
                str(protocolVersion)

        if self.data['type'] == 'build':
            # parse the configuration passed in from the job
            ccfg = conarycfg.ConaryConfiguration()
            [ccfg.configLine(x) for x in self.data['project']['conaryCfg'].split("\n")]

            cc = conaryclient.ConaryClient(ccfg)
            repos = cc.getRepos()
            n = self.data['troveName'].encode('utf8')
            v = versions.ThawVersion(self.data['troveVersion'].encode('utf8'))
            f = deps.ThawFlavor(self.data.get('troveFlavor').encode('utf8'))
            NVF = repos.findTrove(None, (n, v, f), cc.cfg.flavor)[0]
            trove = repos.getTrove(*NVF)
            troveSize = trove.troveInfo.size()

            if troveSize:
                return troveSize
            else:
                # Not sure how we got here, but better to return something
                # reasonable than None
                log.warning('Failed to get size of trove %r', NVF)
                return 1024 * 1024 * 1024
        else:
            # currently the only non-build job is a cook. assuming 1G
            return 1024 * 1024 * 1024

    def addMountSizes(self):
        mountDict = self.data.get('data', {}).get('mountDict', {})
        # this ends up double counting if both freeSpace and requested size
        # are used in combination. requested size is often double counted with
        # respect to actual trove contents. This is simply an estimate. if we
        # must err, we need to overestimate, so it's fine.

        # mountDict is in MB. other measurements are in bytes
        return sum([x[0] + x[1] for x in mountDict.values()]) * 1024 * 1024

    def estimateScratchSize(self):
        protocolVersion = self.data.get('protocolVersion')
        troveSize = self.getTroveSize()
        if self.data.get('type') == 'cook':
            return troveSize / (1024 * 1024)

        # these two handle legacy formats
        freeSpace = int(self.data.get('data', {}).get('freespace', 0))
        swapSize = int(self.data.get('data', {}).get('swapSize', 0))

        mountOverhead = self.addMountSizes()

        size = troveSize + freeSpace + swapSize + mountOverhead
        size = int(math.ceil((size + 20 * 1024 * 1024) / 0.87))
        # partition offset is being ignored for our purposes. we're going to be
        # pretty generous so it shouldn't matter
        # we're not rounding up for cylinder size. LVM will do that
        # multiply scratch size by 4. LiveCDs could potentially consume that
        # much overhead. (base + z-tree + inner ISO + outer ISO) this is
        # almost definitely too much in the general case, but there's pretty
        # little harm in overesitmation.
        size *= 4
        blockSize = 1024 * 1024
        size /= blockSize + ((size % blockSize) and 1 or 0)
        return size

    def isOnline(self):
        self.lock.acquire()
        try:
            return not self.offline
        finally:
            self.lock.release()

    def run(self):
        self.pid = os.fork()
        if not self.pid:
            os.setpgid(0, 0)
            try:
                cfg = self.master().cfg
                # don't use original. make a backup
                log.info('Getting slave image: %s' % self.troveSpec)
                cachedImage = self.imageCache().getImage(self.troveSpec, cfg.debugMode)
                log.info("Making runtime copy of cached image at: %s" % \
                             self.imagePath)
                slaveSize = os.stat(cachedImage)[stat.ST_SIZE]
                # size was given in bytes, but we need megs
                slaveSize = slaveSize / (1024 * 1024) + \
                        ((slaveSize % (1024 * 1024)) and 1)
                logCall("lvcreate -n %s-base -L%dM %s" % (self.slaveName, slaveSize, cfg.lvmVolumeName))
                logCall("dd if=%s of=%s bs=16K" % (cachedImage, self.imagePath))
                log.info("making mount point")
                # now add per-instance settings. such as path to MCP
                mntPoint = tempfile.mkdtemp(\
                    dir = os.path.join(cfg.basePath, 'tmp'))
                f = None

                try:
                    # creating temporary scratch space
                    scratchSize = self.estimateScratchSize()
                    scratchName = "%s-scratch" % self.slaveName
                    log.info("creating %dM of scratch temporary space (/dev/%s/%s)" % (scratchSize, cfg.lvmVolumeName, scratchName))

                    logCall("lvcreate -n %s -L%dM %s" % (scratchName, scratchSize, cfg.lvmVolumeName))
                    logCall("mke2fs -m0 /dev/%s/%s" % (cfg.lvmVolumeName, scratchName))

                    log.info('inserting runtime settings into slave')
                    logCall('mount %s %s' % (self.imagePath, mntPoint))

                    # write python SlaveConfig
                    cfgPath = os.path.join(mntPoint, 'srv', 'jobslave', 'config.d',
                                          'runtime')
                    util.mkdirChain(os.path.split(cfgPath)[0])
                    self.writeSlaveConfig(cfgPath, cfg)

                    # insert jobData into slave
                    dataPath = os.path.join(mntPoint, 'srv', 'jobslave', 'data')
                    f = open(dataPath, 'w')
                    f.write(simplejson.dumps(self.data))
                    f.close()

                    # write init script settings
                    initSettings = os.path.join(mntPoint, 'etc', 'sysconfig',
                                                'slave_runtime')
                    util.mkdirChain(os.path.split(initSettings)[0])
                    f = open(initSettings, 'w')

                    # the host IP address is domU IP address + 127 of the last quad
                    quads = [int(x) for x in self.ip.split(".")]
                    masterIP = ".".join(str(x) for x in quads[:3] + [(quads[3]+127) % 256])

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
                    util.copyfile('/etc/resolv.conf', resolv) 

                finally:
                    if f:
                        f.close()
                    logCall('umount %s' % mntPoint, ignoreErrors = True)
                    util.rmtree(mntPoint, ignore_errors = True)

                log.info('booting slave: %s' % self.slaveName)
                logCall('xm create %s' % self.cfgPath)
            except:
                try:
                    exc, e, tb = sys.exc_info()
                    log.error(''.join(traceback.format_tb(tb)))
                    log.error(e)
                    try:
                        self.lock.acquire()
                        self.offline = True
                        self.lock.release()
                        self.slaveStatus(slavestatus.OFFLINE)
                    except Exception, innerException:
                        # this process must exit regardless of failure to log.
                        log.error("Error setting slave status to OFFLINE: " + str(innerException))
                # forcibly exit *now* sys.exit raises a SystemExit exception
                finally:
                    os._exit(1)
            else:
                try:
                    self.slaveStatus(slavestatus.STARTED,
                            jobId = self.data['UUID'])
                finally:
                    os._exit(0)
        os.waitpid(self.pid, 0)
        self.pid = None

class JobMaster(object):
    def __init__(self, cfg):
        mcp_log.addRootLogger(level=cfg.logLevel,
            format ='%(asctime)s %(levelname)s %(message)s',
            filename = cfg.logFile,
            filemode='a')

        if cfg.nodeName is None:
            cfg.nodeName = getIP() or '127.0.0.1'
        self.cfg = cfg
        self.jobQueue = queue.MultiplexedQueue(cfg.queueHost, cfg.queuePort,
                                       namespace = cfg.namespace,
                                       timeOut = 0, queueLimit = cfg.slaveLimit)
        self.arch = os.uname()[-1]
        archs = getAvailableArchs(self.arch)
        assert archs, "Unknown machine architecture."
        for arch in archs:
            self.jobQueue.addDest('job:' + arch)
        self.controlTopic = queue.Topic(cfg.queueHost, cfg.queuePort,
                                       'control', namespace = cfg.namespace,
                                       timeOut = 0)
        self.response = response.MCPResponse(self.cfg.nodeName, cfg)
        self.imageCache = imagecache.ImageCache(os.path.join(self.cfg.basePath,
                                                             'imageCache'), cfg)
        self.slaves = {}
        self.handlers = {}

        signal.signal(signal.SIGTERM, self.catchSignal)
        signal.signal(signal.SIGINT, self.catchSignal)

        self.templateServer = templateserver.getServer(self.cfg.templateCache, hostname=self.cfg.nodeName, tmpDir=os.path.join(self.cfg.basePath, 'tmp'))

        log.info('started jobmaster: %s' % self.cfg.nodeName)
        self.lastHeartbeat = 0

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
        p = os.popen('xm info | grep total_memory | sed "s/.* //"')
        mem = p.read().strip()
        if mem.isdigit():
            mem = int(mem)
        else:
            log.error("can't determine host memory. assuming zero.")
            mem = 0
        p.close()

        # get the dom0 used mem.
        # note, we need to assume this value is constant so it doesn't play
        # well with the balloon driver.
        p = os.popen("xm list 0 | tail -n 1 | awk '{print $3};'")
        dom0Mem = p.read().strip()
        if dom0Mem.isdigit():
            dom0Mem = int(dom0Mem)
        else:
            log.error("can't determine host dom0 Memory. assuming zero.")
            dom0Mem = 0
        p.close()

        # reserve memory for non-slave usage
        mem -= dom0Mem
        count = max(0, mem / self.cfg.slaveMemory)
        if not count:
            log.error("not enough memory: jobmaster cannot support slaves at all")
        if self.cfg.maxSlaveLimit:
            count = min(max(0, self.cfg.maxSlaveLimit), count)
        return count

    def realSlaveLimit(self):
        p = os.popen('xm info | grep total_memory | sed "s/.* //"')
        mem = p.read().strip()
        if mem.isdigit():
            mem = int(mem)
        else:
            log.error("can't determine host memory. assuming zero.")
            mem = 0
        p.close()

        # add up the total memory of all domains
        # tradeoff. more complex bash script for far less complex test cases
        p = os.popen("n = 0; for x in `xm list | grep -v 'Mem(MiB)' | awk '{print $3}'`; do n=$(( $n + $x )); done; echo $n")
        domMem = p.read().strip()
        if domMem.isdigit():
            domMem = int(domMem)
        else:
            log.error("can't determine domain memory. assuming zero.")
            domMem = 0
        p.close()

        mem -= domMem
        # we need to put some swag into this number for the sake of xen.
        # we simply cannot eat *all* the RAM on the box
        mem -= 64
        count = max(0, mem / self.cfg.slaveMemory)
        if not count:
            log.error("memory squeeze won't allow for more slaves")
        if self.cfg.maxSlaveLimit:
            count = min(max(0, self.cfg.maxSlaveLimit), count)
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
                    if (trv[2].difference(refTrove[2])).satisfies(f):
                        refTrove = trv
            troves = [refTrove]
        res = '%s=%s[%s]' % troves[0]
        log.info("Using %s to satisfy %s for slave" % (res, troveSpec))
        return res

    def handleSlaveStart(self, data):
        troveSpec = self.resolveTroveSpec(data['jobSlaveNVF'])
        handler = SlaveHandler(self, troveSpec, data)
        self.handlers[handler.start()] = handler

    def handleSlaveStop(self, slaveId):
        slaveName = slaveId.split(':')[-1]
        handler = None
        if slaveName in self.slaves:
            handler = self.slaves[slaveName]
            del self.slaves[slaveName]
        elif slaveName in self.handlers:
            handler = self.handlers[slaveName]
            del self.handlers[slaveName]
        if handler:
            handler.stop()
            self.checkSlaveCount()
        self.sendStatus()

    def checkSlaveCount(self):
        currentSlaves = len(self.slaves) + len(self.handlers)
        totalCount = self.jobQueue.queueLimit + currentSlaves
        if totalCount != self.cfg.slaveLimit:
            slaveLimit = max(0, min(self.cfg.slaveLimit - currentSlaves,
                    self.realSlaveLimit() - len(self.handlers)))
            if slaveLimit != self.jobQueue.queueLimit:
                log.info('Setting limit of job queue to: %s' % str(slaveLimit))
            self.jobQueue.setLimit(slaveLimit)

    @catchErrors
    def checkJobQueue(self):
        dataStr = self.jobQueue.read()
        if dataStr:
            data = simplejson.loads(dataStr)
            if data['protocolVersion'] == 1:
                self.handleSlaveStart(data)
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

    @catchErrors
    def checkSlaves(self):
        currentSlaves = [x for x in self.slaves]
        if currentSlaves:
            p = os.popen("xm list | awk '{print $1;}' | grep -v 'Name' | grep -v 'Domain-0'")
            runningSlaves = [x.strip() for x in p.readlines()]
            for slave in [x for x in currentSlaves if x not in runningSlaves]:
                log.error('slave: %s unexpectedly died' % slave)
                self.handleSlaveStop(slave)

    @catchErrors
    def heartbeat(self):
        curTime = time.time()
        if (curTime - self.lastHeartbeat) > 30:
            self.lastHeartbeat = curTime
            self.checkSlaveCount()
            self.sendStatus()

    def run(self):
        self.running = True
        self.templateServer.start()
        try:
            while self.running:
                self.checkJobQueue()
                self.checkControlTopic()
                self.checkHandlers()
                self.checkSlaves()
                self.heartbeat()
                time.sleep(0.1)
        finally:
            self.stopAllSlaves()
            self.response.masterOffline()
            self.disconnect()
            self.templateServer.stop()

    def flushJobs(self):
        self.jobQueue.setLimit(0)
        dataStr = self.jobQueue.read()
        count = 0
        while dataStr:
            data = simplejson.loads(dataStr)
            if data.get('protocolVersion') == 1:
                jobId = data['UUID']
                slaveName = 'deadSlave%d' % count
                count += 1
                self.slaveStatus(slaveName, slavestatus.BUILDING, '', jobId)
                self.slaveStatus(slaveName, slavestatus.OFFLINE, '', jobId)
            else:
                # need to tell the mcp a job was lost
                log.error('Invalid Protocol Version %d' % \
                              data['protocolVersion'])
            dataStr = self.jobQueue.read()

    def stopAllSlaves(self):
        self.flushJobs()
        for slaveId in self.slaves.keys() + self.handlers.keys():
            self.handleSlaveStop(slaveId)

    def catchSignal(self, sig, frame):
        log.info('caught signal: %d' % sig)
        self.running = False

    def disconnect(self):
        log.info('stopping jobmaster')
        self.running = False
        self.jobQueue.disconnect()
        self.controlTopic.disconnect()
        del self.response

    def sendStatus(self):
        log.debug('sending master status')
        slaves = self.slaves.keys()
        handlers = [x[0] for x in self.handlers.iteritems() if x[1].isOnline()]
        self.response.masterStatus( \
            arch = self.arch, limit = self.cfg.slaveLimit,
            slaveIds = ['%s:%s' % (self.cfg.nodeName, x) for x in \
                            (slaves + handlers)])

    def slaveStatus(self, slaveName, status, slaveType, jobId):
        log.info('sending slave status: %s %s %s' % \
                     (self.cfg.nodeName + ':' + slaveName, status, slaveType))
        self.response.slaveStatus(self.cfg.nodeName + ':' + slaveName,
                                  status, slaveType, jobId)

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

        newLimit = min(limit, self.getMaxSlaves())
        if limit != newLimit:
            log.warning('System cannot support %d. setting slave limit to %d' \
                    % (limit, newLimit))
            limit = newLimit
        limit = max(limit, 0)
        self.cfg.slaveLimit = limit


        f = open(CONFIG_PATH, 'w')
        f.write('slaveLimit %d\n' % limit)
        f.close()
        self.sendStatus()

        self.checkSlaveCount()

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

def main(cfg):
    jobMaster = JobMaster(cfg)
    jobMaster.run()

def runDaemon():
    cfg = MasterConfig()
    cfg.read(os.path.join(os.path.sep, 'srv', 'rbuilder', 'jobmaster',
                          'config'))

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
            main(cfg)
            os.unlink(pidFile)
