import errno
import logging
import math
import os
import signal
import simplejson
import sys
import tempfile
import threading
import time
import weakref
from conary import conarycfg
from conary import conaryclient
from conary import versions
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib.util import copyfile
from mcp import response
from mcp import slavestatus

from jobmaster import imagecache
from jobmaster import xencfg
from jobmaster.util import createFile, getIP, logCall
from jobmaster.resource import (AutoMountResource, LVMResource,
        XenDomainResource, Resource)

log = logging.getLogger('jobmaster.handler')


class TaskAbortError(RuntimeError):
    """
    Raised when a GroupTask worker is killed.
    """
    def __init__(self, signum):
        RuntimeError.__init__(self, signum)
        self.signum = signum

    def __str__(self):
        return 'Caught signal %d in worker process' % self.signum


class GroupTask(threading.Thread):
    """
    Base class for a task that should run in a self-contained
    process group, but be monitored from a thread in the current
    process.
    """

    def __init__(self):
        threading.Thread.__init__(self)
        self._pid = None
        self._retcode = None
        self._lock = threading.Lock()

    def start(self):
        self._preStart()
        threading.Thread.start(self)

    def run(self):
        self._lock.acquire()
        if self._pid:
            pid = self._pid
            self._lock.release()
            raise RuntimeError("Process already active with pid %d!" % pid)

        try:
            pid = os.fork()
        except:
            self._lock.release()
            raise

        if not pid:
            # Child: setpgid() and do work, then exit.
            retcode = 1
            try:
                self._lock = None # irrelevant to the subprocess

                for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT):
                    signal.signal(signum, self._signal)

                retcode = self._run()
                if retcode is None:
                    retcode = 0
            finally:
                os._exit(retcode)
        else:
            # Parent: wait on child and set status when it exits.
            self._pid = pid
            self._lock.release()

            _, status = os.waitpid(self._pid, 0)

            self._lock.acquire()
            try:
                self._pid = None
                self._retcode = os.WEXITSTATUS(status)
            finally:
                self._lock.release()

            self._cleanup()

    @staticmethod
    def _signal(signum, traceback):
        """
        Throw an exception on signal to force the child to quit
        gracefully.
        """
        del traceback
        raise TaskAbortError(signum)

    def kill(self, join=True):
        """
        Kill the worker process. If C{join} is C{True}, wait for it to
        terminate and return its exit status.
        """
        self._lock.acquire()
        try:
            if not self._pid:
                if join:
                    # It's already stopped, so return the status
                    # code.
                    return self.join(_lock=False)
                raise RuntimeError("Subprocess is not running")

            tries = 3
            while tries:
                useSignal = tries and signal.SIGTERM or signal.SIGKILL
                try:
                    os.kill(self._pid, useSignal)
                except OSError, exc:
                    if exc.errno != errno.ESRCH:
                        raise
                    else:
                        # Process is gone
                        break
                else:
                    if not join:
                        # Don't bother waiting
                        break
                    time.sleep(0.1)
                    tries -= 1

            if join:
                return self.join(_lock=False)
        finally:
            self._lock.release()

    def join(self, _lock=True):
        """
        Wait for the worker process to terminate and return its exit
        status.
        """

        threading.Thread.join(self)

        if _lock:
            self._lock.acquire()
        try:
            return self._retcode
        finally:
            if _lock:
                self._lock.release()

    # Override these:
    @staticmethod
    def _preStart():
        pass

    @staticmethod
    def _run():
        raise NotImplementedError

    @staticmethod
    def _cleanup():
        pass


class _SlaveHandler(GroupTask):
    def __init__(self, master, troveTup, jobData):
        GroupTask.__init__(self)

        self.cfg = master.cfg
        self.conaryCfg = master.conaryCfg
        self.kernelData = master.kernelData
        self.response = master.response

        self.troveTup = troveTup
        self.jobData = jobData

        self.slaveName = None

        self._resources = None

        self.jobQueueName = self._getJobQueueName()

    # Public methods -- these are callable from outside the handler
    def stop(self):
        # XXX: rewrite
        log.info('Stopping slave %s', self.slaveName)
        self.kill()

        #logCall('xm destroy %s' % self.slaveName, ignoreErrors=True)
        log.info('DESTROYING %s', self.slaveName)

        self._waitForSlave()

        logCall("lvremove -f /dev/%s/%s-base" %
                (self.cfg.lvmVolumeName, self.slaveName, subvol),
                ignoreErrors=True)

        # pylint: disable-msg=E1101
        self._slaveStatus(slavestatus.OFFLINE)

    # Internal parent-side methods
    def _preStart(self):
        """
        Create and write domain configuration.
        """
        self.slaveName = self.xenCfg.cfg['name']
        # pylint: disable-msg=E1101
        self._slaveStatus(slavestatus.BUILDING, jobId = self.jobData['UUID'])

    def _slaveStatus(self, status, jobId=None):
        name = '%s:%s' % (self.cfg.nodeName, self.slaveName)
        log.info("Setting slave status for %s to %s%s", name, status,
                jobId and ' (job %s)' % jobId or '')
        self.response.slaveStatus(name, status, self.jobQueueName[3:], jobId)

    def _getJobQueueName(self):
        jsVersion = str(self.troveTup[1].trailingRevision())

        arch = 'unknown'
        for refFlv, refArch in (('1#x86_64', 'x86_64'), ('1#x86', 'x86')):
            if self.troveTup[2].satisfies(deps.ThawFlavor(refFlv)):
                arch = refArch
                break
        return 'job%s:%s' % (jsVersion, arch)

    def getTroveSize(self):
        protocolVersion = self.jobData.get('protocolVersion')
        assert protocolVersion in (1,), "Unknown protocol version %s" % \
                str(protocolVersion)

        if self.jobData['type'] == 'build':
            # parse the configuration passed in from the job
            ccfg = conarycfg.ConaryConfiguration()
            for x in self.jobData['project']['conaryCfg'].split("\n"):
                ccfg.configLine(x)

            cc = conaryclient.ConaryClient(ccfg)
            repos = cc.getRepos()
            n = self.jobData['troveName'].encode('utf8')
            v = versions.ThawVersion(self.jobData['troveVersion'].encode('utf8'))
            f = deps.ThawFlavor(self.jobData.get('troveFlavor').encode('utf8'))
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
        mountDict = self.jobData.get('data', {}).get('mountDict', {})
        # this ends up double counting if both freeSpace and requested size
        # are used in combination. requested size is often double counted with
        # respect to actual trove contents. This is simply an estimate. if we
        # must err, we need to overestimate, so it's fine.

        # mountDict is in MB. other measurements are in bytes
        return sum([x[0] + x[1] for x in mountDict.values()]) * 1024 * 1024

    def estimateScratchSize(self):
        troveSize = self.getTroveSize()
        if self.jobData.get('type') == 'cook':
            return troveSize / (1024 * 1024)

        # these two handle legacy formats
        freeSpace = int(self.jobData.get('data', {}).get('freespace', 0)) \
            * 1024 * 1024
        swapSize = int(self.jobData.get('data', {}).get('swapSize', 0)) \
            * 1024 * 1024

        mountOverhead = self.addMountSizes()

        size = troveSize + freeSpace + swapSize + mountOverhead
        #Pad 15% for filesystem overhead (inodes, etc)
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

        minslavesize = self.cfg.minSlaveSize
        if size > minslavesize:
            return size
        else:
            return minslavesize

    # Internal child-side methods
    def _run(self):
        """
        Build, boot, and watch a jobslave, then clean up when
        it's done.
        """

        # Holding the same response object in multiple processes means
        # a risk of collisions, so reopen.
        #self.response = response.MCPResponse(self.cfg.nodeName, self.cfg) # XXX

        self._resources = []
        try:
            self._buildSlave()
            self._waitForSlave()
        finally:
            while self._resources:
                try:
                    self._resources.pop().close()
                except:
                    log.exception("Error in cleanup; continuing:")

    def _createJSRootDisk(self):
        # Fetch the root tarball for this js + kernel
        log.info("Getting slave image: %s=%s[%s]", *self.troveTup)
        imagePath, metadata = imagecache.getImage(
                self.conaryCfg,
                os.path.join(self.cfg.basePath, 'imageCache'),
                [self.troveTup])

        # Allocate LV
        rootSize = long(metadata['tree_size']) / 1048576 + 60
        rootSize += self.estimateScratchSize()
        rootName = self.slaveName + '-base'
        rootDevice = '/dev/%s/%s' % (self.cfg.lvmVolumeName, rootName)
        log.info("Creating slave root of %dMiB at %s", rootSize, rootDevice)
        logCall("lvcreate -n '%s' -L %dM '%s'" % (rootName, rootSize,
            self.cfg.lvmVolumeName))

        rootResource = LVMResource(rootDevice)
        self._resources.append(rootResource)

        # Format
        logCall("mkfs.xfs -f '%s'" % (rootDevice,))

        # Mount, unpack, and tweak configuration
        log.info("Preparing jobslave root")
        mountResource = AutoMountResource(['-t', 'xfs', rootDevice])
        self._resources.append(mountResource)

        logCall("lzma -dc '%s' | tar -xC '%s' " % (imagePath,
            mountResource.mountPoint))
        self._writeJobSlaveConfig(mountResource.mountPoint)

        mountResource.close()
        self._resources.remove(mountResource)

        return rootResource

    def _buildSlave(self):
        """
        Create a jobslave with all its assorted disks and boot it.
        """

        try:
            self._createJSRootDisk()

            self._boot()
        except:
            log.exception("Error building jobslave:")
            try:
                self._slaveStatus(slavestatus.OFFLINE)
            except:
                log.exception("Error setting slave status to OFFLINE:")
            raise

    def _boot(self):
        raise NotImplementedError

    def _waitForSlave(self):
        raise NotImplementedError

    def _writeJobSlaveConfig(self, mountPoint):
        """
        Write runtime jobslave configuration data, including job data,
        jobslave and networking configuration.
        """
        # Jobslave configuration
        config = ''
        config += 'queueHost %s\n' % (
            (self.cfg.queueHost != '127.0.0.1')
            and self.cfg.queueHost or getIP())
        config += 'queuePort %s\n' % str(self.cfg.queuePort)
        config += 'nodeName %s:%s\n' % (self.cfg.nodeName, self.slaveName)
        config += 'jobQueueName %s\n' % self.jobQueueName
        if self.cfg.conaryProxy:
            config += 'conaryProxy %s\n' % self.cfg.conaryProxy
        config += 'debugMode %s\n' % str(self.cfg.debugMode)
        createFile(mountPoint, 'srv/jobslave/config.d/runtime', config)

        # Job data
        createFile(mountPoint, 'srv/jobslave/data',
                simplejson.dumps(self.jobData))

        # Networking - master IP is domU IP + 127
        # TODO: check if this scheme makes any sense --
        # the master and slave IP spaces overlap, though not
        # at the same point in time since a low slave octet results in
        # a high master octet. The overlap is still unnecessary.
        masterIP = [int(x) for x in self.xenCfg.ip.split('.')]
        masterIP[3] += 127
        masterIP[3] %= 256
        masterIP = '.'.join(str(x) for x in masterIP)

        network = ''
        network += 'DEVICE=eth0\n'
        network += 'BOOTPROTO=static\n'
        network += 'IPADDR=%s\n' % self.xenCfg.ip
        network += 'GATEWAY=%s\n' % masterIP
        network += 'ONBOOT=yes\n'
        network += 'TYPE=Ethernet\n'
        createFile(mountPoint, 'etc/sysconfig/network-scripts/ifcfg-eth0',
                network)
        createFile(mountPoint, 'etc/sysconfig/slave_runtime',
                'MASTER_IP=%s' % masterIP)
        copyfile('/etc/resolv.conf',
                os.path.join(mountPoint, 'etc/resolv.conf'))


class DummyHandler(_SlaveHandler):
    class DummyResource(Resource):
        def _close(self):
            log.info("Shutting down")

    def _boot(self):
        log.info("Booting")
        self._resources.append(self.DummyResource())

    def _waitForSlave(self):
        time.sleep(5)


class XenHandler(_SlaveHandler):
    def _boot(self):
        """
        Boot the xen domain.
        """
        fObj = tempfile.NamedTemporaryFile(suffix='.cfg',
                dir=os.path.join(self.cfg.basePath, 'tmp'))
        self.xenCfg.write(fObj)
        fObj.flush()

        log.info('Booting slave %s' % self.slaveName)
        logCall('xm create %s' % fObj.name)
        fObj.seek(0)
        sys.stdout.write(fObj.read())
        import epdb;epdb.st()
        self._resources.append(XenDomainResource(self.slaveName))

    def _waitForSlave(self):
        """
        Wait until all LVs in use by this handler are no longer in use.
        """
        while True:
            time.sleep(5)

            data = os.popen("lvdisplay -c").read()
            for line in data.splitlines():
                pieces = line.strip().split(':')
                deviceName, users = pieces[0], int(pieces[5])
                if self.slaveName in deviceName and users > 0:
                    # Disk belongs to this handler and is in use.
                    break
            else:
                # No disks, or none in use.
                break


class LXCHandler(_SlaveHandler):
    def _boot(self):
        pass

    def _waitForSlave(self):
        pass


def main(args):
    # test function
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
    ccfg = conarycfg.ConaryConfiguration(True)
    ccfg.initializeFlavors()
    cli = conaryclient.ConaryClient(ccfg)
    spec = cmdline.parseTroveSpec(args[0])
    tup = max(cli.getRepos().findTrove(None, spec, ccfg.flavor))

    from jobmaster import master
    from jobmaster.util import getRunningKernel
    class Master:
        conaryCfg = ccfg
        cfg = master.MasterConfig()
        kernelData = getRunningKernel()
        class response:
            @staticmethod
            def slaveStatus(*args):
                pass
    m = Master()
    m.cfg.lvmVolumeName = 'vg_darco'
    m.cfg.nodeName = 'wut'
    handler = XenHandler(m, tup, {
        'UUID': 'wut',
        'protocolVersion': 1,
        'type': 'build',
        'project': {
            'conaryCfg': '',
            },
        'data': {
            'freespace': 0,
            'swapSize': 0,
            'mountDict': {},
            },
        'troveName': 'group-core',
        'troveVersion': '/conary.rpath.com@rpl:devel//2/1234284446.569:2.0-0.31-1',
        'troveFlavor': '1#x86:i486:i586:i686:sse:sse2|1#x86_64|5#use:~!dom0:~!domU:~!vmware:~!xen',
        })
    handler.start()
    rv = handler.join()
    print 'status:', rv
    #handler._preStart()
    #handler.run()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
