#!/usr/bin/python
#
# Copyright (c) 2011 rPath, Inc.
#

import logging
import os
import sys
import tempfile
import traceback
from conary import conarycfg
from conary import conaryclient
from conary.lib.log import setupLogging
from jobmaster import cgroup
from jobmaster import linuxns
from jobmaster import osutil
from jobmaster.config import MasterConfig
from jobmaster.networking import AddressGenerator
from jobmaster.resource import ResourceStack
from jobmaster.resources.block import ScratchDisk
from jobmaster.resources.chroot import BoundContentsRoot
from jobmaster.resources.devfs import DevFS
from jobmaster.resources.mount import MountableDirectory
from jobmaster.resources.network import NetworkPairResource
from jobmaster.resources.tempdir import TempDir
from jobmaster.subprocutil import Pipe, Subprocess
from jobmaster.util import createDirectory, createFile, devNull, logCall, mount

log = logging.getLogger(__name__)


class ContainerWrapper(ResourceStack):
    """
    This resource stack creates and tears down all resources that live outside
    of the container process, specifically the scratch disk and contents root.
    """
    def __init__(self, name, troves, cfg, conaryClient, loopManager, network,
            scratchSize):
        ResourceStack.__init__(self)

        self.name = name
        self.cfg = cfg

        self.contents = BoundContentsRoot(troves, cfg, conaryClient)
        self.append(self.contents)

        self.scratch = ScratchDisk(cfg.lvmVolumeName, 'scratch_' + self.name,
                scratchSize)
        self.append(self.scratch)

        self.devFS = DevFS(loopManager)
        self.append(self.devFS)

        self.network = network
        self.append(self.network)

        self.container = Container(self.name, cfg)
        self.append(self.container)

    def start(self, jobData, prepareCB=None):
        self.contents.start(prepareCB)
        self.scratch.start()
        self.devFS.start()
        self.network.start()

        templateDir = self.cfg.getTemplateCache()
        if not os.path.isdir(templateDir):
            os.makedirs(templateDir)

        if self.cfg.debugMode:
            path = '/tmp/jobslave-%s-scratch' % self.name
            os.mkdir(path, 0700)
            self.append(self.scratch.mount(path, delete=True))

        pid = self.container.start(self.network, jobData,
                mounts=[
                    (self.contents, '', True),
                    (self.devFS, 'dev', True),
                    (self.scratch, 'tmp', False),
                    (self.scratch, 'var/tmp', False),
                    (self.scratch, 'var/lock', False),
                    (MountableDirectory(templateDir),
                        'mnt/anaconda-templates', True),
                    ])

        # Set up device capabilities and networking for the now-running cgroup
        cgroup.clearDeviceCaps(pid)
        cgroup.addDeviceCap(pid, perms='m') # allow mknod
        self.devFS.writeCaps(pid)
        self.network.moveSlave(pid)

        # Done configuring, so tell the child process to move on.
        self.container.release()

    def check(self):
        return self.container.check()

    def wait(self):
        return self.container.wait()

    def kill(self):
        return self.container.kill()


class Container(TempDir, Subprocess):
    procName = 'container'

    def __init__(self, name, cfg): 
        TempDir.__init__(self, prefix='root-')
        self.name = name
        self.cfg = cfg
        self.pid = self.network = self.jobData = self.mounts = None
        self.c2p_pipe = self.p2c_pipe = None

    def start(self, network, jobData, mounts):
        """
        Start the child container process and return its pid. The child will
        then wait for C{self.release()} to be called.
        """
        if self.pid:
            return
        self.network = network
        self.jobData = jobData
        self.mounts = mounts

        self.c2p_pipe, self.p2c_pipe = Pipe(), Pipe()
        self.pid = linuxns.clone(self._run_wrapper, (), new_uts=True,
                new_ipc=True, new_pid=True, new_net=True, new_user=True)
        self.c2p_pipe.closeWriter()
        self.p2c_pipe.closeReader()

        # Wait for the jobslave to finish doing mounts so we don't deny it
        # access to the scratch disk while it's still setting up.
        self.c2p_pipe.read()
        self.c2p_pipe.close()

        return self.pid

    def release(self):
        """
        Close the write pipe to the child proccess, triggering it to move on
        and do work. Call this after finished configuring the child's cgroup.
        """
        self.p2c_pipe.close()

    def _close(self):
        self.kill()
        TempDir._close(self)

    def _run_wrapper(self):
        try:
            try:
                rv = self._run()
                os._exit(rv)
            except:
                traceback.print_exc()
        finally:
            os._exit(70)

    def _run(self):
        self.c2p_pipe.closeReader()
        self.p2c_pipe.closeWriter()
        self._close_fds((self.c2p_pipe.writer, self.p2c_pipe.reader))

        osutil.sethostname("localhost.localdomain")

        self.doMounts()
        self.writeConfigs()

        # Signal the jobmaster to start configuring the cgroup.
        self.c2p_pipe.close()

        # Wait for the jobmaster to finish configuring the cgroup.
        self.p2c_pipe.read()
        self.p2c_pipe.close()

        # Finish network configuration from inside the cgroup.
        self.network.finishConfiguration()

        # Import this early to make sure we can unpickle exceptions thrown by
        # subprocess after chrooting, if the chroot python is different from
        # our own.
        import encodings.string_escape
        encodings = encodings

        os.chroot(self.path)
        os.chdir('/')

        #return logCall(["/bin/bash"], ignoreErrors=True, captureOutput=False, stdin=None)[0]
        null = (not self.cfg.debugMode) and devNull() or None
        return logCall(["/usr/bin/jobslave", "/tmp/etc/jobslave.conf"],
                ignoreErrors=True, logCmd=True, captureOutput=False,
                stdin=null, stdout=null, stderr=null)[0]

    def doMounts(self):
        """
        Mount contents, scratch, devices, etc. This is after the filesystem was
        unshared, so there's no need to ever unmount these -- when the
        container exits, they will be obliterated.
        """
        containerMounts = set()
        for resource, path, readOnly in self.mounts:
            target = os.path.join(self.path, path)
            containerMounts.add(target)
            mountRes = resource.mount(target, readOnly)
            mountRes.release()
        for fstype, path in [('proc', '/proc'), ('sysfs', '/sys')]:
            path = self.path + path
            containerMounts.add(path)
            mount(fstype, path, fstype)

        # Try to umount stuff not in this container to avoid blocking those
        # things from being umounted, especially in the case of other
        # jobslaves' scratch disks if they are mounted in the outer OS.
        otherMounts = set()
        for line in open('/proc/mounts'):
            path = line.split()[1]
            otherMounts.add(path)
        otherMounts -= containerMounts
        otherMounts.discard('/')
        for path in otherMounts:
            log.debug("Unmounting %s", path)
            logCall(["/bin/umount", "-dn", path], ignoreErrors=True)

    def writeConfigs(self):
        master = self.network.masterAddr.format(useMask=False)
        proxyURL = 'http://[%s]:%d/conary/' % (master,
                self.cfg.conaryProxyPort)
        masterURL = 'http://[%s]:%d/' % (master, self.cfg.masterProxyPort)
        createFile(self.path, 'tmp/etc/conaryrc',
                'conaryProxy http %s\n'
                'conaryProxy https %s\n'
                % (proxyURL, proxyURL))
        createFile(self.path, 'tmp/etc/jobslave.conf',
                'debugMode %s\n'
                'masterUrl %s\n'
                'conaryProxy %s\n'
                'jobDataPath /tmp/etc/jobslave.data\n'
                'templateCache /mnt/anaconda-templates\n'
                % (self.cfg.debugMode, masterURL, proxyURL))
        createFile(self.path, 'tmp/etc/jobslave.data', self.jobData)
        createDirectory(self.path, 'var/lock/rpm')


def main(args):
    import json
    import threading
    from conary.conaryclient import cmdline
    from conary.lib import util
    from jobmaster.proxy import ProxyServer
    from jobmaster.resources.devfs import LoopManager

    if len(args) < 2:
        sys.exit("Usage: %s <cfg> <trovespec>+" % sys.argv[0])

    setupLogging(consoleLevel=logging.DEBUG, consoleFormat='file')

    cfgPath, = args[:1]
    troveSpecs = args[1:]

    mcfg = MasterConfig()
    mcfg.read(cfgPath)

    proxy = ProxyServer(port=mcfg.masterProxyPort)
    proxyThread = threading.Thread(target=proxy.serve_forever)
    proxyThread.setDaemon(True)
    proxyThread.start()

    ccfg = conarycfg.ConaryConfiguration(True)
    ccfg.initializeFlavors()
    if mcfg.conaryProxy != 'self':
        ccfg.configLine('conaryProxy http %s' % mcfg.conaryProxy)
        ccfg.configLine('conaryProxy https %s' % mcfg.conaryProxy)
    cli = conaryclient.ConaryClient(ccfg)
    searchSource = cli.getSearchSource()

    specTups = [cmdline.parseTroveSpec(x) for x in troveSpecs]
    troveTups = [max(x) for x in searchSource.findTroves(specTups).values()]

    jobData = open('data').read()
    jobDataDict = json.loads(jobData)

    loopDir = tempfile.mkdtemp()
    try:
        loopManager = LoopManager(loopDir)
        name = os.urandom(6).encode('hex')
        generator = AddressGenerator(mcfg.pairSubnet)
        network = NetworkPairResource(generator, name)
        proxy.addTarget(network.slaveAddr, jobDataDict['rbuilderUrl'])
        container = ContainerWrapper(name, troveTups, mcfg,
                ccfg, loopManager, network)
        container.start(open('data').read())
        container.wait()

    finally:
        util.rmtree(loopDir)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
