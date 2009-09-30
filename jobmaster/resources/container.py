#!/usr/bin/python
#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import logging
import os
import sys
import tempfile
import time
import traceback
from conary import conarycfg
from conary import conaryclient
from jobmaster import cgroup, linuxns
from jobmaster.config import MasterConfig
from jobmaster.resource import Resource, ResourceStack
from jobmaster.resources.block import ScratchDisk
from jobmaster.resources.chroot import BoundContentsRoot
from jobmaster.resources.devfs import DevFS
from jobmaster.resources.network import NetworkPairResource
from jobmaster.resources.tempdir import TempDir
from jobmaster.subprocutil import Subprocess
from jobmaster.util import createFile, logCall, setupLogging

log = logging.getLogger(__name__)


class ContainerWrapper(ResourceStack):
    """
    This resource stack creates and tears down all resources that live outside
    of the container process, specifically the scratch disk and contents root.
    """
    def __init__(self, troves, cfg, conaryCfg, loopManager=None):
        ResourceStack.__init__(self)

        self.cfg = cfg
        self.name = os.urandom(6).encode('hex')

        self.contents = BoundContentsRoot(troves, cfg, conaryCfg)
        self.append(self.contents)

        scratchSize = self.cfg.minSlaveSize
        self.scratch = ScratchDisk(cfg.lvmVolumeName, 'scratch_' + self.name,
                scratchSize * 1048576)
        self.append(self.scratch)

        self.devFS = DevFS(loopManager)
        self.append(self.devFS)

        self.network = NetworkPairResource(self.name)
        self.append(self.network)

        self.container = Container(self.name, cfg)
        self.append(self.container)

    def start(self):
        self.contents.start()
        self.scratch.start()
        self.devFS.start()
        self.network.start()

        pid = self.container.start(self.network,
                mounts=[
                    (self.contents, '', True),
                    (self.devFS, 'dev', True),
                    (self.scratch, 'tmp', False),
                    (self.scratch, 'var/tmp', False),
                    ])

        # Set up device capabilities and networking for the now-running cgroup
        #cgroup.clearDeviceCaps(pid)
        #cgroup.addDeviceCap(pid, perms='m') # allow mknod
        #self.devFS.writeCaps(pid)
        self.network.moveSlave(pid)

    def check(self):
        return self.container.check()

    def wait(self):
        return self.container.wait()

    def kill(self):
        return self.container.kill()


class Container(TempDir, Subprocess):
    def __init__(self, name, cfg): 
        TempDir.__init__(self, prefix='root-')
        self.name = name
        self.cfg = cfg
        self.pid = self.network = self.mounts = None

    def start(self, network, mounts):
        if self.pid:
            return
        self.network = network
        self.mounts = mounts

        self.pid = linuxns.clone(self._run_wrapper, (), new_uts=True,
                new_ipc=True, new_pid=True, new_net=True, new_user=True)
        return self.pid

    def _close(self):
        self.kill()

    def _run_wrapper(self):
        try:
            try:
                self._run()
                os._exit(0)
            except:
                traceback.print_exc()
        finally:
            os._exit(70)

    def _run(self):
        self.doMounts()
        self.writeConfigs()

        os.chroot(self.path)
        os.chdir('/')

        logCall(["/usr/bin/jobslave"], ignoreErrors=True, logCmd=True)

    def doMounts(self):
        for resource, path, readOnly in self.mounts:
            target = os.path.join(self.path, path)
            mount = resource.mount(target, readOnly)
            mount.release()
        logCall(['/bin/mount', 'proc', self.path + '/proc', '-t', 'proc'])
        logCall(['/bin/mount', 'sysfs', self.path + '/sys', '-t', 'sysfs'])

    def writeConfigs(self):
        proxyURL = ('http://[%s]:7778/conary/' %
                self.network.masterAddr.format(False))
        createFile(self.path, 'tmp/etc/conary/config.d/runtime',
                'conaryProxy http %s\n'
                'conaryProxy https %s\n'
                % (proxyURL, proxyURL))


def main(args):
    from conary.conaryclient import cmdline
    from conary.lib import util
    from jobmaster.resources.devfs import LoopManager

    if len(args) < 2:
        sys.exit("Usage: %s <cfg> <trovespec>+" % sys.argv[0])

    setupLogging(logging.DEBUG)

    cfgPath, = args[:1]
    troveSpecs = args[1:]

    mcfg = MasterConfig()
    mcfg.read(cfgPath)

    ccfg = conarycfg.ConaryConfiguration(True)
    ccfg.initializeFlavors()
    if mcfg.conaryProxy != 'self':
        ccfg.configLine('conaryProxy http %s' % mcfg.conaryProxy)
        ccfg.configLine('conaryProxy https %s' % mcfg.conaryProxy)
    cli = conaryclient.ConaryClient(ccfg)
    searchSource = cli.getSearchSource()

    specTups = [cmdline.parseTroveSpec(x) for x in troveSpecs]
    troveTups = [max(x) for x in searchSource.findTroves(specTups).values()]

    loopDir = tempfile.mkdtemp()
    try:
        loopManager = LoopManager(loopDir)
        container = ContainerWrapper(troveTups, mcfg,
                conaryCfg=ccfg, loopManager=loopManager)
        _start = time.time()
        container.start()
        _end = time.time()

        print
        print 'Started in %.03f s' % (_end - _start)
        print 'Master IP:', container.network.masterAddr.format(False)
        print 'Slave IP:', container.network.slaveAddr.format(False)

        container.wait()

    finally:
        util.rmtree(loopDir)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
