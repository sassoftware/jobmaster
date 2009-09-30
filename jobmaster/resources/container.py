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
from conary import conarycfg
from conary import conaryclient
from jobmaster.config import MasterConfig
from jobmaster.networking import AddressGenerator, formatIPv6
from jobmaster.resource import ResourceStack
from jobmaster.resources.chroot import MountRoot
from jobmaster.resources.network import NetworkPairResource
from jobmaster.util import createFile, logCall, setupLogging

log = logging.getLogger(__name__)


class Container(ResourceStack):
    def __init__(self, troves, cfg, conaryCfg, loopManager=None):
        ResourceStack.__init__(self)

        self.troves = troves
        self.cfg = cfg

        self.name = os.urandom(6).encode('hex')

        self.masterAddr, self.slaveAddr = AddressGenerator().generateHostPair()

        self.chroot = MountRoot(self.name, troves, cfg, conaryCfg,
                loopManager=loopManager)
        self.append(self.chroot)

        self.config = None
        self.started = False

    def start(self):
        if self.started:
            return

        try:
            # Build chroot
            self.chroot.start()
            root = self.chroot.mountPoint

            # Configure network devices
            self.append(NetworkPairResource('jm.' + self.name,
                self.masterAddr, 'js.' + self.name))

            # Write out system configuration
            proxyURL = 'http://[%s]:7778/conary/' % formatIPv6(
                    self.masterAddr[0])
            createFile(root, 'tmp/etc/conary/config.d/runtime',
                    'conaryProxy http %s\n'
                    'conaryProxy https %s\n'
                    % (proxyURL, proxyURL))

            # Write out LXC config
            config = tempfile.NamedTemporaryFile(prefix='lxc-')
            print >> config, 'lxc.utsname = localhost'
            #print >> config, 'lxc.rootfs = ' + root

            print >> config, 'lxc.cgroup.devices.deny = a'
            print >> config, 'lxc.cgroup.devices.allow = b *:* m' # allow mknod
            print >> config, 'lxc.cgroup.devices.allow = c *:* m' # allow mknod
            self.chroot.devFS.writeCaps(config)

            print >> config, 'lxc.network.type = phys'
            print >> config, 'lxc.network.name = eth0'
            print >> config, 'lxc.network.link = js.' + self.name
            print >> config, 'lxc.network.ipv6 = ' + (
                    formatIPv6(*self.slaveAddr))
            print >> config, 'lxc.network.flags = up'
            config.flush()
            self.config = config
        except:
            self.close()
            raise

        self.started = True

    def run(self, args, interactive=False, logCmd=True):
        self.start()
        if interactive:
            kwargs = dict(captureOutput=False, stdin=None)
        else:
            kwargs = dict()
        sys.stdin.read()
        logCall(["/usr/bin/lxc-execute", "-n", self.name,
            "-f", self.config.name, #"chroot", self.chroot.mountPoint
            ] + args,
            ignoreErrors=True, logCmd=logCmd, **kwargs)

    def createFile(self, path, contents, mode=0644):
        createFile(self.chroot.mountPoint, path, contents, mode)


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
        container = Container(troveTups, mcfg,
                conaryCfg=ccfg, loopManager=loopManager)
        _start = time.time()
        container.start()
        _end = time.time()

        print
        print 'Started in %.03f s' % (_end - _start)
        print 'Master IP:', formatIPv6(container.masterAddr[0])
        print 'Slave IP:', formatIPv6(container.slaveAddr[0])

        container.run(['/bin/bash'], interactive=True, logCmd=False)
    finally:
        util.rmtree(loopDir)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
