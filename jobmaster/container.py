#!/usr/bin/python
#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import logging
import random
import sys
import tempfile
import time
from conary import conarycfg
from conary import conaryclient
from jobmaster.chroot import MountRoot
from jobmaster.config import MasterConfig
from jobmaster.networking import AddressGenerator, formatIPv6
from jobmaster.resource import ResourceStack, NetworkPairResource
from jobmaster.util import createFile, logCall, setupLogging

log = logging.getLogger(__name__)


class Container(ResourceStack):
    def __init__(self, troves, cfg, conaryCfg, loopManager=None):
        ResourceStack.__init__(self)

        self.troves = troves
        self.cfg = cfg

        self.name = '%08x' % random.getrandbits(32)

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
            config = tempfile.NamedTemporaryFile()
            print >> config, 'lxc.utsname = localhost'
            print >> config, 'lxc.rootfs = ' + root

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
        logCall(["/usr/bin/lxc-execute", "-n", self.name,
            "-f", self.config.name] + args, ignoreErrors=True,
            logCmd=logCmd, **kwargs)


def main(args):
    from conary import conaryclient
    from conary.conaryclient import cmdline
    from jobmaster.devfs import LoopManager

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

    loopManager = LoopManager()
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


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
