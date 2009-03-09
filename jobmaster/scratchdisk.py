#!/usr/bin/python
#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved
#

import logging
import random
from jobmaster.resource import ResourceStack, LVMResource, AutoMountResource
from jobmaster.util import logCall

log = logging.getLogger(__name__)


class ScratchDisk(ResourceStack):
    def __init__(self, vgName, diskSize):
        ResourceStack.__init__(self)

        self.vgName = vgName
        self.diskSize = diskSize

        self.lvName = self.lvPath = self.mountPoint = None

        self._open()

    def _open(self):
        # Allocate LV
        self.lvName = 'scratch_%08x' % random.randint(0, 2**32 - 1)
        self.lvPath = '/dev/%s/%s' % (self.vgName, self.lvName)

        logCall(["/usr/sbin/lvm", "lvcreate", "-n", self.lvName,
            "-L", "%dK" % ((self.diskSize + 1023) / 1024), self.vgName])
        self.append(LVMResource(self.lvPath))

        # Format
        logCall(["/sbin/mkfs.xfs", "-f", self.lvPath])

        # Mount
        mount = AutoMountResource(["-t", "xfs", self.lvPath])
        self.mountPoint = mount.mountPoint
        self.append(mount)
