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
    def __init__(self, vgName, lvName, diskSize):
        ResourceStack.__init__(self)

        self.vgName = vgName
        self.lvName = lvName
        self.diskSize = diskSize

        self.lvPath = self.mountPoint = None

    def start(self):
        # Allocate LV
        self.lvPath = '/dev/%s/%s' % (self.vgName, self.lvName)

        logCall(["/usr/sbin/lvm", "lvcreate", "-n", self.lvName,
            "-L", "%dM" % self.diskSize, self.vgName])
        self.append(LVMResource(self.lvPath))

        # Format
        logCall(["/sbin/mkfs.xfs", "-f", self.lvPath])

        # Mount
        mount = AutoMountResource(self.lvPath, options=["-t", "xfs"])
        self.mountPoint = mount.mountPoint
        self.append(mount)
