#!/usr/bin/python
#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved
#

import logging
import os
import stat
import subprocess
from jobmaster.resource import Resource, ResourceStack
from jobmaster.resources.mount import AutoMountResource
from jobmaster.util import call, logCall, null

log = logging.getLogger(__name__)


class OutOfSpaceError(RuntimeError):
    def __init__(self, required, free):
        RuntimeError.__init__(self, required, free)
        self.required = required
        self.free = free

    def __str__(self):
        return ("Not enough scratch space for build: %d extents required but "
                "only %d free" % (self.required, self.free))


class LVMResource(Resource):
    """
    Resource for a LVM2 logical volume to be removed on close.
    """

    def __init__(self, devicePath):
        Resource.__init__(self)
        self.devicePath = devicePath

        try:
            devstat = os.stat(devicePath)
        except OSError:
            self._close()
            raise

        if not stat.S_ISBLK(devstat.st_mode):
            raise RuntimeError("%s is not a block device" % devicePath)

        self.device = devstat.st_rdev

    def _close(self):
        """
        Call C{lvremove} on close.
        """
        # Free loop devices
        # Caveats: can't handle nested loops, or loops allocated
        # non-sequentially (loop100 when loop99 has no node yet)
        proc = subprocess.Popen(['/sbin/losetup', '-a'],
                stdout=subprocess.PIPE, shell=False)
        for line in proc.stdout.readlines():
            node, dev = line.split()[:2]
            node = node[:-1]
            dev = dev.split(':')[0]
            dev = int(dev[1:-1], 16)
            if dev == self.device:
                log.warning("Loop device %s references LV; destroying.", node)
                logCall(['/sbin/losetup', '-d', node])

        logCall(['/usr/sbin/lvm', 'lvremove', '-f', self.devicePath])


class ScratchDisk(ResourceStack):
    def __init__(self, vgName, lvName, diskSize):
        ResourceStack.__init__(self)

        self.vgName = vgName
        self.lvName = lvName
        self.diskSize = diskSize

        self.lvPath = self.mountPoint = None

    def start(self):
        # Allocate LV
        self.lvPath = allocate_scratch(self.vgName, self.lvName, self.diskSize)
        self.append(LVMResource(self.lvPath))

        # Format
        logCall(["/sbin/mkfs.xfs", "-fq", self.lvPath])

        # Mount
        mount = AutoMountResource(self.lvPath, options=["-t", "xfs"])
        self.mountPoint = mount.mountPoint
        self.append(mount)


def allocate_scratch(vg_name, lv_name, disk_bytes):
    """
    Allocate one logical volume named C{lv_name} in the volume group C{vg_name}
    with a size of at least C{disk_bytes}.
    """
    # Determine how many free extents there are, and how big an extent is.
    ret = call(['/usr/sbin/lvm', 'vgs', '-o', 'extent_size,free_count',
        vg_name], logCmd=False)[1]
    ret = ret.splitlines()[1:]
    if not ret:
        raise RuntimeError("Volume group %s could not be read" % (vg_name,))

    extent_size, extents_free = ret[0].split()
    assert extent_size.endswith('M')
    extent_size = 1048576 * int(float(extent_size[:-1]))
    extents_free = int(extents_free)

    # Round requested size up to the nearest extent.
    extents_required = (int(disk_bytes) + extent_size - 1) / extent_size
    if extents_required > extents_free:
        raise OutOfSpaceError(extents_required, extents_free)

    logCall(['/usr/sbin/lvm', 'lvcreate', '-l', str(extents_required),
        '-n', lv_name, vg_name], stdout=null())
    return os.path.join('/dev', vg_name, lv_name)
