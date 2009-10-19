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
from jobmaster.resources.mount import AutoMountResource, BindMountResource
from jobmaster.util import call, logCall, devNull

log = logging.getLogger(__name__)


class OutOfSpaceError(RuntimeError):
    def __init__(self, required, free):
        RuntimeError.__init__(self, required, free)
        self.required = required
        self.free = free

    def __str__(self):
        return ("Not enough scratch space for build: %d extents required but "
                "only %d free" % (self.required, self.free))


class ScratchDisk(Resource):
    """
    Resource for a LVM2 logical volume to be removed on close.
    """

    def __init__(self, vgName, lvName, size):
        Resource.__init__(self)
        self.vgName = vgName
        self.lvName = lvName
        self.size = size
        self.devicePath = os.path.join('/dev', vgName, lvName)
        self.firstMount = None

    def start(self):
        # Allocate LV
        allocate_scratch(self.vgName, self.lvName, self.size)

        # Format
        logCall(["/sbin/mkfs.xfs", "-fq", self.devicePath])

    def _close(self):
        """
        Call C{lvremove} on close.
        """
        logCall(['/usr/sbin/lvm', 'lvremove', '-f', self.devicePath])

    def mount(self, path, readOnly=False):
        if self.firstMount:
            return BindMountResource(self.firstMount, path)
        else:
            self.firstMount = path
            return AutoMountResource(self.devicePath, path,
                    options=["-t", "xfs", "-o", "noatime"])


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
        '-n', lv_name, vg_name], stdout=devNull())


def get_scratch_lvs(vg_name):
    """
    Return a list of all scratch LVs.
    """

    ret = call(['/usr/sbin/lvm', 'lvs', '-o', 'name', vg_name])[1]
    ret = ret.splitlines()[1:]
    if not ret:
        raise RuntimeError("Volume group %s could not be read" % (vg_name,))

    return [x.strip() for x in ret if x.strip().startswith('scratch_')]
