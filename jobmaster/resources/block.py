#!/usr/bin/python
#
# Copyright (c) 2011 rPath, Inc.
#

import logging
import os
from jobmaster.resource import Resource
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
        if os.path.exists(self.devicePath):
            logCall(['/sbin/lvm', 'lvremove', '-f', self.devicePath])

    def mount(self, path, readOnly=False, delete=False):
        if self.firstMount:
            return BindMountResource(self.firstMount, path, delete=delete)
        else:
            self.firstMount = path
            return AutoMountResource(self.devicePath, path,
                    options=["-t", "xfs", "-o", "noatime,barrier=0"],
                    delete=delete,
                    )


def allocate_scratch(vg_name, lv_name, disk_bytes):
    """
    Allocate one logical volume named C{lv_name} in the volume group C{vg_name}
    with a size of at least C{disk_bytes}.
    """
    # Determine how many free extents there are, and how big an extent is.
    ret = call(['/sbin/lvm', 'vgs', '-o', 'extent_size,free_count',
        vg_name], logCmd=False)[1]
    ret = ret.splitlines()[1:]
    if not ret:
        raise RuntimeError("Volume group %s could not be read" % (vg_name,))

    extent_size, extents_free = ret[0].split()
    assert extent_size.upper().endswith('M')
    extent_size = 1048576 * int(float(extent_size[:-1]))
    extents_free = int(extents_free)

    # Round requested size up to the nearest extent.
    extents_required = (int(disk_bytes) + extent_size - 1) / extent_size
    if extents_required > extents_free:
        raise OutOfSpaceError(extents_required, extents_free)

    logCall(['/sbin/lvm', 'lvcreate', '-l', str(extents_required),
        '-n', lv_name, vg_name], stdout=devNull())


def get_scratch_lvs(vg_name):
    """
    Return a list of all scratch LVs.
    """

    ret, stdout, _ = call(
            ['/sbin/lvm', 'lvs', '-o', 'name', vg_name], ignoreErrors=True)
    if ret:
        raise RuntimeError("Volume group %s could not be read" % (vg_name,))

    return [x.strip() for x in stdout.splitlines()
            if x.strip().startswith('scratch_')]
