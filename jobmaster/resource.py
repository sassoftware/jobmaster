#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved
#

"""
Defines several "resources" -- objects that are closed in LIFO order on
error or at the end of the task.

Typical resources include LVM volumes, mount points, and virtual machines.
These need to be cleaned up in reverse order, e.g. first stop the VM, then
unmount its disk, then destroy the disk. So, one would create a stack of
resources (e.g. via C{ResourceStack}), push each resource onto the stack as
it is allocated, and pop each resource on shutdown to free it.
"""

import glob
import logging
import os
import stat
import subprocess
import tempfile
from jobmaster.networking import formatIPv6
from jobmaster.util import call, logCall, CommandError

log = logging.getLogger(__name__)


class Resource(object):
    """
    Base class for some sort of "resource" that must be freed both
    when done and when unwinding the stack (on exception).

    Typically, one would keep a stack of these, and close them in
    reverse order at the end of the section.
    """
    closed = False

    def close(self):
        """
        Close the resource if it is not already closed.
        """
        if not self.closed:
            self._close()
            self.closed = True
    __del__ = close

    def _close(self):
        "Override this to add cleanup functionality."

    def release(self):
        """
        Release the resource by marking it as closed without actually
        destroying it, e.g. after a sucessful preparatory section.
        """
        if not self.closed:
            self._release()
            self.closed = True

    def _release(self):
        "Override this to add extra on-release handling."


class ResourceStack(Resource):
    """
    A stack of resources that itself acts as a resource.
    """
    resources = None

    def __init__(self, resources=None):
        Resource.__init__(self)
        if resources:
            self.resources = resources
        else:
            self.resources = []

    def append(self, resource):
        """
        Add a new C{resource} to the top of the stack.
        """
        self.resources.append(resource)

    def _close(self):
        """
        Close each resource in LIFO order.
        """
        while self.resources:
            self.resources.pop().close()

    def _release(self):
        """
        Release each resource in LIFO order.
        """
        while self.resources:
            self.resources.pop().release()


class LVMResource(Resource):
    """
    Resource for a LVM2 logical volume to be removed on close.
    """

    def __init__(self, devicePath):
        Resource.__init__(self)
        self.devicePath = devicePath

        try:
            st = os.stat(devicePath)
        except OSError:
            self._close()
            raise

        if not stat.S_ISBLK(st.st_mode):
            raise RuntimeError("%s is not a block device" % devicePath)

        self.device = st.st_rdev

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


class MountResource(Resource):
    """
    Resource for a mounted partition to be unmounted on close.
    """

    def __init__(self, mountPoint, **kwargs):
        Resource.__init__(self)
        self.mountPoint = mountPoint
        self.delete = kwargs.pop('delete', False)
        if kwargs:
            raise TypeError("Unknown keyword argument %s" % kwargs.keys()[0])

    def _close(self):
        """
        Call C{umount} on close, optionally deleting the mount point.
        """
        call(['/bin/umount', '-fdn', self.mountPoint])
        if self.delete:
            os.rmdir(self.mountPoint)


class AutoMountResource(MountResource):
    """
    Resource that mounts a device at a temp directory on start and
    unmounts and cleans up on close.
    """

    def __init__(self, device, mountPoint=None, **kwargs):
        self.device = device
        self.options = kwargs.pop('options', ())

        if mountPoint is None:
            prefix = kwargs.pop('prefix', 'mount-')
            mountPoint = tempfile.mkdtemp(prefix=prefix)
            kwargs['delete'] = True

        MountResource.__init__(self, mountPoint, **kwargs)

        try:
            self._doMount()
        except:
            try:
                if self.delete:
                    os.rmdir(self.mountPoint)
            except:
                log.exception("Error deleting mountpoint:")
            self.closed = True
            raise

    def _doMount(self):
        """
        Mount using the provided options.
        """
        logCall(['/bin/mount', '-n', self.device, self.mountPoint]
                + list(self.options))


class BindMountResource(AutoMountResource):
    """
    Resource that mounts an existing directory at a new point, possibly
    read-only, and cleans up on close.
    """

    def __init__(self, fromPath, mountPoint=None, **kwargs):
        self.readOnly = kwargs.pop('readOnly', False)
        AutoMountResource.__init__(self, fromPath, mountPoint, **kwargs)

    def _doMount(self):
        """
        Bind-mount, then remount read-only if requested.
        """
        cmd = ['/bin/mount', '-n', '--bind', self.device, self.mountPoint]
        logCall(cmd)
        try:
            if self.readOnly:
                # Bind mounts can be read-only, but it is only effective if you
                # re-mount.
                cmd += ['-o', 'remount,ro']
                logCall(cmd)
        except:
            call(['/bin/umount', '-fn', self.mountPoint],
                    ignoreErrors=True)
            raise


class LinuxContainerResource(Resource):
    """
    Resource that destroys a linux container on close.
    """

    def __init__(self, container):
        Resource.__init__(self)
        self.container = container

    def _close(self):
        """
        Stop and destroy a linux container on close.
        """
        logCall(['/usr/bin/lxc-stop', '-n', self.container], ignoreErrors=True)
        logCall(['/usr/bin/lxc-destroy', '-n', self.container])


class NetworkPairResource(Resource):
    """
    Resource that sets up and tears down a veth network pair.
    """

    def __init__(self, masterName, masterAddr, slaveName):
        Resource.__init__(self)
        self.masterName = masterName
        self.masterAddr = masterAddr
        self.slaveName = slaveName

        logCall(['/sbin/ip', 'link', 'add',
            'name', masterName, 'type', 'veth', 'peer', 'name', slaveName])
        logCall(['/sbin/ip', 'addr', 'add', formatIPv6(*masterAddr),
            'dev', masterName])
        logCall(['/sbin/ip', 'link', 'set', masterName, 'up'])

    def _close(self):
            try:
                call(['/sbin/ip', 'link', 'del', self.masterName])
            except CommandError:
                # Checking first is racy, so check afterwards and only raise
                # if it's still present
                if os.path.isdir(os.path.join('/sys/class/net',
                        self.masterName)):
                    raise
