#
# Copyright (c) 2011 rPath, Inc.
#

import errno
import logging
import os
import tempfile
import time
from jobmaster.resource import Resource
from jobmaster.util import logCall, CommandError

log = logging.getLogger(__name__)


def _umount(path):
    retries = 5
    for x in range(retries):
        try:
            logCall(['/bin/umount', '-dn', path])
            return True
        except CommandError:
            if x == retries:
                log.error("Failed to unmount %s, giving up", path)
            else:
                log.error("Failed to unmount %s, trying again", path)
                time.sleep(1)


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
        _umount(self.mountPoint)
        if self.delete:
            try:
                os.rmdir(self.mountPoint)
            except OSError, err:
                if err.errno == errno.EBUSY:
                    log.error("Could not delete mount point %s", self.mountPoint)
                else:
                    raise


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
            _umount(self.mountPoint)
            raise


class MountableDirectory(object):
    """
    Dummy object for container's doMounts() method to use to mount an existing
    directory.
    """
    def __init__(self, path):
        self.path = path

    def mount(self, target, readOnly=True):
        return BindMountResource(self.path, target, readOnly=readOnly)
