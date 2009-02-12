#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved
#

import os
import tempfile
from jobmaster.util import logCall


class Resource(object):
    """
    Base class for some sort of "resource" that must be freed both
    when done and when unwinding the stack (on exception).

    Typically, one would keep a stack of these, and close them in
    reverse order at the end of the section.
    """

    def __init__(self):
        self.closed = False

    def close(self):
        """
        Close the resource if it is not already closed.
        """
        if not self.closed:
            self._close()
            self.closed = True
    __del__ = close

    @staticmethod
    def _close():
        pass

    def release(self):
        """
        Release the resource by marking it as closed without actually
        destroying it, e.g. after a sucessful preparatory section.
        """
        if not self.closed:
            self.closed = True


class LVMResource(Resource):
    """
    Resource for a LVM2 logical volume to be removed on close.
    """

    def __init__(self, devicePath):
        Resource.__init__(self)
        self.devicePath = devicePath

    def _close(self):
        logCall(['/usr/sbin/lvm', 'lvremove', '-f', self.devicePath])


class MountResource(Resource):
    """
    Resource for a mounted partition to be unmounted on close.
    """

    def __init__(self, devicePath):
        Resource.__init__(self)
        self.devicePath = devicePath

    def _close(self):
        logCall(['/bin/umount', '-fd', self.devicePath])


class AutoMountResource(Resource):
    """
    Resource that mounts a device at a temp directory on start and
    unmounts and cleans up on close.
    """

    def __init__(self, options):
        Resource.__init__(self)

        self.mountPoint = tempfile.mkdtemp(prefix='mount-')
        try:
            logCall(['/bin/mount'] + list(options) + [self.mountPoint])
        except:
            os.rmdir(self.mountPoint)
            self.closed = True
            raise

    def _close(self):
        logCall(['/bin/umount', '-fd', self.mountPoint])
        os.rmdir(self.mountPoint)


class XenDomainResource(Resource):
    """
    Resource that destroys a xen domain on close.
    """

    def __init__(self, domain):
        Resource.__init__(self)
        self.domain = domain

    def _close(self):
        #logCall(['/usr/sbin/xm', 'destroy', self.domain])
        print 'DESTROYING', self.domain # XXX
