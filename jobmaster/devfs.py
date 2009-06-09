#!/usr/bin/python
#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved
#

import errno
import fcntl
import logging
import os
import random
import threading
from jobmaster.resource import ResourceStack, AutoMountResource
from jobmaster.util import call

log = logging.getLogger(__name__)

LOOP_CLR_FD = 0x4C01


class DevFS(ResourceStack):
    def __init__(self, loopManager=None):
        ResourceStack.__init__(self)
        self.mountPoint = None
        self.devices = []
        self.loopManager = loopManager
        self.loops = None

    def start(self):
        mount = AutoMountResource('tmpfs', options=["-t", "tmpfs"],
                prefix='devfs-')
        self.append(mount)
        self.mountPoint = mount.mountPoint

        self.mknod('null',      'c', 1, 3)
        self.mknod('zero',      'c', 1, 5)
        self.mknod('random',    'c', 1, 8)
        self.mknod('urandom',   'c', 1, 9)

        if self.loopManager:
            self.loops = self.loopManager.get()
            for n, minor in enumerate(self.loops):
                self.mknod('loop%d' % n, 'b', 7, minor)

    def _close(self):
        if self.loops:
            self._freeLoops()
        ResourceStack._close(self)

    def _freeLoops(self):
        # Try N times to free N devices; that's enough to handle
        # nested devices but won't get stuck in really sticky cases.
        for tries in range(self.loopManager.chunk):
            goAgain = False
            for n, minor in enumerate(self.loops):
                path = os.path.join(self.mountPoint, 'loop%d' % n)
                try:
                    fcntl.ioctl(open(path), LOOP_CLR_FD)
                except IOError, err:
                    if err.errno == errno.ENXIO:
                        # No such device or address - OK
                        continue
                    elif err.errno == errno.EBUSY:
                        # Device or resource busy - try again
                        log.warning("Loop device %d is busy", n)
                        goAgain = True
                        continue
                    else:
                        # Something else
                        log.exception("Error freeing loop device:")
                else:
                    log.info("Freed loop device %d", n)

            if not goAgain:
                break

    def mknod(self, path, kind, major, minor):
        call(['/bin/mknod', os.path.join(self.mountPoint, path),
            kind, str(major), str(minor)])
        self.devices.append((path, kind, major, minor))

    def writeCaps(self, fObj):
        for _, kind, major, minor in self.devices:
            print >> fObj, 'lxc.cgroup.devices.allow = %s %s:%s rwm' % (
                    kind, major, minor)


class OutOfLoopDevices(RuntimeError):
    pass


class LoopAllocation(object):
    def __init__(self, manager, index, start, end):
        self.manager = manager
        self.index = index
        self.start = start
        self.end = end

    def __del__(self):
        self.manager.free(self.index)

    def __repr__(self):
        return '<LoopAllocation %d..%d>' % (self.start, self.end)

    def __iter__(self):
        return iter(xrange(self.start, self.end))


class LoopManager(object):
    def __init__(self, start=64, end=1024, chunk=8):
        assert start % chunk == 0
        assert end % chunk == 0
        self.start, self.end, self.chunk = start, end, chunk
        self._usageMap = [False] * ((end - start) / chunk)
        self._lock = threading.Lock()

    def get(self):
        self._lock.acquire()
        try:
            free = [n for n, used in enumerate(self._usageMap) if not used]
            if not free:
                raise OutOfLoopDevices()
            index = random.choice(free)
            self._usageMap[index] = True

            start = self.start + self.chunk * index
            return LoopAllocation(self, index, start, start + self.chunk)
        finally:
            self._lock.release()

    def free(self, index):
        self._lock.acquire()
        try:
            self._usageMap[index] = False
        finally:
            self._lock.release()
