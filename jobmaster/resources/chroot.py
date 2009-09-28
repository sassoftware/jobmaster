#!/usr/bin/python
#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import errno
import fcntl
import logging
import os
import random
import sys
import tempfile
import time
from conary import conarycfg
from conary.lib.util import mkdirChain, rmtree
from jobmaster import archiveroot
from jobmaster import buildroot
from jobmaster.config import MasterConfig
from jobmaster.resource import Resource, ResourceStack
from jobmaster.resources.block import ScratchDisk
from jobmaster.resources.devfs import DevFS
from jobmaster.resources.mount import BindMountResource
from jobmaster.util import setupLogging, specHash

log = logging.getLogger(__name__)


class LockError(RuntimeError):
    pass


class LockTimeoutError(LockError):
    pass


class _ContentsRoot(Resource):
    def __init__(self, troves, cfg, conaryCfg):
        Resource.__init__(self)

        self.troves = troves
        self.cfg = cfg
        self.conaryCfg = conaryCfg

        archivePath = os.path.realpath(os.path.join(cfg.basePath, 'archive'))
        mkdirChain(archivePath)

        self._hash = specHash(troves)
        self._archivePath = os.path.join(archivePath, self._hash) + '.tar.xz'
        self._lockFile = None
        self._lockLevel = fcntl.LOCK_UN

        # To be set by subclasses
        self._basePath = None
        self._lockPath = None

    @staticmethod
    def _sleep():
        time.sleep(random.uniform(0.1, 0.5))

    def _lock(self, mode=fcntl.LOCK_SH):
        # Short-circuit if we already have the lock
        if mode == self._lockLevel:
            return True

        if not self._lockFile:
            self._lockFile = open(self._lockPath, 'w')

        oldLevel = self._lockLevel

        try:
            try:
                fcntl.flock(self._lockFile.fileno(), mode | fcntl.LOCK_NB)
            except IOError, err:
                if err.errno in (errno.EACCES, errno.EAGAIN):
                    # Already locked, retry later.
                    raise LockError()
                raise
            else:
                self._lockLevel = mode

        finally:
            if mode == fcntl.LOCK_UN:
                # If we don't have any lock at the moment then close the file
                # so that if another process deletes the lockfile we don't end
                # up locking the now-nameless file. The other process *must*
                # hold an exclusive lock to delete the lockfile, so this
                # assures lock safety.
                self._lockFile.close()
                self._lockFile = None

        return True

    def _lockWait(self, mode=fcntl.LOCK_SH, timeout=600.0, breakIf=None):
        logged = False
        runUntil = time.time() + timeout
        while True:
            # First, try to lock.
            try:
                return self._lock(mode)
            except LockError:
                pass

            if breakIf and breakIf():
                return False

            if time.time() > runUntil:
                raise LockTimeoutError()

            if not logged:
                logged = True
                log.debug("Waiting for lock")

            self._sleep()

    def _close(self):
        if self._lockFile:
            self._lockFile.close()
            self._lockFile = None

    def unpackRoot(self, fObj=None):
        if not fObj:
            fObj = self._archivePath
        archiveroot.unpackRoot(fObj, self._basePath)

    def archiveRoot(self):
        log.info("Archiving root %s", self._hash)
        return archiveroot.archiveRoot(self._basePath, self._archivePath)

    def buildRoot(self):
        self._lock(fcntl.LOCK_EX)
        buildroot.buildRoot(self.conaryCfg, self.troves, self._basePath)

    def getRoot(self):
        raise NotImplementedError


class BoundContentsRoot(_ContentsRoot):
    """
    This strategy maintains a single contents root which is to be bind-mounted
    read-only by users.
    """
    def __init__(self, troves, cfg, conaryCfg):
        _ContentsRoot.__init__(self, troves, cfg, conaryCfg)

        rootPath = os.path.realpath(os.path.join(cfg.basePath, 'roots'))
        mkdirChain(rootPath)
        self._basePath = os.path.join(rootPath, self._hash)
        self._lockPath = self._basePath + '.lock'

    def _rootExists(self):
        return os.path.isdir(self._basePath)

    def getRoot(self):
        # Grab a shared lock and check if the root exists.
        self._lockWait(fcntl.LOCK_SH)
        if self._rootExists():
            log.info("Using existing contents for root %s", self._hash)
            return self._basePath

        # Now we need an exclusive lock to build the root. Drop the shared lock
        # before attempting to get the exclusive lock to ensure that another
        # process doing the same thing will not deadlock.
        self._lock(fcntl.LOCK_UN)
        log.debug("Acquiring exclusive lock on %s", self._basePath)
        self._lockWait(fcntl.LOCK_EX, breakIf=self._rootExists)

        if self._rootExists():
            # Contents were created while waiting to acquire the lock.
            # Recursing is extremely paranoid, but it ensures that we get
            # confirmation that the root is present while holding a shared
            # lock.
            return self.getRoot()

        if os.path.isfile(self._archivePath):
            # Check for an archived root. If it exists, unpack it and return.
            log.info("Unpacking contents for root %s", self._hash)
            self.unpackRoot()
        else:
            # Build the root from scratch.
            log.info("Building contents for root %s", self._hash)
            self.buildRoot()

        self._lock(fcntl.LOCK_SH)
        return self._basePath


class ArchiveContentsRoot(_ContentsRoot):
    """
    This strategy maintains an archive and unpacks it once for each user, so
    the resulting roots can be modified.
    """
    def __init__(self, troves, cfg, conaryCfg):
        _ContentsRoot.__init__(self, troves, cfg, conaryCfg)

        self._lockPath = self._archivePath + '.lock'
        self._basePath = tempfile.mkdtemp(prefix='contents-')

    def _close(self):
        _ContentsRoot._close(self)
        if self._basePath:
            rmtree(self._basePath)
            self._basePath = None

    def _archiveExists(self):
        return os.path.isfile(self._archivePath)

    def _openArchive(self):
        try:
            return open(self._archivePath, 'rb')
        except IOError, err:
            if err.errno == errno.ENOENT:
                return None
            else:
                raise

    def getRoot(self):
        fObj = self._openArchive()
        if not fObj:
            log.debug("Acquiring exclusive lock on %s", self._archivePath)
            self._lockWait(fcntl.LOCK_EX, breakIf=self._archiveExists)
            if not self._archiveExists():
                log.info("Building contents for root %s", self._hash)
                self.buildRoot()
                log.info("Archiving root %s", self._hash)
                self.archiveRoot()
                # At this point we already have the root we need, so just
                # return.
                self._lock(fcntl.LOCK_UN)
                return self._basePath
            self._lock(fcntl.LOCK_UN)

        log.info("Unpacking contents for root %s", self._hash)
        self.unpackRoot(fObj)
        return self._basePath


class MountRoot(ResourceStack):
    def __init__(self, name, troves, cfg, conaryCfg, scratchSize=0,
            loopManager=None):
        ResourceStack.__init__(self)

        self.name = name
        self.troves = troves
        self.cfg = cfg
        self.scratchSize = scratchSize

        self.mountPoint = None

        self.contents = ArchiveContentsRoot(troves, cfg, conaryCfg)
        self.append(self.contents)

        scratchSize = max(self.scratchSize, self.cfg.minSlaveSize)
        self.scratch = ScratchDisk(self.cfg.lvmVolumeName,
                'scratch_' + self.name, scratchSize * 1048576)
        self.append(self.scratch)

        self.devFS = DevFS(loopManager)
        self.append(self.devFS)

    def start(self):
        try:
            contentsPath = self.contents.getRoot()

            self.scratch.start()
            self.devFS.start()

            root = BindMountResource(contentsPath, readOnly=True,
                    prefix='root-')
            self.append(root)
            self.mountPoint = root.mountPoint

            self.append(BindMountResource(self.devFS.mountPoint,
                os.path.join(self.mountPoint, 'dev'), readOnly=True))

            self.append(BindMountResource(self.scratch.mountPoint,
                os.path.join(self.mountPoint, 'tmp')))
            self.append(BindMountResource(self.scratch.mountPoint,
                os.path.join(self.mountPoint, 'var/tmp')))

        except:
            self.close()
            raise


def main(args):
    from conary import conaryclient
    from conary.conaryclient import cmdline

    if len(args) < 3:
        sys.exit("Usage: %s <cfg> <name> <trovespec>+" % sys.argv[0])

    setupLogging(logging.DEBUG)

    cfgPath, name = args[:2]
    troveSpecs = args[2:]

    mcfg = MasterConfig()
    mcfg.read(cfgPath)

    ccfg = conarycfg.ConaryConfiguration(True)
    ccfg.initializeFlavors()
    cli = conaryclient.ConaryClient(ccfg)
    searchSource = cli.getSearchSource()

    specTups = [cmdline.parseTroveSpec(x) for x in troveSpecs]
    troveTups = [max(x) for x in searchSource.findTroves(specTups).values()]


    root = MountRoot(name, troveTups, mcfg, conaryCfg=ccfg)

    _start = time.time()
    root.start()
    _end = time.time()

    print 'Mounted at %s in %.03f s -- Ctrl-D to clean up' % (
            root.mountPoint, _end - _start)
    sys.stdin.read()

    root.close()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
