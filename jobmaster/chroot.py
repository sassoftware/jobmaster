import errno
import fcntl
import logging
import os
import random
import shutil
import sys
import time
from conary import conarycfg
from jobmaster import archiveroot
from jobmaster import buildroot
from jobmaster.scratchdisk import ScratchDisk
from jobmaster.resource import (Resource, ResourceStack,
        AutoMountResource, BindMountResource)
from jobmaster.util import setupLogging, specHash

log = logging.getLogger(__name__)


class LockError(RuntimeError):
    pass


class LockTimeoutError(LockError):
    pass


class ContentsRoot(Resource):
    def __init__(self, troves, rootPath, archivePath=None, archiveRoots=True,
            conaryCfg=None):
        Resource.__init__(self)

        self.troves = troves
        self.rootPath = os.path.realpath(rootPath)
        if archivePath:
            self.archivePath = os.path.realpath(archivePath)
        else:
            self.archivePath = rootPath
        self.archiveRoots = archiveRoots
        self.conaryCfg = conaryCfg
        
        self._hash = specHash(troves)
        self._basePath = os.path.join(rootPath, self._hash)
        self._lockFile = None
        self._lockLevel = fcntl.LOCK_UN

    @staticmethod
    def _sleep():
        time.sleep(random.uniform(0.1, 0.5))

    def _lock(self, mode=fcntl.LOCK_SH):
        # Short-circuit if we already have the lock
        if mode == self._lockLevel:
            return True

        if not self._lockFile:
            self._lockFile = open(self._basePath + '.lock', 'w')

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

        return oldLevel

    def _lockWait(self, mode=fcntl.LOCK_SH, timeout=600.0):
        logged = False
        runUntil = time.time() + timeout
        while True:
            # First, try to lock.
            try:
                return self._lock(mode)
            except LockError:
                pass

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

    def _getArchivePath(self):
        return os.path.join(self.archivePath, self._hash + '.tar.lzma')

    def getRoot(self):
        # Grab a shared lock and check if the root exists.
        self._lockWait(fcntl.LOCK_SH)
        if os.path.isdir(self._basePath):
            log.info("Using existing contents for root %s", self._hash)
            return self._basePath

        # Need an exclusive lock to unpack or build the root. There's no
        # race here as other processes would need an exclusive lock to create
        # the directory, and we already hold a shared lock.
        log.debug("Acquiring exclusive lock on %s", self._basePath)
        self._lockWait(fcntl.LOCK_EX)

        if os.path.isfile(self._getArchivePath()):
            # Check for an archived root. If it exists, unpack it and return.
            log.info("Unpacking contents for root %s", self._hash)
            self.unpackRoot()
            self._lock(fcntl.LOCK_SH)

            return self._basePath
        else:
            # Build the root from scratch.
            log.info("Building contents for root %s", self._hash)
            self.buildRoot()
            self._lock(fcntl.LOCK_SH)

            if self.archiveRoots:
                # Fork and archive the root.
                self.archiveRoot()

            return self._basePath

    def unpackRoot(self):
        self._lock(fcntl.LOCK_EX)
        archiveroot.unpackRoot(self._getArchivePath(), self._basePath)

    def archiveRoot(self):
        pid = os.fork()
        if not pid:
            # TODO: double-fork
            try:
                try:
                    log.info("Archiving root %s", self._hash)

                    # Re-acquire the lock under the child process so the parent
                    # doesn't yank the root out from under us.
                    self._lockFile.close()
                    self._lockFile = None
                    self._lockWait()

                    archiveroot.archiveRoot(self._basePath,
                            self._getArchivePath())

                    log.debug("Archiving of root %s done", self._hash)
                    os._exit(0)
                except:
                    log.exception("Error archiving root at %s :",
                            self._basePath)
            finally:
                os._exit(1)
        else:
            os.waitpid(pid, 0)

    def buildRoot(self):
        self._lock(fcntl.LOCK_EX)

        conaryCfg = self.conaryCfg
        if not conaryCfg:
            conaryCfg = conarycfg.ConaryConfiguration(True)

        buildroot.buildRoot(conaryCfg, self.troves, self._basePath)


class MountRoot(ResourceStack):
    def __init__(self, troves, rootPath, scratchVG, scratchSize,
            archivePath=None, archiveRoots=True, conaryCfg=None):
        ResourceStack.__init__(self)

        self.scratchVG = scratchVG
        self.scratchSize = scratchSize

        self.mountPoint = None

        self.contents = ContentsRoot(troves, rootPath, archivePath,
                archiveRoots, conaryCfg)
        self.append(self.contents)

        self._open()

    def _open(self):
        try:
            scratch = ScratchDisk(self.scratchVG, self.scratchSize)
            self.append(scratch)

            root = BindMountResource(self.contents.getRoot(), readOnly=True)
            self.append(root)
            self.mountPoint = root.mountPoint

            self.append(BindMountResource(scratch.mountPoint,
                os.path.join(self.mountPoint, 'tmp')))
            self.append(BindMountResource(scratch.mountPoint,
                os.path.join(self.mountPoint, 'var/tmp')))
            self.append(AutoMountResource('proc',
                os.path.join(self.mountPoint, 'proc'), ('-t', 'proc')))

        except:
            self.close()
            raise


def main(args):
    from conary import conaryclient
    from conary.conaryclient import cmdline

    if len(args) < 3:
        sys.exit("Usage: %s <buildroot> <vg> <trovespec>+" % sys.argv[0])

    setupLogging(logging.DEBUG)

    root, vg = args[:2]
    troveSpecs = args[2:]

    cfg = conarycfg.ConaryConfiguration(True)
    cfg.initializeFlavors()
    cli = conaryclient.ConaryClient(cfg)
    searchSource = cli.getSearchSource()

    specTups = [cmdline.parseTroveSpec(x) for x in troveSpecs]
    troveTups = [max(x) for x in searchSource.findTroves(specTups).values()]

    #root = ContentsRoot(troveTups, root, conaryCfg=cfg)
    #print root.getRoot()

    root = MountRoot(troveTups, root, vg, 16 * 1024 * 1024, conaryCfg=cfg)

    print 'Mounted at %s -- Ctrl-D to clean up' % root.mountPoint
    sys.stdin.read()

    root.close()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
