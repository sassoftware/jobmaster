#
# Copyright (c) 2011 rPath, Inc.
#

import fcntl
import logging
import os
from conary.lib.util import mkdirChain, AtomicFile, rmtree
from jobmaster import archiveroot
from jobmaster import buildroot
from jobmaster.resource import Resource
from jobmaster.resources.mount import BindMountResource
from jobmaster.subprocutil import Lockable

log = logging.getLogger(__name__)


class _ContentsRoot(Resource, Lockable):
    def __init__(self, troves, cfg, conaryClient):
        Resource.__init__(self)

        self.troves = troves
        self.cfg = cfg
        self.conaryClient = conaryClient

        archivePath = os.path.realpath(os.path.join(cfg.basePath, 'archive'))
        mkdirChain(archivePath)

        self._hash = self._getHash()
        self._archivePath = os.path.join(archivePath, self._hash) + '.tar.xz'

        # To be set by subclasses
        self._basePath = None
        self._lockPath = None
        self._statusPath = None

    def _getHash(self):
        if isinstance(self.troves, basestring):
            # Used by the cleanup script
            return self.troves
        else:
            return '--'.join(x[1].trailingRevision().version
                    for x in sorted(self.troves))

    def unpackRoot(self, fObj=None, prepareCB=None):
        if not fObj:
            fObj = self._archivePath
        archiveroot.unpackRoot(fObj, self._basePath, callback=prepareCB)

    def archiveRoot(self):
        log.info("Archiving root %s", self._hash)
        return archiveroot.archiveRoot(self._basePath, self._archivePath)

    def buildRoot(self, prepareCB=None):
        self._lock(fcntl.LOCK_EX)
        buildroot.buildRoot(self.conaryClient.cfg, self.troves, self._basePath,
                callback=prepareCB)

    def start(self):
        raise NotImplementedError

    def mount(self, path, readOnly=True):
        return BindMountResource(self._basePath, path, readOnly=readOnly)

    def delete(self):
        self._lock(fcntl.LOCK_EX)
        rmtree(self._basePath)
        self._deleteLock()


class BoundContentsRoot(_ContentsRoot):
    """
    This strategy maintains a single contents root which is to be bind-mounted
    read-only by users.
    """
    def __init__(self, troves, cfg, conaryClient):
        _ContentsRoot.__init__(self, troves, cfg, conaryClient)

        rootPath = os.path.realpath(os.path.join(cfg.basePath, 'roots'))
        mkdirChain(rootPath)
        self._basePath = os.path.join(rootPath, self._hash)
        self._lockPath = self._basePath + '.lock'
        self._statusPath = self._basePath + '.status'
        self._lastStatus = ''
        self._statusCB = None

    def _rootExists(self):
        return os.path.isdir(self._basePath)

    def _lockLoop(self):
        """Poll status from the handler building the root while waiting."""
        if self._statusCB:
            try:
                status = open(self._statusPath).read().strip()
            except IOError:
                status = ''
            if status != self._lastStatus:
                self._statusCB(status)
                self._lastStatus = status
        return False

    def _lockLoop2(self):
        """Poll status and break if the dir exists."""
        if self._rootExists():
            return True
        self._lockLoop()
        return False

    def start(self, prepareCB=None):
        # Grab a shared lock and check if the root exists.
        self._statusCB = prepareCB
        self._lockWait(fcntl.LOCK_SH, timeout=3600, breakIf=self._lockLoop)
        if self._rootExists():
            log.info("Using existing contents for root %s", self._hash)
            self._statusCB = None
            return

        # Now we need an exclusive lock to build the root. Drop the shared lock
        # before attempting to get the exclusive lock to ensure that another
        # process doing the same thing will not deadlock.
        self._lock(fcntl.LOCK_UN)
        log.debug("Acquiring exclusive lock on %s", self._basePath)
        self._lockWait(fcntl.LOCK_EX, timeout=3600, breakIf=self._lockLoop2)
        self._statusCB = None

        if self._rootExists():
            # Contents were created while waiting to acquire the lock.
            # Recursing is extremely paranoid, but it ensures that we get
            # confirmation that the root is present while holding a shared
            # lock.
            return self.start(prepareCB=prepareCB)

        # Hook the status callback to write to a file so that processes waiting
        # for us to finish can present it to the user.
        localCB = None
        if prepareCB:
            def localCB(msg):
                fObj = AtomicFile(self._statusPath)
                fObj.write(msg)
                fObj.commit()
                prepareCB(msg)
            self._statusCB = localCB

        if os.path.isfile(self._archivePath):
            # Check for an archived root. If it exists, unpack it and return.
            log.info("Unpacking contents for root %s", self._hash)
            self.unpackRoot(prepareCB=localCB)
        else:
            # Build the root from scratch.
            log.info("Building contents for root %s", self._hash)
            self.buildRoot(prepareCB=localCB)

        try:
            os.unlink(self._statusPath)
        except OSError:
            pass

        self._lock(fcntl.LOCK_SH)
