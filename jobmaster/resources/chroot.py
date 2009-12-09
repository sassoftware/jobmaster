#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import conary.trove
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
from jobmaster.subprocutil import Lockable
from jobmaster.util import setupLogging, specHash

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

    def _getHash(self):
        repos = self.conaryClient.getRepos()
        buildTimes = [x() for x in repos.getTroveInfo(
            conary.trove._TROVEINFO_TAG_BUILDTIME, self.troves)]
        return specHash(self.troves, buildTimes)

    def unpackRoot(self, fObj=None):
        if not fObj:
            fObj = self._archivePath
        archiveroot.unpackRoot(fObj, self._basePath)

    def archiveRoot(self):
        log.info("Archiving root %s", self._hash)
        return archiveroot.archiveRoot(self._basePath, self._archivePath)

    def buildRoot(self):
        self._lock(fcntl.LOCK_EX)
        buildroot.buildRoot(self.conaryClient.cfg, self.troves, self._basePath)

    def start(self):
        raise NotImplementedError

    def mount(self, path, readOnly=True):
        return BindMountResource(self._basePath, path, readOnly=readOnly)


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

    def _rootExists(self):
        return os.path.isdir(self._basePath)

    def start(self):
        # Grab a shared lock and check if the root exists.
        self._lockWait(fcntl.LOCK_SH)
        if self._rootExists():
            log.info("Using existing contents for root %s", self._hash)
            return

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


class ArchiveContentsRoot(_ContentsRoot):
    """
    This strategy maintains an archive and unpacks it once for each user, so
    the resulting roots can be modified.
    """
    def __init__(self, troves, cfg, conaryClient):
        _ContentsRoot.__init__(self, troves, cfg, conaryClient)

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

    def start(self):
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
                return
            self._lock(fcntl.LOCK_UN)

        log.info("Unpacking contents for root %s", self._hash)
        self.unpackRoot(fObj)
