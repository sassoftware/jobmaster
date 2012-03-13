#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

"""
Code for installing a jobslave into a target root.
"""

import copy
import logging
import os
import tempfile
from conary import callbacks
from conary import conaryclient
from conary.lib import util
from jobmaster.util import call, devNull, createFile

log = logging.getLogger(__name__)


def _status(callback, status, *args):
    if args:
        status %= args
    log.info(status)
    if callback:
        callback(status)


def buildRoot(ccfg, troveTups, destRoot, callback=None):
    destRoot = os.path.realpath(destRoot)
    fsRoot = tempfile.mkdtemp(prefix='temproot-',
            dir=os.path.dirname(destRoot))

    rootCfg = copy.deepcopy(ccfg)
    rootCfg.proxyMap = ccfg.proxyMap  # remove after conary 2.3.13
    rootCfg.root = fsRoot
    rootCfg.autoResolve = False
    #rootCfg.updateThreshold = 0

    rootClient = conaryclient.ConaryClient(rootCfg)
    try:
        try:
            os.mkdir(os.path.join(fsRoot, 'root'))

            _status(callback, "Preparing update job")
            rootClient.setUpdateCallback(UpdateCallback(callback))
            job = rootClient.newUpdateJob()
            jobTups = [(n, (None, None), (v, f), True)
                    for (n, v, f) in troveTups]
            rootClient.prepareUpdateJob(job, jobTups, resolveDeps=False)

            rootClient.applyUpdateJob(job,
                    tagScript=os.path.join(fsRoot, 'root/conary-tag-script'))

            _status(callback, "Running tag scripts")
            preTagScripts(fsRoot)
            runTagScripts(fsRoot)
            postTagScripts(fsRoot)

        finally:
            rootClient.close()
            job = None

        os.rename(fsRoot, destRoot)

    except:
        if os.path.isdir(fsRoot):
            util.rmtree(fsRoot)
        raise


def preTagScripts(fsRoot):
    """
    Prepare the image root for running tag scripts.
    """
    # Fix up rootdir permissions as tar actually restores them when
    # extracting.
    os.chmod(fsRoot, 0755)


def runTagScripts(fsRoot):
    pid = os.fork()
    if pid:
        _, status = os.waitpid(pid, 0)
        if status:
            raise RuntimeError("Failed to execute tag scripts")
        return

    try:
        try:
            # subprocess needs this to unpickle exceptions
            import encodings.string_escape
            encodings = encodings

            null = devNull()
            os.chroot(fsRoot)
            call('bash /root/conary-tag-script', ignoreErrors=True,
                    captureOutput=False, stdin=null, stdout=null, stderr=null)
            os._exit(0)
        except:
            log.exception("Failed to execute tag scripts:")
    finally:
        os._exit(70)


def postTagScripts(fsRoot):
    """
    Clean up after running tag scripts.
    """
    # tune2fs requires mtab to be present or it won't touch any block
    # devices (e.g. /dev/loop*)
    createFile(fsRoot, 'etc/mtab')


class UpdateCallback(callbacks.UpdateCallback):
    def __init__(self, cbmethod):
        callbacks.UpdateCallback.__init__(self)
        self.cbmethod = cbmethod

    def eatMe(self, *P, **K):
        pass

    tagHandlerOutput = troveScriptOutput = troveScriptFailure = eatMe

    def setUpdateHunk(self, hunk, total):
        status = "Applying update job %d of %d" % (hunk, total)
        log.info(status)
        if self.cbmethod:
            self.cbmethod(status)
