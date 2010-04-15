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


def buildRoot(ccfg, troveTups, destRoot):
    destRoot = os.path.realpath(destRoot)
    fsRoot = tempfile.mkdtemp(prefix='temproot-',
            dir=os.path.dirname(destRoot))

    rootCfg = copy.deepcopy(ccfg)
    rootCfg.root = fsRoot
    rootCfg.autoResolve = False
    #rootCfg.updateThreshold = 0

    rootClient = conaryclient.ConaryClient(rootCfg)
    try:
        try:
            os.mkdir(os.path.join(fsRoot, 'root'))

            log.info("Preparing update job")
            rootClient.setUpdateCallback(UpdateCallback())
            job = rootClient.newUpdateJob()
            jobTups = [(n, (None, None), (v, f), True)
                    for (n, v, f) in troveTups]
            rootClient.prepareUpdateJob(job, jobTups, resolveDeps=False)

            rootClient.applyUpdateJob(job,
                    tagScript=os.path.join(fsRoot, 'root/conary-tag-script'))

            log.info("Running tag scripts")
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
    def eatMe(self, *P, **K):
        pass

    tagHandlerOutput = troveScriptOutput = troveScriptFailure = eatMe

    def setUpdateHunk(self, hunk, total):
        log.info('Applying update job %d of %d', hunk, total)
