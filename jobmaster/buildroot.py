#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

"""
Code for installing a jobslave into a target root.
"""

import copy
import hashlib
import logging
import os
import subprocess
import sys
import tempfile
from conary import callbacks
from conary import conarycfg
from conary import conaryclient
from conary import updatecmd
from conary.conaryclient import cmdline
from conary.lib import util
from jobmaster.util import setupLogging, createFile

log = logging.getLogger(__name__)


def buildRoot(ccfg, troveTups, destRoot):
    destRoot = os.path.realpath(destRoot)
    fsRoot = tempfile.mkdtemp(prefix='temproot-',
            dir=os.path.dirname(destRoot))

    rootCfg = copy.deepcopy(ccfg)
    rootCfg.root = fsRoot
    rootCfg.autoResolve = False
    rootCfg.updateThreshold = 0

    rootClient = conaryclient.ConaryClient(rootCfg)
    try:
        try:
            os.mkdir(os.path.join(fsRoot, 'root'))

            log.info("Preparing update job")
            rootClient.setUpdateCallback(UpdateCallback())
            job = rootClient.newUpdateJob()
            jobTups = [(n, (None, None), (v, f), True) for (n, v, f) in troveTups]
            rootClient.prepareUpdateJob(job, jobTups)

            rootClient.applyUpdateJob(job,
                    tagScript=os.path.join(fsRoot, 'root/conary-tag-script'))

            log.info("Running tag scripts")
            preTagScripts(fsRoot)
            util.execute("/usr/sbin/chroot '%s' bash -c '"
                    "sh -x /root/conary-tag-script "
                    ">/root/conary-tag-script.output 2>&1'" % (fsRoot,))
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


def postTagScripts(fsRoot):
    """
    Clean up after running tag scripts.
    """


class UpdateCallback(callbacks.UpdateCallback):
    def eatMe(self, *P, **K):
        pass

    tagHandlerOutput = troveScriptOutput = troveScriptFailure = eatMe

    def setUpdateHunk(self, hunk, total):
        log.info('Applying update job %d of %d', hunk, total)
