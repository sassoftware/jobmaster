#
# Copyright (c) 2011 rPath, Inc.
#

import math
import logging
import os
import pickle
import random
import signal
import simplejson
import sys
from conary import trovetup
from conary.conaryclient import ConaryClient
from conary.deps.deps import ThawFlavor
from conary.errors import TroveNotFound
from conary.lib.util import AtomicFile
from conary.versions import ThawVersion
from mcp import jobstatus
from jobmaster.resources.block import OutOfSpaceError
from jobmaster.resources.container import ContainerWrapper
from jobmaster.resources.network import NetworkPairResource
from jobmaster.response import ResponseProxy
from jobmaster.subprocutil import Subprocess
from jobmaster.util import prettySize

log = logging.getLogger(__name__)

MEBI = 1048576 # 1 MiB
GIBI = 1073741824 # 1 GiB


class JobHandler(Subprocess):
    procName = "job handler"
    setsid = True

    _catchSignals = [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]

    def __init__(self, master, job):
        self.cfg = master.cfg
        self.job = job
        self.job_data = simplejson.loads(job.job_data)
        self.uuid = job.uuid
        self.response = ResponseProxy(self.job.rbuilder_url, self.job_data)

        self.conaryCfg = master.getConaryConfig(job.rbuilder_url, cache=False)
        self.conaryClient = None
        self.loopManager = master.loopManager

        self.name = os.urandom(6).encode('hex')
        self.network = NetworkPairResource(master.addressGenerator, self.name)

        self.pid = None

    def run(self):
        log.info("Running job %s in pid %d", self.uuid, os.getpid())
        self.response.sendStatus(jobstatus.RUNNING,
                "Preparing build environment")
        random.seed()

        for line in self.job_data['project']['conaryCfg'].splitlines():
            self.conaryCfg.configLine(line)
        self.conaryClient = ConaryClient(self.conaryCfg)
        troveTup = self.findSlave()

        # Calculate how much scratch space will be required for this build.
        scratchSize = self.getScratchSize()

        # Allocate early resources.
        jobslave = ContainerWrapper(self.name, [troveTup], self.cfg,
                self.conaryClient, self.loopManager, self.network, scratchSize)
        ret = -1
        try:
            try:
                # Start up the container process and wait for it to finish.
                jobslave.start(self.job.job_data, self._cb_preparing)
                for signum in self._catchSignals:
                    signal.signal(signum, self._onSignal)
                ret = jobslave.wait()
            finally:
                # Ignore signals during cleanup to make sure an impatient
                # initscript or sysadmin doesn't cause stuck LVs.
                for signum in self._catchSignals:
                    signal.signal(signum, signal.SIG_IGN)
                jobslave.close()
        except OutOfSpaceError, err:
            log.error(str(err))
            self.failJob(str(err))
            ret = 0  # error handled
        except StopJob:
            log.info("Stopping job due to user request.")
            self.failJob("Job stopped by user")
            ret = 0 # error handled
        except:
            log.exception("Error starting jobslave for %s:", self.uuid)
            self.failJob("Error starting build environment. "
                    "Please check the jobmaster log for details.")
            ret = 0  # error handled

        if ret != 0:
            log.info("Job %s exited with status %d", self.uuid, ret)
            self.failJob("Job terminated unexpectedly")
        return 0

    def stop(self):
        """Terminate a running job handler."""
        self.kill(signal.SIGQUIT)

    def _onSignal(self, signum, sigtb):
        log.error("Received signal %d, cleaning up...", signum)
        if signum == signal.SIGQUIT:
            # Stop requested by user.
            raise StopJob()
        else:
            # Abnormal signal.
            raise RuntimeError("Job handler terminated by signal %d"
                    % (signum,))

    def failJob(self, reason):
        self.response.sendStatus(jobstatus.FAILED, reason)
        # Exit normally to indicate that we have handled the error.
        sys.exit(0)

    def _cb_preparing(self, status):
        self.response.sendStatus(jobstatus.RUNNING,
                "Preparing build environment: " + status)

    def findSlave(self):
        if '/' in self.cfg.troveVersion:
            version = self.cfg.troveVersion
        else:
            try:
                ver = self.conaryClient.db.findTrove(None,
                        ('rbuilder-mcp', None, None))[0][1]
                label = ver.trailingLabel()
            except TroveNotFound:
                log.error("Can't locate jobslave trove: no troveLabel "
                        "configured and no rbuilder-mcp installed")
                raise RuntimeError("Configuration error")
            version = '%s/%s' % (label, self.cfg.troveVersion)

        # Cache findTrove calls so images can be built even if the products
        # repository is down temporarily.
        cachePath = self.cfg.getVersionCachePath()
        try:
            cache = pickle.load(open(cachePath))
        except IOError:
            cache = {}

        troveSpec = ('group-jobslave', version, None)
        if troveSpec not in cache:
            repos = self.conaryClient.getRepos()
            try:
                cache[troveSpec] = sorted(repos.findTrove(None, troveSpec))[-1]
            except:
                log.exception("Failed to locate jobslave trove:")
                self.failJob("Could not locate the required build environment.")
            else:
                fobj = AtomicFile(cachePath)
                pickle.dump(cache, fobj, 2)
                fobj.commit()

        return cache[troveSpec]

    def _getTroveSize(self, spec=None):
        repos = self.conaryClient.getRepos()
        if spec is None:
            name = self.job_data['troveName'].encode('utf8')
            version = ThawVersion(self.job_data['troveVersion'].encode('utf8'))
            flavor = ThawFlavor(self.job_data['troveFlavor'].encode('utf8'))
        else:
            if isinstance(spec, unicode):
                spec = spec.encode('utf8')
            troveSpec = trovetup.TroveSpec.fromString(spec)
            troveTup = sorted(repos.findTrove(None, troveSpec))[-1]
            name, version, flavor = troveTup

        trove = repos.getTrove(name, version, flavor, withFiles=False)
        return trove.troveInfo.size()

    def getTroveSize(self):
        """
        Return the size, in bytes, of the image group.
        """
        try:
            troveSize = self._getTroveSize()
        except:
            log.exception("Failed to retrieve image group:")
            self.failJob("Failed to retrieve image group")
        if not troveSize:
            troveSize = GIBI
            log.warning("Trove has no size; using %s", prettySize(troveSize))
        return troveSize

    def getScratchSize(self):
        """
        Return the total bytes of scratch space to be requested.
        """

        def metaDataSlop(size):
            # Slop handling for normal filesystems, assume reasonable
            # inode balance
            return int(math.ceil((size + 20 * MEBI) * 1.15))

        def swapSlop(size):
            # Slop handling for swap files, just need to account for
            # indirect blocks, so 1% is overkill but not large
            return int(math.ceil((size + 20 * MEBI) * 1.01))

        troveSize = self.getTroveSize()

        data = self.job_data.get('data', {})
        buildType = self.job_data['buildType']
        freeSpace = int(data.get('freespace', 0)) * MEBI
        swapSpace = int(data.get('swapSize', 0)) * MEBI
        mountSpace = sum([x[0] + x[1] for x in data.get('mountDict', {})]
                ) * MEBI

        anacondaSpace = 0
        if buildType in (1, 16):
            anacondaSpace = 250 * MEBI
            anacondaTemplates = data.get('anaconda-templates', None)
            if anacondaTemplates:
                anacondaSpace = min(anacondaSpace,
                        self._getTroveSize(anacondaTemplates))

        # Pad 15% for filesystem overhead (inodes, etc.)
        packageSpace = metaDataSlop(troveSize + mountSpace)
        totalSize = (packageSpace +
                     freeSpace +
                     swapSlop(swapSpace) +
                     anacondaSpace)

        # Space to transform into image
        totalSize *= 2.5
        if buildType == 9: # buildtypes.VMWARE_ESX_IMAGE
            # Account for extra sparse image to be built
            totalSize += packageSpace + swapSlop(swapSpace)

        # Never allocate less than the configured minimum.
        totalSize = max(totalSize, self.cfg.minSlaveSize * MEBI)

        log.info("Allocating %s of scratch space for job %s in slave %s",
                prettySize(totalSize), self.uuid, self.name)
        return totalSize


class StopJob(Exception):
    """Thrown by signal handler when the user requests the job to stop."""
