#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import math
import logging
import os
import random
import signal
import simplejson
import sys
from conary.conaryclient import ConaryClient
from conary.deps.deps import ThawFlavor
from conary.errors import TroveNotFound
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

    def __init__(self, master, job):
        self.cfg = master.cfg
        self.job = job
        self.job_data = simplejson.loads(job.job_data)
        self.uuid = job.uuid
        self.response = ResponseProxy(self.job.rbuilder_url, self.job_data)

        self.conaryCfg = master.getConaryConfig(job.rbuilder_url)
        for line in self.job_data['project']['conaryCfg'].splitlines():
            self.conaryCfg.configLine(line)
        self.conaryClient = ConaryClient(self.conaryCfg)
        self.loopManager = master.loopManager

        self.name = os.urandom(6).encode('hex')
        self.network = NetworkPairResource(master.addressGenerator, self.name)

        self.pid = None

    def run(self):
        log.info("Running job %s in pid %d", self.uuid, os.getpid())
        self.response.sendStatus(jobstatus.RUNNING,
                "Preparing build environment")
        random.seed()

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
                jobslave.start(self.job.job_data)
                signal.signal(signal.SIGTERM, self._onSignal)
                signal.signal(signal.SIGQUIT, self._onSignal)
                ret = jobslave.wait()
            finally:
                jobslave.close()
        except OutOfSpaceError, err:
            log.error(str(err))
            self.failJob(str(err))
            ret = 0  # error handled
        except:
            log.exception("Error starting jobslave for %s:", self.uuid)
            self.failJob("Error starting build environment. "
                    "Please check the jobmaster log for details.")
            ret = 0  # error handled

        if ret != 0:
            log.info("Job %s exited with status %d", self.uuid, ret)
            self.failJob("Job terminated unexpectedly")
        return 0

    def _onSignal(self, signum, sigtb):
        log.info("Terminating jobslave due to signal %d", signum)
        self.kill()

    def failJob(self, reason):
        self.response.sendStatus(jobstatus.FAILED, reason)
        # Exit normally to indicate that we have handled the error.
        sys.exit(0)

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

        troveSpec = ('group-jobslave', version, None)
        repos = self.conaryClient.getRepos()
        try:
            return sorted(repos.findTrove(None, troveSpec))[-1]
        except:
            log.exception("Failed to locate jobslave trove:")
            self.failJob("Could not locate the required build environment.")

    def getTroveSize(self):
        """
        Return the size, in bytes, of the image group.
        """
        name = self.job_data['troveName'].encode('utf8')
        version = ThawVersion(self.job_data['troveVersion'].encode('utf8'))
        flavor = ThawFlavor(self.job_data['troveFlavor'].encode('utf8'))

        repos = self.conaryClient.getRepos()
        try:
            trove = repos.getTrove(name, version, flavor, withFiles=False)
        except:
            log.exception("Failed to retrieve image group:")
            self.failJob("Failed to retrieve image group")
        troveSize = trove.troveInfo.size()
        if not troveSize:
            troveSize = GIBI
            log.warning("Trove has no size; using %s", prettySize(troveSize))
        return troveSize

    def getScratchSize(self):
        """
        Return the total bytes of scratch space to be requested.
        """
        troveSize = self.getTroveSize()

        data = self.job_data.get('data', {})
        freeSpace = int(data.get('freespace', 0)) * MEBI
        swapSpace = int(data.get('swapSize', 0)) * MEBI
        mountSpace = sum([x[0] + x[1] for x in data.get('mountDict', {})]
                ) * 1048576

        totalSize = troveSize + freeSpace + swapSpace + mountSpace
        # Pad 15% for filesystem overhead (inodes, etc.)
        totalSize = int(math.ceil((totalSize + 20 * MEBI) * 1.15))
        # Multiply by 4. A minimum of 2 should suffice, but this has not been
        # tested thoroughly.
        totalSize *= 4
        # Never allocate less than the configured minimum.
        totalSize = max(totalSize, self.cfg.minSlaveSize * MEBI)

        log.info("Allocating %s of scratch space for job %s",
                prettySize(totalSize), self.uuid)
        return totalSize
