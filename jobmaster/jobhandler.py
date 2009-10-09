#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import logging
import os
import random
import signal
from conary.conaryclient import cmdline
from jobmaster.resources.container import ContainerWrapper
from jobmaster.subprocutil import Subprocess

log = logging.getLogger(__name__)


class JobHandler(Subprocess):
    procName = "job handler"
    setsid = True

    def __init__(self, master, job):
        self.cfg = master.cfg
        self.job = job

        self.conaryCfg = master.getConaryConfig(job.rbuilder_url)
        self.loopManager = master.loopManager
        self.proxyServer = master.proxyServer

        self.pid = None

    def run(self):
        log.info("Running job %s in pid %d", self.job.uuid, os.getpid())
        random.seed()

        from conary import conaryclient
        ccli = conaryclient.ConaryClient(self.conaryCfg)
        source = ccli.getSearchSource()
        troveSpec = cmdline.parseTroveSpec(self.cfg.troveSpec)
        troveTup = sorted(source.findTrove(troveSpec))[0]

        # Allocate early resources.
        jobslave = ContainerWrapper([troveTup], self.cfg, self.conaryCfg,
                self.loopManager)

        # Instruct the proxy to forward this slave's requests to the
        # originating rBuilder.
        address = jobslave.network.slaveAddr.format(False)
        self.proxyServer.addTarget(address, self.job.rbuilder_url)

        # Start up the container process and wait for it to finish.
        jobslave.start(self.job.job_data)
        signal.signal(signal.SIGTERM, self._onSignal)
        signal.signal(signal.SIGQUIT, self._onSignal)
        jobslave.wait()

        self.proxyServer.removeTarget(address)
        log.info("Job %s exited", self.job.uuid)

    def _onSignal(self, signum, sigtb):
        self.kill()
