#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import logging
import os
import random
import signal

from jobmaster.resources.container import ContainerWrapper
from jobmaster.subprocutil import Subprocess

log = logging.getLogger(__name__)


class JobHandler(Subprocess):
    def __init__(self, master, job):
        self.cfg = master.cfg
        self.job = job

        self.conaryCfg = master.getConaryConfig(job.rbuilder_url)
        self.loopManager = master.loopManager

        self.pid = None

    def start(self):
        self.pid = os.fork()
        if not self.pid:
            try:
                try:
                    os.setsid()
                    self._run()
                except:
                    log.exception("Unhandled exception in job handler:")
            finally:
                os._exit(0)
        return self.pid

    def _run(self):
        log.info("Running job %s in pid %d", self.job.uuid, os.getpid())
        random.seed()

        from conary import conaryclient
        ccli = conaryclient.ConaryClient(self.conaryCfg)
        repos = ccli.getRepos()
        troveTup = repos.findTrove(None, ('group-jobslave',
            'lkg.rb.rpath.com@rpath:rba-dexen-js', None))[0]
        jobslave = ContainerWrapper([troveTup], self.cfg, self.conaryCfg,
                self.loopManager)
        jobslave.start(self.job.job_data)
        signal.signal(signal.SIGTERM, self._onSignal)
        signal.signal(signal.SIGQUIT, self._onSignal)
        jobslave.wait()

        log.info("Job %s exited", self.job.uuid)

    def _onSignal(self, signal, traceback):
        self.kill()
