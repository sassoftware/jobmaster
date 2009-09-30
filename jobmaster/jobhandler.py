#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import logging
import os
import random
import simplejson

from jobmaster.resources.container import Container
from jobmaster.subprocutil import Subprocess

log = logging.getLogger(__name__)


class JobHandler(Subprocess):
    def __init__(self, master, job):
        self.cfg = master.cfg
        self.job = job

        self.conaryCfg = master.conaryCfg
        self.loopManager = master.loopManager

        self.pid = None

    def start(self):
        self.pid = os.fork()
        if not self.pid:
            try:
                try:
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
        troveTup = repos.findTrove(None, ('group-jobslave', 'bananas.rb.rpath.com@rpl:trash', None))[0]
        jobslave = Container([troveTup], self.cfg, ccfg, self.loopManager)
        jobslave.start()
        jobslave.createFile('tmp/jobslave/data',
                simplejson.dumps(self.job.job_data))
        try:
            jobslave.run(['/usr/bin/jobslave'])
        finally:
            jobslave.close()
