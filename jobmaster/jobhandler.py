#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import logging
import os
import signal
import simplejson
import time
import weakref

from jobmaster.resources.container import Container

log = logging.getLogger(__name__)


class JobHandler(object):
    def __init__(self, master, job):
        #self.master = weakref.ref(master)
        self.cfg = master.cfg
        self.job = job

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

    def check(self):
        """
        Return C{True} if the handler is running.
        """
        if not self.pid:
            return False
        if os.waitpid(self.pid, os.WNOHANG)[0]:
            log.info("Job %s finished", self.job.uuid)
            return False
        return True

    def kill(self):
        if not self.pid:
            return
        # Try SIGTERM first, but don't wait for longer than 1 second.
        os.kill(self.pid, signal.SIGTERM)
        start = time.time()
        while time.time() - start < 1.0 and self.check():
            time.sleep(0.1)
        else:
            # If it's still going, use SIGKILL and wait indefinitely.
            os.kill(self.pid, signal.SIGKILL)
            os.waitpid(self.pid, 0)
        self.pid = None

    def _run(self):
        log.info("Running job %s in pid %d", self.job.uuid, os.getpid())

        from conary import conarycfg
        from conary import conaryclient
        ccfg = conarycfg.ConaryConfiguration(True)
        ccli = conaryclient.ConaryClient(ccfg)
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
