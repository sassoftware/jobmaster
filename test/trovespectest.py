#!/usr/bin/python2.4
#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()

import logging
import os
import tempfile

import jobmaster_helper

from jobmaster import master

from conary import conaryclient
from conary.conaryclient import cmdline
from conary.lib import util
from conary.repository import trovesource

class FakeClient(object):
    repos = None
    __init__ = lambda *args, **kwargs: None
    getRepos = lambda x, *args, **kwargs: x.repos

class MasterTest(testsuite.TestCase):
    def setUp(self):
        self.basePath = tempfile.mkdtemp()
        os.mkdir(os.path.join(self.basePath, 'imageCache'))
        os.mkdir(os.path.join(self.basePath, 'logs'))
        os.mkdir(os.path.join(self.basePath, 'config.d'))
        os.mkdir(os.path.join(self.basePath, 'tmp'))

        master.CONFIG_PATH = os.path.join(self.basePath, 'config.d', 'runtime')

        self.cfg = master.MasterConfig()
        self.cfg.nodeName = 'testMaster'
        self.cfg.nameSpace = 'test'
        self.cfg.basePath = self.basePath
        self.cfg.logFile = os.path.join(self.basePath, 'logs', 'jobmaster.log')

        master.getRunningKernel = jobmaster_helper.FakeGetRunningKernel

        self.ConaryClient = conaryclient.ConaryClient
        conaryclient.ConaryClient = FakeClient
        FakeClient.repos = trovesource.SimpleTroveSource()
        self.jobMaster = master.JobMaster(self.cfg)
        self.jobMaster.response.response.connection.sent = []
        self.__class__.__base__.setUp(self)

    def tearDown(self):
        self.__class__.__base__.tearDown(self)
        conaryclient.ConaryClient = self.ConaryClient
        util.rmtree(self.basePath)
        for x in logging._handlers:
            logging.getLogger().removeHandler(x)

    def addTrove(self, troveSpec):
        from conary import versions
        nvf = cmdline.parseTroveSpec(troveSpec)
        ver = versions.VersionFromString(nvf[1])
        FakeClient.repos.addTrove(nvf[0], ver, nvf[2])

    def testUnmatchedResolve(self):
        # neither of the candidate flavors satisfy requirements for xen, so
        # no suitable jobslave will be found

        # squelch the warning
        logging.getLogger().setLevel(logging.ERROR)

        troveSpec1 = 'jobslave=/test.rpath.local@rpl:1/4.0.0-21-1[is: x86]'
        troveSpec2 = 'jobslave=/test.rpath.local@rpl:1/4.0.0-21-1[is: x86_64]'
        self.addTrove(troveSpec1)
        self.addTrove(troveSpec2)
        res = self.jobMaster.resolveTroveSpec(troveSpec1)
        self.failIf(res != troveSpec1, "expected '%s' but got '%s'" % \
                (troveSpec1, res))

    def testXenX86Resolve(self):
        search = 'group-jobslave=test.rpath.local@rpath:js-4-test/4.0.0-22-20[is: x86]'
        troveSpec1 = 'group-jobslave=/test.rpath.local@rpath:js-4-test/4.0.0-22-20[~!dom0,domU,~!vmware,xen is: x86(i486,i586,i686,mmx,~!sse2)]'
        troveSpec2 = 'group-jobslave=/test.rpath.local@rpath:js-4-test/4.0.0-22-20[~!dom0,domU,~!vmware,xen is: x86(i486,i586,i686) x86_64]'
        self.addTrove(troveSpec1)
        self.addTrove(troveSpec2)
        res = self.jobMaster.resolveTroveSpec(search)
        self.failIf(res != troveSpec1, "expected '%s' but got '%s'" % \
                (troveSpec1, res))

    def testXenX86_64Resolve(self):
        search = 'group-jobslave=test.rpath.local@rpath:js-4-test/4.0.0-22-20[is: x86_64]'
        troveSpec1 = 'group-jobslave=/test.rpath.local@rpath:js-4-test/4.0.0-22-20[~!dom0,domU,~!vmware,xen is: x86(i486,i586,i686,mmx,~!sse2)]'
        troveSpec2 = 'group-jobslave=/test.rpath.local@rpath:js-4-test/4.0.0-22-20[~!dom0,domU,~!vmware,xen is: x86(i486,i586,i686) x86_64]'
        self.addTrove(troveSpec1)
        self.addTrove(troveSpec2)
        res = self.jobMaster.resolveTroveSpec(search)
        self.failIf(res != troveSpec2, "expected '%s' but got '%s'" % \
                (troveSpec2, res))


if __name__ == "__main__":
    testsuite.main()
