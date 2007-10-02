#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()

import jobmaster_helper

import glob
import os, sys
import re
import time
import tempfile

from jobmaster import master
from jobmaster import xenmac, xenip
from jobmaster import imagecache

from conary.lib import util
from conary import conaryclient

class HandlerTest(jobmaster_helper.JobMasterHelper):
    def testTroveSpec(self):
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec, {})
        self.failIf(troveSpec != handler.troveSpec,
                    "Slave Handler should not alter troveSpec")

    def testStartHandler(self):
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec,
                {'UUID': 'test.rpath.local-build-64'})
        handler.run = lambda: None
        genMac = xenmac.genMac
        genIP = xenip.genIP
        try:
            xenmac.genMac = lambda: '00:16:3e:00:01:34'
            xenip.genIP = lambda: '10.0.0.1'
            slaveName = handler.start()
        finally:
            xenmac.genMac = genMac
            xenip.genIP = genIP
        self.failIf(slaveName != 'slave34',
                    "Expected slaveName of slave34, got %s" % slaveName)

    def testStopHandler(self):
        if not glob.glob("/boot/vmlinuz*"):
            raise testsuite.SkipTestException("No kernel on this machine, skipping test")
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec,
                {'UUID' : 'test.rpath.local-build-65'})

        def dummyRun():
            handler.pid = os.fork()
            if not handler.pid:
                os.setsid()
                time.sleep(10)

        handler.run = dummyRun
        genMac = xenmac.genMac
        genIP = xenip.genIP
        waitForSlave = master.waitForSlave
        try:
            master.waitForSlave = lambda *args, **kwargs: None
            xenmac.genMac = lambda: '00:16:3e:00:01:22'
            xenip.genIP = lambda: '10.0.0.1'
            handler.start()
            handler.stop()
        finally:
            master.waitForSlave = waitForSlave
            xenmac.genMac = genMac
            xenip.genIP = genIP
        self.failUnlessEqual(self.callLog, ['xm destroy slave22', 'lvremove -f /dev/vg00/slave22-scratch', 'lvremove -f /dev/vg00/slave22-base'])

    def testRunHandler(self):
        class SysExit(Exception):
            def __init__(self, exitCode):
                self.exitCode = exitCode
            def __str__(self):
                return "os._exit(%d)" % exitCode

        def dummyExit(exitCode):
            raise SysExit(exitCode)

        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec,
                {'UUID' : 'test.rpath.local-build-55'})

        def dummyMakeImage(self, troveSpec, hash):
            filePath = os.path.join(handler.imageCache().cachePath, hash)
            f = open(filePath, 'w')
            f.write('')
            f.close()
            return filePath

        fd, handler.imagePath = tempfile.mkstemp()
        os.close(fd)
        handler.slaveName = 'xen44'
        handler.cfgPath = '/tmp/test-config'
        handler.ip = '10.0.0.1'
        waitForSlave = master.waitForSlave

        makeImage = imagecache.ImageCache.makeImage
        genMac = xenmac.genMac
        genIP = xenip.genIP
        fork = os.fork
        setsid = os.setsid
        exit = os._exit
        try:
            master.waitForSlave = lambda *args, **kwargs: None
            imagecache. ImageCache.makeImage = dummyMakeImage
            os.fork = lambda: 0
            os.setsid = lambda: None
            xenmac.genMac = lambda: '00:16:3e:00:01:66'
            xenip.genIP = lambda: '10.0.0.1'
            os._exit = dummyExit
            handler.estimateScratchSize = lambda *args, **kwargs: 10240
            try:
                handler.run()
            except SysExit, e:
                self.failIf(e.exitCode != 0,
                            "Run exited abnormally: exit code %d" % e.exitCode)
        finally:
            master.waitForSlave = waitForSlave
            imagecache.ImageCache.makeImage = makeImage
            xenmac.genMac = genMac
            xenip.genIP = genIP
            os.fork = fork
            os.setsid = setsid
            os._exit = exit

        syscalls = ('lvcreate -n [^\s]*-base -L0M vg00',
                    'dd if=[^\s]* of=[^\s]*',
                    'lvcreate -n [^\s]*-scratch -L10240M vg00',
                    'mke2fs -m0 /dev/vg00/[^\s]',
                    'mount [^\s]* [^\s]*$', 'umount [^\s]*$',
                    'xm create /tmp/test-config$')
        for index, (rgx, cmd) in [x for x in enumerate(zip(syscalls, self.callLog))]:
            self.failIf(not re.match(rgx, cmd), "Unexpected command sent to system at position %d: %s" % (index, cmd))

        util.rmtree(handler.imagePath, ignore_errors = True)

    def testWriteSlaveConfig(self):
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec,
                {'UUID' : 'test.rpath.local-build-55'})

        fd, cfgPath = tempfile.mkstemp()
        os.close(fd)
        handler.slaveName = 'xen44'

        cfg = master.MasterConfig()
        cfg.queueHost = '127.0.0.1'
        cfg.nodeName = 'testMaster'
        cfg.jobQueueName = 'job1-1-1:x86'
        cfg.conaryProxy = 'self'

        getIP = master.getIP
        ref1 = '\n'.join(( \
            'queueHost 192.168.0.1', 'queuePort 61613',
            'nodeName testMaster:xen44', 'jobQueueName job1-1-1:x86',
            'conaryProxy http://192.168.0.1/', 'debugMode False', ''))
        ref2 = '\n'.join(( \
            'queueHost 192.168.0.1', 'queuePort 61613',
            'nodeName testMaster:xen44', 'jobQueueName job1-1-1:x86',
            'debugMode True', ''))
        try:
            master.getIP = lambda: '192.168.0.1'
            handler.writeSlaveConfig(cfgPath, cfg)
            res = open(cfgPath).read()
            self.failIf(ref1 != res,
                    "EXPECTED:\n%s\nBUT GOT:\n%s" % (ref1, res))

            # Make a second pass, tweaking some options
            cfg.conaryProxy = None
            cfg.debugMode = True
            handler.writeSlaveConfig(cfgPath, cfg)
            res = open(cfgPath).read()
            self.failIf(ref2 != res,
                    "EXPECTED:\n%s\nBUT GOT:\n%s" % (ref2, res))
        finally:
            util.rmtree(cfgPath, ignore_errors = True)
            master.getIP = getIP

    def testEstimateTroveSize(self):
        troveName = 'group-test'
        troveVersion = '/test.rpath.local@rpl:1/0.0:1-1-1'
        troveFlavor = '1#x86'
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec,
                {'UUID' : 'test.rpath.local-build-55', 'troveName': troveName,
                    'troveVersion': troveVersion, 'troveFlavor': troveFlavor,
                    'type': 'build', 'protocolVersion': 1, 'project': {'conaryCfg': 'name RonaldFrobnitz'}})

        handler.slaveName = 'xen44'

        ConaryClient = conaryclient.ConaryClient
        class MockClient(object):
            def __init__(x, y = None):
                x.getRepos = lambda *args, **kwargs: x
                x.findTrove = lambda *args, **kwargs: [[x]]
                x.getTrove = lambda *args, **kwargs: x
                x.troveInfo = x
                x.cfg = y
                x.flavor = None
                x.size = lambda *args, **kwargs: 55 * 1024 * 1024 # 55M trove

                self.failUnlessEqual(y.name, "RonaldFrobnitz")
        try:
            conaryclient.ConaryClient = MockClient
            res = handler.estimateScratchSize()
            self.failIf(res != 344,
                    "scratch calculation did not match expected value")
        finally:
            conaryclient.ConaryClient = ConaryClient

    def testEstimateCookSize(self):
        troveName = 'group-test'
        troveVersion = '/test.rpath.local@rpl:1/0.0:1-1-1'
        troveFlavor = '1#x86'
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec,
                {'UUID' : 'test.rpath.local-cook-55-1',
                    'type': 'cook', 'protocolVersion': 1})

        handler.slaveName = 'xen44'

        res = handler.estimateScratchSize()
        self.failIf(res != 4800,
                "scratch calculation did not match expected value")


if __name__ == "__main__":
    testsuite.main()
