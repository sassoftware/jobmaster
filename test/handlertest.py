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
from jobmaster import util as jmutil

from conary.lib import util
from conary import conaryclient

class HandlerTest(jobmaster_helper.JobMasterHelper):
    def testTroveSpec(self):
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec,
                jobmaster_helper.kernelData, {'UUID': 'uuid'})
        self.failIf(troveSpec != handler.troveSpec,
                    "Slave Handler should not alter troveSpec")

    def testStartHandler(self):
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec,
                jobmaster_helper.kernelData,
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
                jobmaster_helper.kernelData,
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
        exists = os.path.exists
        try:
            master.waitForSlave = lambda *args, **kwargs: None
            xenmac.genMac = lambda: '00:16:3e:00:01:22'
            xenip.genIP = lambda: '10.0.0.1'
            os.path.exists = lambda path: True
            handler.start()
            handler.stop()
        finally:
            master.waitForSlave = waitForSlave
            xenmac.genMac = genMac
            xenip.genIP = genIP
            os.path.exists = exists
        self.failUnlessEqual(self.callLog, ['xm destroy slave22',
            'lvremove -f /dev/vg00/slave22-base >/dev/null',
            'lvremove -f /dev/vg00/slave22-scratch >/dev/null',
            'lvremove -f /dev/vg00/slave22-swap >/dev/null',
            ])

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
                jobmaster_helper.kernelData,
                {'UUID' : 'test.rpath.local-build-55'})

        def dummyMakeImage(self, troveSpec, kernelData, hash):
            filePath = os.path.join(handler.imageCache().cachePath, hash)
            f = open(filePath, 'w')
            f.write('')
            f.close()
            return filePath

        temp = tempfile.mkdtemp()
        handler.imageBase = temp + '/image'
        handler.slaveName = 'xen44'
        handler.cfgPath = temp + '/test-config'
        handler.ip = '10.0.0.1'
        waitForSlave = master.waitForSlave

        makeImage = imagecache.ImageCache.makeImage
        genMac = xenmac.genMac
        genIP = xenip.genIP
        fork = os.fork
        setsid = os.setsid
        exit = os._exit
        alloc = jmutil.allocateScratch
        try:
            master.waitForSlave = lambda *args, **kwargs: None
            imagecache. ImageCache.makeImage = dummyMakeImage
            os.fork = lambda: 0
            os.setsid = lambda: None
            xenmac.genMac = lambda: '00:16:3e:00:01:66'
            xenip.genIP = lambda: '10.0.0.1'
            os._exit = dummyExit
            jmutil.allocateScratch = lambda *args, **kwargs: None
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
            jmutil.allocateScratch = alloc

            util.rmtree(temp, ignore_errors=True)

        syscalls = (
                'dd if=\S+/imageCache/\S+ of=\S+-base',
                'mkfs -t ext2 -F -q -m0 \S+-scratch',
                'mkswap -f \S+-swap',
                'mount \S+-base \S+$',
                'umount \S+$',
                'xm create /tmp/\S+/test-config$',
                )
        for index, (rgx, cmd) in [x for x in enumerate(zip(syscalls, self.callLog))]:
            self.failIf(not re.match(rgx, cmd),
                    "Unexpected command sent to system at position %d: %s"
                    % (index, cmd))

    def testWriteSlaveConfig(self):
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec,
                jobmaster_helper.kernelData,
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
            cfg.conaryProxy = 'http://192.168.0.1/'
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
                jobmaster_helper.kernelData,
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
                # 255M trove to avoid the minimum scratch size
                x.size = lambda *args, **kwargs: 255 * 1024 * 1024

                self.failUnlessEqual(y.name, "RonaldFrobnitz")
        try:
            conaryclient.ConaryClient = MockClient
            res = handler.estimateScratchSize()
            self.failUnlessEqual(res, 1265)
        finally:
            conaryclient.ConaryClient = ConaryClient

    def testEstimateCookSize(self):
        troveName = 'group-test'
        troveVersion = '/test.rpath.local@rpl:1/0.0:1-1-1'
        troveFlavor = '1#x86'
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec,
                jobmaster_helper.kernelData,
                {'UUID' : 'test.rpath.local-cook-55-1',
                    'type': 'cook', 'protocolVersion': 1})

        handler.slaveName = 'xen44'

        res = handler.estimateScratchSize()
        self.failIf(res != 1024,
                "scratch calculation did not match expected value")

    def testSwapSizeCalc(self):
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec,
                jobmaster_helper.kernelData,
                {'UUID' : 'test.rpath.local-build-55'})
        #2x memory size until 2GB, then memsize + 2GB
        self.assertEquals(512 * 1048576, handler.calcSwapSize(256))
        self.assertEquals(2048 * 1048576, handler.calcSwapSize(1024))
        self.assertEquals(4096 * 1048576, handler.calcSwapSize(2048))
        self.assertEquals(6144 * 1048576, handler.calcSwapSize(4096))


if __name__ == "__main__":
    testsuite.main()
