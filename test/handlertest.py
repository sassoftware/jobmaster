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

class HandlerTest(jobmaster_helper.JobMasterHelper):
    def testTroveSpec(self):
        troveSpec = 'group-test=/test.rpath.local@rpl:1/1-1-1[is: x86]'
        handler = master.SlaveHandler(self.jobMaster, troveSpec, {})
        self.failIf(troveSpec != handler.troveSpec,
                    "Slave Handler should not alter troveSpec")

    def testStartHandler(self):
        try:
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
        except IndexError: # from getBootPaths, kernel/boot dir mismatch
            raise testsuite.SkipTestException("running kernel mismatch with /boot, skipping test")

    def testStopHandler(self):
        try:
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
            try:
                xenmac.genMac = lambda: '00:16:3e:00:01:22'
                xenip.genIP = lambda: '10.0.0.1'
                handler.start()
                handler.stop()
            finally:
                xenmac.genMac = genMac
                xenip.genIP = genIP
            self.failUnlessEqual(self.callLog, ['xm destroy slave22', 'sleep 2; lvremove -f /dev/vg00/slave22'])
        except IndexError: # from getBootPaths, kernel/boot dir mismatch
            raise testsuite.SkipTestException("running kernel mismatch with /boot, skipping test")

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

        makeImage = imagecache.ImageCache.makeImage
        genMac = xenmac.genMac
        genIP = xenip.genIP
        fork = os.fork
        setsid = os.setsid
        exit = os._exit
        try:
            imagecache. ImageCache.makeImage = dummyMakeImage
            os.fork = lambda: 0
            os.setsid = lambda: None
            xenmac.genMac = lambda: '00:16:3e:00:01:66'
            xenip.genIP = lambda: '10.0.0.1'
            os._exit = dummyExit
            try:
                handler.run()
            except SysExit, e:
                self.failIf(e.exitCode != 0,
                            "Run exited abnormally: exit code %d" % e.exitCode)
        finally:
            imagecache.ImageCache.makeImage = makeImage
            xenmac.genMac = genMac
            xenip.genIP = genIP
            os.fork = fork
            os.setsid = setsid
            os._exit = exit

        syscalls = ('lvcreate -n [^\s]* -L10240M vg00',
                    'mke2fs -m0 /dev/vg00/[^\s]',
                    'mount -o loop [^\s]* [^\s]*$', 'umount [^\s]*$',
                    'xm create /tmp/test-config$')
        for index, (rgx, cmd) in [x for x in enumerate(zip(syscalls, self.callLog))]:
            self.failIf(not re.match(rgx, cmd), "Unexpected command sent to system at position %d: %s" % (index, cmd))

        util.rmtree(handler.imagePath, ignore_errors = True)


if __name__ == "__main__":
    testsuite.main()
