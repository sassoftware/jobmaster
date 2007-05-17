#!/usr/bin/python2.4
#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()

import os
import tempfile

import jobmaster_helper
from jobmaster import xenmac
from jobmaster import xenip

class MasterTest(jobmaster_helper.JobMasterHelper):
    def testSuperUser(self):
        try:
            raise xenmac.SuperUser
        except xenmac.SuperUser, e:
            assert str(e) == "You must be superuser to use this function"

    def testNetworkInterface(self):
        try:
            raise xenmac.NetworkInterface
        except xenmac.NetworkInterface, e:
            assert str(e) == "IP address cannot be determined. There must be " \
                "a default route associated with an active interface."

    def testPipe(self):
        data = xenmac.readPipe('echo -n "foo"')
        self.failIf(data != 'foo', "readPipe did not function correctly")

    def testMaxSeqLow(self):
        MAX_SEQ = xenmac.MAX_SEQ
        try:
            xenmac.setMaxSeq(0)
            self.failIf(xenmac.MAX_SEQ < 1,
                        "max sequence underflow protection did not work")
        finally:
            xenmac.MAX_SEQ = MAX_SEQ

    def testMaxSeqHigh(self):
        MAX_SEQ = xenmac.MAX_SEQ
        try:
            xenmac.setMaxSeq(300)
            self.failIf(xenmac.MAX_SEQ > 256,
                        "max sequence overflow protection did not work")
        finally:
            xenmac.MAX_SEQ = MAX_SEQ

    def testCheckMac(self):
        readPipe = xenmac.readPipe
        try:
            xenmac.readPipe = lambda x: 'not a mac address'
            self.failIf(xenmac.checkMac(''), "checkMac matched erroneously")
        finally:
            xenmac.readPipe = readPipe

    def testGenMac(self):
        sequencePath = xenmac.sequencePath
        geteuid = os.geteuid
        readPipe = xenmac.readPipe
        MAX_SEQ = xenmac.MAX_SEQ
        try:
            fd, xenmac.sequencePath = tempfile.mkstemp()
            os.close(fd)
            os.geteuid = lambda: 0
            xenmac.readPipe = lambda x: '192.168.1.1'
            xenmac.setMaxSeq(5)
            for i in range(2):
                for i in range(5):
                    mac = xenmac.genMac()
                    self.failIf(mac != '00:16:3e:01:01:0%d' % i,
                                "expected mac: 00:16:3e:01:01:0%d, but got: %s"\
                                    % (i, mac))
            xenmac.setMaxSeq(1)
            for i in range(2):
                mac = xenmac.genMac()
                self.failIf(mac != '00:16:3e:01:01:00',
                            "expected mac: 00:16:3e:01:01:00, but got: %s" % \
                                mac)

        finally:
            xenmac.readPipe = readPipe
            os.geteuid = geteuid
            xenmac.sequencePath = sequencePath
            xenmac.MAX_SEQ = MAX_SEQ

    def testGenMacUser(self):
        geteuid = os.geteuid
        try:
            os.geteuid = lambda: 500
            self.assertRaises(xenmac.SuperUser, xenmac.genMac)
        finally:
            os.geteuid = geteuid

    def testGenMacIP(self):
        geteuid = os.geteuid
        readPipe = xenmac.readPipe
        try:
            xenmac.readPipe = lambda x: ''
            os.geteuid = lambda: 0
            self.assertRaises(xenmac.NetworkInterface, xenmac.genMac)
        finally:
            os.geteuid = geteuid
            xenmac.readPipe = readPipe

    def testGenIP(self):
        sequencePath = xenip.sequencePath
        geteuid = os.geteuid
        checkIP = xenip.checkIP
        MAX_SEQ = xenip.MAX_SEQ
        try:
            fd, xenip.sequencePath = tempfile.mkstemp()
            os.close(fd)
            os.geteuid = lambda: 0
            xenip.checkIP = lambda x: True
            xenip.setMaxSeq(6)
            for i in range(5):
                mac = xenip.genIP()
                self.failUnlessEqual(mac, '10.5.6.%d' % (i+1))

            xenip.setMaxSeq(0)
            xenip.checkIP = lambda x: False
            self.assertRaises(xenip.NoIPAddressAvailable, xenip.genIP)

        finally:
            xenip.checkIP = checkIP
            os.geteuid = geteuid
            xenip.sequencePath = sequencePath
            xenip.MAX_SEQ = MAX_SEQ


if __name__ == "__main__":
    testsuite.main()
