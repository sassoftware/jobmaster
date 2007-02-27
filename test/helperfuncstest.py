#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()

import testhelp

import os

from jobmaster import master, master_error

class HelperFuncsTest(testhelp.TestCase):
    def testArch(self):
        self.failIf(master.getAvailableArchs('i686') != ('x86',),
                    "Incorrect arch returned for x86")
        self.failIf(master.getAvailableArchs('x86_64') != ('x86', 'x86_64'),
                    "Incorrect arch returned for x86_64")

    def testGetIp(self):
        IP = master.getIP()
        assert type(IP) is str
        assert len(IP) != 0

    def testSingleProtocol(self):
        @master.protocols(1)
        def stubFunction(self):
            return 'stub'

        res = stubFunction(self, protocolVersion = 1)

        assert res == 'stub'

    def testBadProtocol(self):
        @master.protocols((1,))
        def stubFunction(self):
            return 'stub'

        self.assertRaises(master_error.ProtocolError, stubFunction,
                          self, protocolVersion = -1)


if __name__ == '__main__':
    testsuite.main()
