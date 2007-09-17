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
import tempfile

from jobmaster import master, master_error
from jobmaster import util as masterUtil

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

    def testRewriteFile(self):
        fn, src = tempfile.mkstemp()
        os.close(fn)
        f = open(src, 'w')
        f.write('%(test)s')
        f.close()
        fn, dest = tempfile.mkstemp()
        os.close(fn)
        try:
            masterUtil.rewriteFile(src, dest, {'test': 'foo'})
            data = open(dest).read()
            self.failIf(data != 'foo')
        finally:
            if os.path.exists(src):
                os.unlink(src)
            os.unlink(dest)


if __name__ == '__main__':
    testsuite.main()
