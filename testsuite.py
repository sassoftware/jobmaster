#!/usr/bin/python
# -*- mode: python -*-
#
# Copyright (c) 2004-2009 rPath, Inc.
#

import os
import sys
import unittest
from testrunner import pathManager
from testrunner import testhelp


def setup():
    pathManager.addExecPath('MCP_PATH')
    pathManager.addExecPath('CONARY_PATH')
    pathManager.addExecPath('JOB_MASTER_PATH', isTestRoot=True)

    from conary.lib import util
    sys.excepthook = util.genExcepthook(True)

    # if we're running with COVERAGE_DIR, we'll start covering now
    from conary.lib import coveragehook

    # import tools normally expected in findTrove.
    from testrunner.testhelp import context, TestCase, findPorts, SkipTestException
    sys.modules[__name__].context = context
    sys.modules[__name__].TestCase = TestCase
    sys.modules[__name__].findPorts = findPorts
    sys.modules[__name__].SkipTestException = SkipTestException

    # MCP specific tweaks
    import stomp
    from jobmaster_test import jobmaster_helper
    stomp.Connection = jobmaster_helper.DummyConnection
    #end MCP specific tweaks


_individual = False


EXCLUDED_PATHS = ['test', 'scripts', 'raaplugins', 'schema.py', 'dist', '/build/', 'setup.py']

def main(argv=None):
    class rBuilderTestSuiteHandler(testhelp.TestSuiteHandler):
        suiteClass = testhelp.ConaryTestSuite

        def getCoverageDirs(self, environ):
            return os.getenv('JOB_MASTER_PATH')

        def getCoverageExclusions(self, environ):
            return EXCLUDED_PATHS

    handler = rBuilderTestSuiteHandler(individual=False)

    if argv is None:
        argv = list(sys.argv)
    results = handler.main(argv)
    return results.getExitCode()


if __name__ == '__main__':
    setup()
    sys.exit(main(sys.argv))
