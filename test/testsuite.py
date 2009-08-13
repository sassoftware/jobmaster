#!/usr/bin/python
# -*- mode: python -*-
#
# Copyright (c) 2004-2006 rPath, Inc.
#

import bdb
import cPickle
import grp
import sys
import os
import os.path
import pwd
import socket
import re
import tempfile
import time
import types
import unittest
import __builtin__

#from pychecker import checker

def enforceBuiltin(result):
    failure = False
    if isinstance(result, (list, tuple)):
        for item in result:
            failure = failure or enforceBuiltin(item)
    elif isinstance(result, dict):
        for item in result.values():
            failure = failure or enforceBuiltin(item)
    failure =  failure or (result.__class__.__name__ \
                           not in __builtin__.__dict__)
    return failure

def filteredCall(self, *args, **kwargs):
    isException, result = self._server.callWrapper(self._name,
                                                   self._authToken, args)

    if not isException:
        if enforceBuiltin(result):
            # if the return type appears to be correct, check the types
            # some items get cast to look like built-ins for str()
            # an extremely common example is sql result rows.
            raise AssertionError('XML cannot marshall return value: %s '
                                 'for method %s' % (str(result), self._name))
        return result
    else:
        self.handleError(result)

_setupPath = None
def setup():
    global _setupPath
    if _setupPath:
        return _setupPath

    from testrunner import pathManager
    mcpPath = pathManager.addExecPath('MCP_PATH')
    conaryPath = pathManager.addExecPath('CONARY_PATH')
    conaryTestPath = pathManager.addExecPath('CONARY_TEST_PATH')
    jobmasterPath = pathManager.addExecPath('JOB_MASTER_PATH')
    jmTestPath = pathManager.addExecPath('JOB_MASTER_TEST_PATH')
    pathManager.addResourcePath('TEST_PATH',path=jmTestPath)
    stompPath = pathManager.addExecPath('STOMP_PATH')

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
    import jobmaster_helper
    import stomp
    stomp.Connection = jobmaster_helper.DummyConnection
    #end MCP specific tweaks

    _setupPath = jmTestPath
    return jmTestPath

_individual = False
def isIndividual():
    global _individual
    return _individual


EXCLUDED_PATHS = ['test', 'scripts', 'raaplugins', 'schema.py', 'dist', '/build/', 'setup.py']

def main(argv=None, individual=True):
    from testrunner import testhelp
    testhelp.isIndividual = isIndividual
    class rBuilderTestSuiteHandler(testhelp.TestSuiteHandler):
        suiteClass = testhelp.ConaryTestSuite

        def getCoverageDirs(self, environ):
            return os.getenv('JOB_MASTER_PATH')

        def getCoverageExclusions(self, environ):
            return EXCLUDED_PATHS

    global _handler
    global _individual
    _individual = individual

    from testrunner import pathManager
    handler = rBuilderTestSuiteHandler(individual=individual, topdir=pathManager.getPath('JOB_MASTER_TEST_PATH'), 
                                       testPath=pathManager.getPath('JOB_MASTER_TEST_PATH'), 
                                       conaryDir=pathManager.getPath('CONARY_PATH')
                                       )
    _handler = handler

    if argv is None:
        argv = list(sys.argv)
    results = handler.main(argv)
    return (not results.wasSuccessful())

if __name__ == '__main__':
    setup()
    sys.exit(main(sys.argv, individual=False))
