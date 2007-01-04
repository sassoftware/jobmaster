#!/usr/bin/python2.4
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

archivePath = None
testPath = None

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

conaryDir = None
_setupPath = None
def setup():
    global _setupPath
    if _setupPath:
        return _setupPath
    global testPath
    global archivePath

    # set default CONARY_POLICY_PATH is it was not set.
    conaryPolicy = os.getenv('CONARY_POLICY_PATH', '/usr/lib/conary/policy')
    os.environ['CONARY_POLICY_PATH'] = conaryPolicy

    # set default paths, if it was not set.
    if not os.environ.has_key('MCP_PATH'):
        print "please set MCP_PATH"
        sys.exit(1)
    mcpPath = os.getenv('MCP_PATH')
    if mcpPath not in sys.path:
        sys.path.insert(0, mcpPath)

    parDir = '/'.join(os.path.realpath(__file__).split('/')[:-2])
    jobMasterPath = os.getenv('JOB_MASTER_PATH', parDir)
    os.environ['JOB_MASTER_PATH'] = jobMasterPath
    if jobMasterPath not in sys.path:
        sys.path.insert(0, jobMasterPath)
    # end setting default paths

    if not os.environ.has_key('CONARY_PATH'):
	print "please set CONARY_PATH"
	sys.exit(1)
    paths = (os.environ['JOB_MASTER_PATH'],
             os.environ['JOB_MASTER_PATH'] + '/test',
             os.environ['CONARY_PATH'],
             os.path.normpath(os.environ['CONARY_PATH'] + "/../rmake"),
             os.path.normpath(os.environ['CONARY_PATH'] + "/../conary-test"),)
    pythonPath = os.getenv('PYTHONPATH') or ""
    for p in reversed(paths):
        if p in sys.path:
            sys.path.remove(p)
        sys.path.insert(0, p)
    for p in paths:
        if p not in pythonPath:
            pythonPath = os.pathsep.join((pythonPath, p))
    os.environ['PYTHONPATH'] = pythonPath

    if isIndividual():
        serverDir = '/tmp/conary-server'
        if os.path.exists(serverDir) and not os.path.access(serverDir, os.W_OK):
            serverDir = serverDir + '-' + pwd.getpwuid(os.getuid())[0]
        os.environ['SERVER_FILE_PATH'] = serverDir
    import testhelp
    testPath = testhelp.getTestPath()
    archivePath = testPath + '/' + "archive"
    parent = os.path.dirname(testPath)

    global conaryDir
    conaryDir = os.environ['CONARY_PATH']

    from conary.lib import util
    sys.excepthook = util.genExcepthook(True)

    # if we're running with COVERAGE_DIR, we'll start covering now
    from conary.lib import coveragehook

    # import tools normally expected in findTrove.
    from testhelp import context, TestCase, findPorts, SkipTestException
    sys.modules[__name__].context = context
    sys.modules[__name__].TestCase = TestCase
    sys.modules[__name__].findPorts = findPorts
    sys.modules[__name__].SkipTestException = SkipTestException

    # MCP specific tweaks
    import jobmaster_helper
    import stomp
    stomp.Connection = jobmaster_helper.DummyConnection
    #end MCP specific tweaks

    _setupPath = testPath
    return testPath

_individual = False

def isIndividual():
    global _individual
    return _individual


EXCLUDED_PATHS = ['test', 'scripts', 'raaplugins', 'schema.py']

def main(argv=None, individual=True):
    import testhelp
    testhelp.isIndividual = isIndividual
    class rBuilderTestSuiteHandler(testhelp.TestSuiteHandler):
        suiteClass = testhelp.ConaryTestSuite

        def getCoverageDirs(self, environ):
            return environ['jobmaster']

        def getCoverageExclusions(self, environ):
            return EXCLUDED_PATHS

    global _handler
    global _individual
    _individual = individual
    if argv is None:
        argv = list(sys.argv)
    topdir = testhelp.getTestPath()
    cwd = os.getcwd()
    if topdir not in sys.path:
        sys.path.insert(0, topdir)
    if cwd != topdir and cwd not in sys.path:
        sys.path.insert(0, cwd)

    handler = rBuilderTestSuiteHandler(individual=individual, topdir=topdir,
                                       testPath=testPath, conaryDir=conaryDir)
    _handler = handler
    results = handler.main(argv)
    return (not results.wasSuccessful())

if __name__ == '__main__':
    setup()
    sys.exit(main(sys.argv, individual=False))
