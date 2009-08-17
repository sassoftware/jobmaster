#!/usr/bin/python2.4
#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()

import cPickle
import os
import subprocess
import stat
import sys
import tempfile

import jobmaster_helper

from jobmaster import templategen

from conary.lib import util


class FakeUJob(object):
    getPrimaryJobs = lambda x, *args, **kwargs: [['test', None, ['/test.rpath.local/1-1-1', 'is: x86']]]

class FakeClient(object):
    setUpdateCallback = lambda *args, **kwargs: None
    applyUpdate = lambda *args, **kwargs: None


class TemplateGenTest(jobmaster_helper.JobMasterHelper):
    def setUp(self):
        self.__class__.__base__.setUp(self)
        version = '/test.rpath.local@rpl:1'
        flavor = 'is: x86'
        self.anacondaCacheDir = tempfile.mkdtemp()
        self.anacondaTmpDir = tempfile.mkdtemp()
        _getUpdateJob = templategen.AnacondaTemplate._getUpdateJob
        try:
            templategen.AnacondaTemplate._getUpdateJob = \
                    lambda *args, **kwargs: FakeUJob()
            self.anaconda = templategen.AnacondaTemplate(version, flavor,
                    self.anacondaCacheDir, tmpDir = self.anacondaTmpDir)
        finally:
            templategen.AnacondaTemplate._getUpdateJob = _getUpdateJob
        self.anaconda._getConaryClient = lambda *args, **kwargs: FakeClient()
        self.anaconda._getUpdateJob = lambda *args, **kwargs: FakeUJob()

    def tearDown(self):
        self.__class__.__base__.tearDown(self)
        util.rmtree(self.anacondaCacheDir)
        util.rmtree(self.anacondaTmpDir)

    def testAnacondaCall(self):
        self.anaconda._call(['test'])
        self.failIf(self.callLog != [['test']])

    def testAnacondaStatus(self):
        tmpDir = tempfile.mkdtemp()
        try:
            res = self.anaconda.status()
            ref = (False, '')
            self.failIf(ref != res, "expected %s but got %s" % (str(ref), str(res)))
            statusPath = os.path.join(tmpDir, 'status')
            open(statusPath, 'w').write('test status')
            self.anaconda.statusPath = statusPath
            res = self.anaconda.status()
            ref = (True, 'test status')
            self.failIf(ref != res, "expected %s but got %s" % (str(ref), str(res)))
        finally:
            util.rmtree(tmpDir)

    def testAnacondaMetadata(self):
        tmpDir = tempfile.mkdtemp()
        log = templategen.log
        try:
            templategen.log = lambda *args, **kwargs: None
            res = self.anaconda.getMetadata()
            ref = {}
            self.failIf(ref != res, "expected %s but got %s" % (ref, res))

            # repeat with metadata
            metadataPath = os.path.join(tmpDir, 'metadata')
            self.touch(metadataPath, contents = cPickle.dumps('test'))
            self.anaconda.metadataPath = metadataPath
            res = self.anaconda.getMetadata()
            ref = 'test'
            self.failIf(ref != res, "expected %s but got %s" % (ref, res))
        finally:
            templategen.log = log
            util.rmtree(tmpDir)

    def testLock(self):
        res = self.anaconda._lock()
        self.failIf(not res, "couldn't obtain lock")
        res = self.anaconda._lock()
        self.failIf(res, "obtained lock twice")

    def testBadLockfile(self):
        self.anaconda.lockPath = os.path.join(self.anaconda.lockPath, 'missing')
        self.assertRaises(OSError, self.anaconda._lock)

    def testStaleLockfile(self):
        self.touch(self.anaconda.lockPath, contents = 'badpid')
        res = self.anaconda._lock()
        self.failIf(not res, "couldn't obtain lock")

    def testUnlock(self):
        self.touch(self.anaconda.lockPath, contents = 'badpid')
        res = self.anaconda._unlock()
        self.failIf(not res, "couldn't unlock")
        res = self.anaconda._unlock()
        self.failIf(res, "unlocked twice")

    def testExists(self):
        res = self.anaconda.exists()
        self.failIf(res, "bad return")
        self.touch(self.anaconda.templatePath)
        self.touch(self.anaconda.metadataPath)
        res = self.anaconda.exists()
        self.failIf(not res, "bad return")

    def testGenerate(self):
        self.anaconda._callback = 1
        manifestContents = ''
        util.mkdirChain(os.path.join(self.anaconda.tmpRoot, 'unified'))
        self.touch(os.path.join(self.anaconda.tmpRoot, 'MANIFEST'),
                contents = manifestContents)
        res = self.anaconda.generate()
        self.failIf(res, "bad return")

    def testGenerateLock(self):
        self.anaconda._lock()
        res = self.anaconda.generate()
        self.failIf(res != 3, "bad return")

    def testGenerateExists(self):
        self.anaconda.exists = lambda *args, **kwargs: True
        res = self.anaconda.generate()
        self.failIf(res != 2, "bad return")

    def testGenerateManifest(self):
        self.anaconda._callback = 1
        manifestContents = 'BAD_COMMAND'
        util.mkdirChain(os.path.join(self.anaconda.tmpRoot, 'unified'))
        self.touch(os.path.join(self.anaconda.tmpRoot, 'MANIFEST'),
                contents = manifestContents)
        res = self.anaconda.generate()
        self.failIf(res != 1, "bad return")

    def test_DO_image(self):
        templateWorkDir = tempfile.mkdtemp()
        try:
            self.touch(os.path.join(templateWorkDir, 'outputfile'))
            self.anaconda.templateWorkDir = templateWorkDir
            self.anaconda._DO_image('mkisofs', 'inputfile', 'outputfile')
            exists = os.path.exists(os.path.join(self.anaconda.tmpRoot,
                'outputfile'))
            self.failIf(not exists, "image command did not salvage output")
        finally:
            util.rmtree(templateWorkDir)

    def test_DO_imageModes(self):
        templateWorkDir = tempfile.mkdtemp()
        try:
            self.touch(os.path.join(templateWorkDir, 'outputfile'))
            self.anaconda.templateWorkDir = templateWorkDir
            self.anaconda._DO_image('mkisofs', 'inputfile', 'outputfile',
                    '0700')
            exists = os.path.exists(os.path.join(self.anaconda.tmpRoot,
                'outputfile'))
            self.failIf(not exists, "image command did not salvage output")
            status = os.stat(os.path.join(self.anaconda.tmpRoot, 'outputfile'))
            mode = status[stat.ST_MODE]
            self.failIf(oct(mode) != '0100700',
                    "output file status was not preserved")
        finally:
            util.rmtree(templateWorkDir)

    def testDoBogus(self):
        self.assertRaises(RuntimeError,
                self.anaconda._DO_image, 'not_a_real_command')

    def test_RUN_cpiogz(self):
        inputDir = tempfile.mkdtemp()
        fd, output = tempfile.mkstemp()
        os.close(fd)
        try:
            res = self.anaconda._RUN_cpiogz(inputDir, output)
            ref = ['find . | cpio --quiet -c -o | gzip -9 > %s' % output]
            self.failIf(ref != self.callLog, "expected '%s', but got '%s'" % \
                    (str(ref), str(self.callLog)))
        finally:
            util.rmtree(inputDir)
            os.unlink(output)

    def test_RUN_mkisofs(self):
        self.anaconda._RUN_mkisofs('inputDir', 'output')
        ref = [['mkisofs', '-quiet', '-o', 'output', '-b',
            'isolinux/isolinux.bin', '-c', 'isolinux/boot.cat',
            '-no-emul-boot', '-boot-load-size', '4', '-boot-info-table',
            '-R', '-J', '-T', '-V', 'rPath Linux', 'inputDir']]
        self.failIf(ref != self.callLog, "expected '%s', but got '%s'" % \
                (str(ref), str(self.callLog)))

    def test_RUN_mkcramfs(self):
        self.anaconda._RUN_mkcramfs('inputDir', 'output')
        ref = [['mkcramfs', 'inputDir', 'output']]
        self.failIf(ref != self.callLog, "expected '%s', but got '%s'" % \
                (str(ref), str(self.callLog)))

    def test_RUN_mkdosfs(self):
        inputDir = tempfile.mkdtemp()
        fd, output = tempfile.mkstemp()
        os.close(fd)
        try:
            self.touch(os.path.join(inputDir, 'testfile'))
            res = self.anaconda._RUN_mkdosfs(inputDir, output)
            self.failIf(len(self.callLog) != 4, "unpexected number of commands")
        finally:
            util.rmtree(inputDir)
            os.unlink(output)


class TemplateCallbackTest(testsuite.TestCase):
    def setUp(self):
        self.__class__.__base__.setUp(self)
        fd, self.logPath = tempfile.mkstemp()
        os.close(fd)
        fd, self.statusPath = tempfile.mkstemp()
        os.close(fd)
        self.callback = templategen.TemplateUpdateCallback(self.logPath,
                self.statusPath)

    def tearDown(self):
        self.__class__.__base__.tearDown(self)
        os.unlink(self.logPath)
        os.unlink(self.statusPath)

    def assertLogContent(self, msg):
        data = open(self.logPath).read()
        self.failIf(msg not in data,
                "expected '%s' to be present, but had:\n%s" % (msg, data))

    def assertStatusContent(self, msg):
        data = open(self.statusPath).read()
        self.failIf(msg not in data,
                "expected '%s' to be present, but had:\n%s" % (msg, data))

    def testRequestingChangeSet(self):
        self.callback.setChangeSet('test')
        self.callback.requestingChangeSet()
        msg = 'Requesting test from repository'
        self.assertLogContent(msg)
        self.assertStatusContent(msg)

    def testDownloadingChangeSet(self):
        self.callback.setChangeSet('test')
        self.callback.downloadingChangeSet(1024, 2048)
        msg = 'Downloading test from repository (50% of 2k)'
        self.assertLogContent(msg)
        self.assertStatusContent(msg)

    def testDownloadingFileContents(self):
        self.callback.setChangeSet('test')
        self.callback.downloadingFileContents(1024, 2048)
        msg = 'Downloading files for test from repository (50% of 2k)'
        self.assertLogContent(msg)
        self.assertStatusContent(msg)

    def testRestoreFiles(self):
        self.callback.setChangeSet('test')
        self.callback.restoreFiles(1024, 2048)
        msg = 'Writing test 1k of 2k (50%)'
        self.assertLogContent(msg)
        self.assertStatusContent(msg)

    def testPrefix(self):
        self.callback.setPrefix('prefix: ')
        self.callback.setChangeSet('test')
        self.callback.restoreFiles(1024, 2048)
        msg = 'prefix: Writing test'
        self.assertLogContent(msg)
        self.assertStatusContent(msg)


class ModuleLevelTest(testsuite.TestCase):
    def testTemplateGenerator(self):
        self.assertRaises(SystemExit, self.captureOutput, templategen.AnacondaTemplateGenerator, [])
        atg = templategen.AnacondaTemplateGenerator(['stuff'])
        self.failIf(atg._outdir != 'stuff', 'param rules to ATG have changed')

    def testCall(self):
        self.commands = []
        def fakeCall(cmds, **kwargs):
            self.commands.append((cmds, kwargs))

        call = subprocess.call
        fd, logPath = tempfile.mkstemp()
        os.close(fd)
        stderr = sys.stderr
        try:
            sys.stderr = open(logPath, 'w')
            subprocess.call = fakeCall
            templategen.call(['test'])
            self.failIf(self.commands != [(['test'], {'env': None})])
        finally:
            sys.stderr = stderr
            os.unlink(logPath)
            subprocess.call = call

    def testLog(self):
        msg = 'test'
        fd, logPath = tempfile.mkstemp()
        os.close(fd)
        fd, statusPath = tempfile.mkstemp()
        os.close(fd)
        try:
            templategen.log(msg, logPath = logPath,
                    statusPath = statusPath, truncateLog=True)
            self.failIf(msg not in open(logPath).read())
            self.failIf(msg not in open(statusPath).read())
        finally:
            os.unlink(logPath)


if __name__ == "__main__":
    testsuite.main()
