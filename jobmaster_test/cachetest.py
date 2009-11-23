#!/usr/bin/python2.4
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All rights reserved
#

import testsuite
testsuite.setup()

import jobmaster_helper

import os
import signal
import StringIO
import tempfile
import time

from conary import conarycfg, conaryclient
from conary.lib import util
from jobmaster import imagecache

class CacheTest(jobmaster_helper.JobMasterHelper):
    def testGetExistingImage(self):
        troveSpec = 'existingImage'
        path = self.jobMaster.imageCache.imagePath(troveSpec,
            jobmaster_helper.kernelData)

        f = open(path, 'w')
        f.write('')
        f.close()

        def stubMakeImage(troveSpec, kernelData, hash):
            self.fail('makeImage should not have been called')

        origMakeImage = self.jobMaster.imageCache.makeImage
        try:
            self.jobMaster.imageCache.makeImage = stubMakeImage
            self.jobMaster.imageCache.getImage(troveSpec,
                jobmaster_helper.kernelData)
        finally:
            self.jobMaster.imageCache.makeImage = origMakeImage

    def testNewKernel(self):
        '''
        Ensure that a new slave is built if the jobmaster's kernel version
        has changed.

        @tests: RBL-2491
        '''

        troveSpec = 'existingImage'
        path = self.jobMaster.imageCache.imagePath(troveSpec,
            jobmaster_helper.kernelData)

        f = open(path, 'w')
        f.write('')
        f.close()

        kernelData = dict(jobmaster_helper.kernelData)
        kernelData['trove'] = ('another:trove', None, None)

        def stubMakeImage(troveSpec, kernelData, hash, hook):
            return 'success!'

        origMakeImage = self.jobMaster.imageCache.makeImage
        try:
            self.jobMaster.imageCache.makeImage = stubMakeImage
            assert self.jobMaster.imageCache.getImage(troveSpec,
                kernelData) == 'success!', \
                'New slave was not built when kernel changed'
        finally:
            self.jobMaster.imageCache.makeImage = origMakeImage


    def testMissingImage(self):
        troveSpec = 'nonExistentImage'

        def stubMakeImage(troveSpec, kernelData, hash, hook):
            return 'makeImage was called successfully'

        origMakeImage = self.jobMaster.imageCache.makeImage
        try:
            self.jobMaster.imageCache.makeImage = stubMakeImage
            assert self.jobMaster.imageCache.getImage(troveSpec,
                jobmaster_helper.kernelData) == \
                'makeImage was called successfully'
        finally:
            self.jobMaster.imageCache.makeImage = origMakeImage

    def testMissingImageCollide(self):
        troveSpec = 'nonExistentImage'

        lockDir = self.jobMaster.imageCache.imagePath(troveSpec,
            jobmaster_helper.kernelData) + '.lock'
        util.mkdirChain(lockDir)

        def stubMakeImage(troveSpec, kernelData, hash, hook):
            return 'makeImage was called successfully'

        def dummySleep(*args, **kwargs):
            util.rmtree(lockDir)

        sleep = time.sleep
        origMakeImage = self.jobMaster.imageCache.makeImage
        try:
            time.sleep = dummySleep
            self.jobMaster.imageCache.makeImage = stubMakeImage
            assert self.jobMaster.imageCache.getImage(troveSpec,
                jobmaster_helper.kernelData) == \
                'makeImage was called successfully'
        finally:
            time.sleep = sleep
            self.jobMaster.imageCache.makeImage = origMakeImage

    def testHaveImage(self):
        troveSpec = 'notReal'
        path = self.jobMaster.imageCache.imagePath(troveSpec,
            jobmaster_helper.kernelData)
        assert not self.jobMaster.imageCache.haveImage(troveSpec,
            jobmaster_helper.kernelData)

        f = open(path, 'w')
        f.write('')
        f.close()

        assert self.jobMaster.imageCache.haveImage(troveSpec,
            jobmaster_helper.kernelData)

    def testDeleteImages(self):
        troveSpec = 'fakeImafe'
        path = self.jobMaster.imageCache.imagePath(troveSpec,
            jobmaster_helper.kernelData)

        f = open(path, 'w')
        f.write('')
        f.close()

        assert self.jobMaster.imageCache.haveImage(troveSpec,
            jobmaster_helper.kernelData)
        self.jobMaster.imageCache.deleteAllImages()
        assert not self.jobMaster.imageCache.haveImage(troveSpec,
            jobmaster_helper.kernelData)

    def testImageSize(self):
        self.assertEquals(imagecache.roundUpSize(0), 96509952)
        self.assertEquals(imagecache.roundUpSize(100000), 97026048)
        self.assertEquals(imagecache.roundUpSize(300 * 1024 * 1024), 458293248)

    def testCreateBlank(self):
        fd, tmpFile = tempfile.mkstemp()
        os.close(fd)
        try:
            imagecache.mkBlankFile(tmpFile, 1024, sparse = False)
            f = open(tmpFile)
            assert f.read() == 1024 * chr(0)
            f.close()
            imagecache.mkBlankFile(tmpFile, 512, sparse = True)
            f = open(tmpFile)
            assert f.read() == 512 * chr(0)
            f.close()
        finally:
            util.rmtree(tmpFile)

    def testCreateFile(self):
        tmpDir = tempfile.mkdtemp()
        try:
            filePath = 'test/path/file.txt'
            contents = 'test'

            imagecache.createFile(tmpDir, filePath, contents)

            f = open(os.path.join(tmpDir, filePath))
            assert f.read() == contents
            f.close()
        finally:
            util.rmtree(tmpDir)

    def testTempRoot(self):
        tmpDir = tempfile.mkdtemp()
        try:
            imagecache.createTemporaryRoot(tmpDir)
            self.failUnlessEqual(
                set(os.listdir(tmpDir)),
                set(['etc', 'boot', 'tmp', 'proc', 'sys', 'root', 'var']))
        finally:
            util.rmtree(tmpDir)

    def testWriteConaryRc(self):
        tmpDir = tempfile.mkdtemp()
        os.mkdir(os.path.join(tmpDir, 'etc'))
        try:
            imagecache.writeConaryRc(tmpDir, mirrorUrl = 'test')
            f = open(os.path.join(tmpDir, 'etc', 'conaryrc'))
            data = f.read()
            self.failIf(data != 'includeConfigFile http://test/conaryrc\n'
                        'pinTroves kernel.*\n'
                        'includeConfigFile /etc/conary/config.d/*\n',
                        "Unexpected contents of config file")
        finally:
            util.rmtree(tmpDir)

    def testFsOddsNEnds(self):
        tmpDir = tempfile.mkdtemp()
        try:
            imagecache.preScript(tmpDir)
            self.failUnlessEqual(
                set(os.listdir(tmpDir)),
                set(['boot', 'etc', 'var']))
            self.failUnlessEqual(
                set(os.listdir(os.path.join(tmpDir, 'etc'))),
                set(['conaryrc', 'fstab', 'hosts', 'sysconfig', 'inittab',
                    'securetty', 'rc.local']))
            self.failUnlessEqual(
                set(os.listdir(os.path.join(tmpDir, 'etc', 'sysconfig'))),
                set(['keyboard', 'network', 'network-scripts']))
            # make sure that /var/tmp -> /tmp (RBL-4202)
            p = os.path.join(tmpDir, 'var', 'tmp')
            self.failUnless(os.path.islink(p))
            self.failUnlessEqual(os.readlink(p), '../tmp')
        finally:
            util.rmtree(tmpDir)

    def testMakeImageKill(self):
        tmpDir = tempfile.mkdtemp()
        troveSpec = 'test=test.rpath.local@rpl:1'
        def waitForever(*args, **kwargs):
            while True:
                time.sleep(1)
        try:
            imageCache = imagecache.ImageCache(tmpDir, self.cfg)
            imageCache.makeImage = waitForever
            lockPath = imageCache.imagePath(troveSpec,
                jobmaster_helper.kernelData) + '.lock'
            pid = os.fork()
            if not pid:
                try:
                    imageCache.getImage(troveSpec,
                        jobmaster_helper.kernelData)
                finally:
                    os._exit(0)
            while not os.path.exists(lockPath):
                time.sleep(0.1)
            os.kill(pid, signal.SIGINT)

            count = 0
            while os.path.exists(lockPath):
                time.sleep(0.1)
                count += 1

                if count > 100:
                    break

            self.failIf(os.path.exists(lockPath),
                    "building lock was not removed by signal")
        finally:
            util.rmtree(tmpDir)


if __name__ == "__main__":
    testsuite.main()