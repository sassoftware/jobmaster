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

from conary.lib import util
from jobmaster import imagecache

class CacheTest(jobmaster_helper.JobMasterHelper):
    def testImagePath(self):
        path = self.jobMaster.imageCache.imagePath('notreal')
        assert path.startswith(os.path.join(self.cfg.basePath, 'imageCache'))
        assert path.endswith('cd5c569173452f8438cf9bbe84d811fa')

    def testGetExistingImage(self):
        troveSpec = 'existingImage'
        path = self.jobMaster.imageCache.imagePath(troveSpec)

        f = open(path, 'w')
        f.write('')
        f.close()

        def stubMakeImage(troveSpec, hash):
            self.fail('makeImage should not have been called')

        origMakeImage = self.jobMaster.imageCache.makeImage
        try:
            self.jobMaster.imageCache.makeImage = stubMakeImage
            self.jobMaster.imageCache.getImage(troveSpec)
        finally:
            self.jobMaster.imageCache.makeImage = origMakeImage

    def testMissingImage(self):
        troveSpec = 'nonExistentImage'

        def stubMakeImage(troveSpec, hash):
            return 'makeImage was called successfully'

        origMakeImage = self.jobMaster.imageCache.makeImage
        try:
            self.jobMaster.imageCache.makeImage = stubMakeImage
            assert self.jobMaster.imageCache.getImage(troveSpec) == \
                'makeImage was called successfully'
        finally:
            self.jobMaster.imageCache.makeImage = origMakeImage

    def testMissingImageCollide(self):
        troveSpec = 'nonExistentImage'

        lockDir = self.jobMaster.imageCache.imagePath(troveSpec) + '.lock'
        util.mkdirChain(lockDir)

        def stubMakeImage(troveSpec, hash):
            return 'makeImage was called successfully'

        def dummySleep(*args, **kwargs):
            util.rmtree(lockDir)

        sleep = time.sleep
        origMakeImage = self.jobMaster.imageCache.makeImage
        try:
            time.sleep = dummySleep
            self.jobMaster.imageCache.makeImage = stubMakeImage
            assert self.jobMaster.imageCache.getImage(troveSpec) == \
                'makeImage was called successfully'
        finally:
            time.sleep = sleep
            self.jobMaster.imageCache.makeImage = origMakeImage

    def testHaveImage(self):
        troveSpec = 'notReal'
        path = self.jobMaster.imageCache.imagePath(troveSpec)
        assert not self.jobMaster.imageCache.haveImage(troveSpec)

        f = open(path, 'w')
        f.write('')
        f.close()

        assert self.jobMaster.imageCache.haveImage(troveSpec)

    def testDeleteImages(self):
        troveSpec = 'fakeImafe'
        path = self.jobMaster.imageCache.imagePath(troveSpec)

        f = open(path, 'w')
        f.write('')
        f.close()

        assert self.jobMaster.imageCache.haveImage(troveSpec)
        self.jobMaster.imageCache.deleteAllImages()
        assert not self.jobMaster.imageCache.haveImage(troveSpec)

    def testImageSize(self):
        # Put the default 256MB swap size back temporarily
        # (it's reduced since some tests actually create files)
        oldSwapSize, imagecache.SWAP_SIZE = imagecache.SWAP_SIZE, 256 * 1048576
        self.assertEquals(imagecache.roundUpSize(0), 332881920)
        self.assertEquals(imagecache.roundUpSize(100000), 332881920)
        self.assertEquals(imagecache.roundUpSize(300 * 1024 * 1024), 694665216)
        imagecache.SWAP_SIZE = oldSwapSize

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
            filePath = os.path.join(tmpDir, 'test', 'path', 'file.txt')

            contents = 'test'
            imagecache.createFile(filePath, contents)

            f = open(filePath)
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
            imagecache.fsOddsNEnds(tmpDir)
            self.failUnlessEqual(
                set(os.listdir(tmpDir)),
                set(['boot', 'etc', 'var']))
            self.failUnlessEqual(
                set(os.listdir(os.path.join(tmpDir, 'etc'))),
                set(['conaryrc', 'fstab', 'hosts', 'sysconfig', 'inittab',
                    'securetty']))
            self.failUnlessEqual(
                set(os.listdir(os.path.join(tmpDir, 'etc', 'sysconfig'))),
                set(['keyboard', 'network', 'network-scripts']))
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
            lockPath = imageCache.imagePath(troveSpec) + '.lock'
            pid = os.fork()
            if not pid:
                try:
                    imageCache.getImage(troveSpec)
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

    def testGetRunningKernel(self):
        self.count = 0
        def FakePopen(*args, **kwargs):
            try:
                if self.count:
                    assert '2.6.22.4-0.0.1' in args[0]
                    return StringIO.StringIO('kernel version from conary')
                else:
                    return StringIO.StringIO( \
                            '2.6.22.4-0.0.1.smp.gcc3.4.x86.i686')
            finally:
                self.count += 1

        popen = os.popen
        try:
            os.popen = FakePopen
            res = imagecache.getRunningKernel()
            self.assertEquals('kernel version from conary', res)
        finally:
            os.popen = popen

if __name__ == "__main__":
    testsuite.main()
