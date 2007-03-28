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
import tempfile

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
        assert imagecache.roundUpSize(0) == 332881920
        assert imagecache.roundUpSize(100000) == 332881920
        assert imagecache.roundUpSize(300 * 1024 * 1024) == 694665216

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
            assert os.listdir(tmpDir) == \
                ['etc', 'boot', 'tmp', 'proc', 'sys', 'root', 'var']
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
            assert sorted(os.listdir(tmpDir)) == \
                ['boot', 'etc', 'var']
            assert sorted(os.listdir(os.path.join(tmpDir, 'etc'))) == \
                ['conaryrc', 'fstab', 'hosts', 'sysconfig']
            assert sorted(os.listdir(os.path.join(tmpDir, 'etc', 'sysconfig')))\
                == ['keyboard', 'network', 'network-scripts']
        finally:
            util.rmtree(tmpDir)


if __name__ == "__main__":
    testsuite.main()
