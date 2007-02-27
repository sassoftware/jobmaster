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


if __name__ == "__main__":
    testsuite.main()
