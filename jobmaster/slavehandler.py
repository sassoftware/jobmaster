#
# Copyright (c) 2005-2006 rPath, Inc.
#
# All rights reserved
#

import os
import tempfile
import threading
import weakref

from jobmaster import xencfg

class XenHandler(threading.Thread):
    def __init__(self, imageCache):
        self.imageCache = weakref.ref(imageCache)
        self.cfgPath = ''

    def startSlave(self, troveSpec):
        imageCache = self.imageCache()
        assert imageCache
        if not imageCache.hasImage(troveSpec):
            self.slaveStatus(data, 'building')
        imagePath = imageCache.imagePath(troveSpec)
        xenCfg = xencfg.XenCfg(imagePath, {'memory' : 512})
        import tempfile
        fd, self.cfgPath = tempfile.mkstemp()
        os.close(fd)
        f = open(self.cfgPath, 'w')
        xenCfg.write(self.cfgPath)
        f.close()
        threading.Thread.start(self)
        self.slaveName = xenCfg.cfg['name']
        return self.slaveName

    def stopSlave(self):
        os.system('xm destroy %s' % self.slaveName)

    def run(self):
        imageCache = self.imageCache()
        imageCache.getImage(troveSpec)
        os.system('xm create %s' % cfgPath)
        self.slaveStatus(data, 'running')

    def __del__(self):
        if os.path.exists(self.cfgPath):
            os.unlink(self.cfgPath)
