#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved
#

import tempfile
from conary.lib.util import rmtree
from jobmaster.resource import Resource
from jobmaster.util import createFile


class TempDir(Resource):
    def __init__(self, prefix='tmp-'):
        Resource.__init__(self)
        self.path = tempfile.mkdtemp(prefix=prefix)

    def _close(self):
        rmtree(self.path)
        self.path = None

    def mount(self, fsTab, path, readOnly=True):
        fsTab.bind(self.path, path, readOnly=readOnly)

    def createFile(self, path, contents, mode=0644):
        createFile(self.path, path, contents, mode)
