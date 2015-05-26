#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
