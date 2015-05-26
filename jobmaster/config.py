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


import logging
import os
from conary.lib import cfgtypes
from mcp import config


CONFIG_PATH = '/srv/rbuilder/jobmaster/config'
RUNTIME_CONFIG_PATH = '/srv/rbuilder/jobmaster/config.d/runtime'


class MasterConfig(config.MCPConfig):
    # Paths
    basePath = '/srv/rbuilder/jobmaster'
    pidFile = '/var/run/jobmaster.pid'
    templateCache = 'anaconda-templates'
    logPath = '/var/log/rbuilder/jobmaster.log'

    # Runtime settings
    slaveLimit = (cfgtypes.CfgInt, 5)

    # Misc settings
    conaryProxyPort = (cfgtypes.CfgInt, 80)
    debugMode       = (cfgtypes.CfgBool, False)
    lvmVolumeName   = 'vg00'
    masterProxyPort = (cfgtypes.CfgInt, 7770)
    minSlaveSize    = (cfgtypes.CfgInt, 1024) # scratch space in MB
    pairSubnet      = 'fdf0:dbe6:3760::/48'
    useNetContainer = (cfgtypes.CfgBool, True)

    # DEPRECATED
    conaryProxy = None
    maxSlaveLimit = None
    troveName = None
    troveVersion = None

    def getTemplateCache(self):
        return os.path.join(self.basePath, self.templateCache)

    def getLogLevel(self):
        level = self.logLevel
        if isinstance(level, basestring):
            level = logging.getLevelName(level.upper())
        return level
