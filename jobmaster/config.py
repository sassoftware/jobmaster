#/usr/bin/python
#
# Copyright (c) 2005-2009 rPath, Inc.
#
# All rights reserved
#

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

    # Trove source settings
    troveName       = (cfgtypes.CfgString, 'group-jobslave')
    troveVersion    = (cfgtypes.CfgString, None)

    # Misc settings
    conaryProxyPort = (cfgtypes.CfgInt, 80)
    debugMode       = (cfgtypes.CfgBool, False)
    lvmVolumeName   = 'vg00'
    masterProxyPort = (cfgtypes.CfgInt, 7770)
    minSlaveSize    = (cfgtypes.CfgInt, 1024) # scratch space in MB
    pairSubnet      = 'fdf0:dbe6:3760::/48'

    # DEPRECATED
    conaryProxy = None
    maxSlaveLimit = None

    def getTemplateCache(self):
        return os.path.join(self.basePath, self.templateCache)