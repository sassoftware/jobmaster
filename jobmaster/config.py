#/usr/bin/python
#
# Copyright (c) 2005-2009 rPath, Inc.
#
# All rights reserved
#

from conary.lib import cfgtypes
from mcp import config


CONFIG_PATH = '/srv/rbuilder/jobmaster/config'
RUNTIME_CONFIG_PATH = '/srv/rbuilder/jobmaster/config.d/runtime'


class MasterConfig(config.MCPConfig):
    # Paths
    basePath = '/srv/rbuilder/jobmaster'
    pidFile = '/var/run/jobmaster.pid'
    templateCache = 'anaconda-templates'

    # Runtime settings
    slaveLimit = (cfgtypes.CfgInt, 1)

    # Misc settings
    debugMode = (cfgtypes.CfgBool, False)
    lvmVolumeName = 'vg00'
    minSlaveSize = (cfgtypes.CfgInt, 1024) # scratch space in MB
    rbuilderUrl = 'http://127.0.0.1/'
    slaveSubnet = 'fdf0:dbe6:3760::/48'

    # DEPRECATED
    conaryProxy = None
    maxSlaveLimit = None
