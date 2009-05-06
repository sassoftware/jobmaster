#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

from conary.lib.cfg import ConfigFile
from conary.lib.cfgtypes import CfgBool, CfgEnum, CfgInt, CfgPath, CfgString


class CfgLogLevel(CfgEnum):
    validValues = ['CRITICAL', 'WARNING', 'INFO', 'DEBUG']
    def checkEntry(self, val):
        CfgEnum.checkEntry(self, val.upper())


class MasterConfig(ConfigFile):
    basePath = (CfgPath, '/srv/rbuilder/jobmaster')

    logFile = (CfgPath, '/var/log/rbuilder/jobmaster.log')
    logLevel = (CfgLogLevel, 'INFO')

    slaveLimit = (CfgInt, 1)
    nodeName = (CfgString, None)

    # Jobslave parameters
    archiveRoots = (CfgBool, False)
    lvmVolumeName = 'vg00'
    minSlaveSize = (CfgInt, 1024, "Minimum scratch space in MiB")
    masterIP = (CfgString, 'fdf0:dbe6:3760::/64',
            "IPv6 address for the jobmaster on the private VM network")

    # This should either be the URI of a rBuilder, or "self" to use the
    # local IP. It must be an rBuilder since the template generation code
    # assumes it can find a conaryrc file here.
    conaryProxy = 'self'

    # DEPRECATED: These are ignored for backwards compatibility
    debugMode = (CfgBool, False)
    maxSlaveLimit = (CfgInt, 0)
    templateCache = (CfgString, 'anaconda-templates')
