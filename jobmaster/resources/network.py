#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import os
from jobmaster.networking import formatIPv6
from jobmaster.resource import Resource
from jobmaster.util import call, logCall, CommandError


class NetworkPairResource(Resource):
    """
    Resource that sets up and tears down a veth network pair.
    """

    def __init__(self, masterName, masterAddr, slaveName):
        Resource.__init__(self)
        self.masterName = masterName
        self.masterAddr = masterAddr
        self.slaveName = slaveName

        logCall(['/sbin/ip', 'link', 'add',
            'name', masterName, 'type', 'veth', 'peer', 'name', slaveName])
        logCall(['/sbin/ip', 'addr', 'add', formatIPv6(*masterAddr),
            'dev', masterName])
        logCall(['/sbin/ip', 'link', 'set', masterName, 'up'])

    def _close(self):
        try:
            call(['/sbin/ip', 'link', 'del', self.masterName])
        except CommandError:
            # Checking first is racy, so check afterwards and only raise
            # if it's still present
            if os.path.isdir(os.path.join('/sys/class/net', self.masterName)):
                raise
