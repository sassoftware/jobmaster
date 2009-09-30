#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import os
from jobmaster.networking import AddressGenerator
from jobmaster.resource import Resource
from jobmaster.util import call, logCall, CommandError


class NetworkPairResource(Resource):
    """
    Resource that sets up and tears down a veth network pair.
    """

    def __init__(self, name):
        Resource.__init__(self)
        self.masterName = 'jm.' + name
        self.slaveName = 'js.' + name
        self.masterAddr, self.slaveAddr = AddressGenerator().generateHostPair()

    def start(self):
        logCall(['/sbin/ip', 'link', 'add', 'name', self.masterName, 'type',
            'veth', 'peer', 'name', self.slaveName])

        logCall(['/sbin/ip', 'addr', 'add', self.masterAddr.format(True),
            'dev', self.masterName])
        logCall(['/sbin/ip', 'link', 'set', self.masterName, 'up'])

        logCall(['/sbin/ip', 'addr', 'add', self.slaveAddr.format(True),
            'dev', self.slaveName])
        logCall(['/sbin/ip', 'link', 'set', self.slaveName, 'up'])

    def _close(self):
        try:
            call(['/sbin/ip', 'link', 'del', self.masterName])
        except CommandError:
            # Checking first is racy, so check afterwards and only raise
            # if it's still present
            if os.path.isdir(os.path.join('/sys/class/net', self.masterName)):
                raise

    def moveSlave(self, cgroup):
        """
        Move the slave end of the network pair to a different C{cgroup}.
        """
