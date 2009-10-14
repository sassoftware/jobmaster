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

    def __init__(self, generator, name):
        Resource.__init__(self)
        self.masterName = 'jm.' + name
        self.slaveName = 'js.' + name
        self.masterAddr, self.slaveAddr = generator.generateHostPair()

    def start(self):
        logCall(['/sbin/ip', 'link', 'add', 'name', self.masterName, 'type',
            'veth', 'peer', 'name', self.slaveName])

        # Configure the master end immediately. The slave end can only be
        # configured inside the cgroup.
        self._add(self.masterName, self.masterAddr)
        self._setUp(self.masterName)

    def _close(self):
        # When a cgroup is freed, all of the network devices that exist only in
        # that cgroup are freed. Hence it is likely that by the time this
        # resource is asked to clean up, the slave device has already been
        # freed by its cgroup, and the master device freed by the slave device.
        try:
            call(['/sbin/ip', 'link', 'del', self.masterName])
        except CommandError:
            # Checking first is racy, so check afterwards and only raise
            # if it's still present
            if os.path.isdir(os.path.join('/sys/class/net', self.masterName)):
                raise

    def moveSlave(self, pid):
        """
        Move the slave end of the network pair to a different cgroup.
        """
        logCall(['/sbin/ip', 'link', 'set', self.slaveName, 'netns', str(pid)])

    def finishConfiguration(self):
        """
        Configure loopback and the slave end of the network pair from inside
        the slave cgroup.
        """
        # Loopback
        self._add('lo', '127.0.0.1/8')
        self._add('lo', '::1/128')
        self._setUp('lo')
        #logCall(['/sbin/ip', '-6', 'route']);logCall(['/sbin/ip', '-6', 'addr'])

        # js.*
        self._add(self.slaveName, self.slaveAddr)
        self._setUp(self.slaveName)

        #logCall(['/sbin/ip', '-6', 'route']);logCall(['/sbin/ip', '-6', 'addr'])

    @staticmethod
    def _add(device, address):
        logCall(['/sbin/ip', 'addr', 'add', str(address), 'dev', device,
            'valid_lft', 'forever', 'preferred_lft', 'forever', 'nodad'])

    @staticmethod
    def _setUp(device):
        logCall(['/sbin/ip', 'link', 'set', device, 'up'])
