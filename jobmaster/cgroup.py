#
# Copyright (c) 2005-2009 rPath, Inc.
#
# All rights reserved.
#

import os

CGROUP_PATH = '/cgroup'


def _write(pid, path, contents):
    fObj = open(os.path.join(CGROUP_PATH, str(pid), path), 'w')
    fObj.write(contents)
    fObj.close()


def clearDeviceCaps(pid):
    _write(pid, 'devices.deny', 'b *:* rwm\nc *:* rwm\n')


def addDeviceCap(pid, kinds='a', major='*', minor='*', perms='rwm'):
    if kinds == 'a':
        addDeviceCap(pid, 'b', major, minor, perms)
        addDeviceCap(pid, 'c', major, minor, perms)
    else:
        _write(pid, 'devices.allow',
                ' '.join((kinds, '%s:%s' % (major, minor), perms)))
