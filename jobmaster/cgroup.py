#
# Copyright (c) 2005-2009 rPath, Inc.
#
# All rights reserved.
#

import errno
import os

CGROUP_PATH = '/cgroup'


def _write(pid, path, contents):
    try:
        fObj = open(os.path.join(CGROUP_PATH, str(pid), path), 'w')
    except IOError, err:
        if err.errno == errno.ENOENT:
            try:
                os.stat(os.path.join(CGROUP_PATH, 'devices.list'))
            except OSError, err:
                if err.errno == errno.ENOENT:
                    raise RuntimeError("%s is not mounted or is not a cgroupfs"
                            % (CGROUP_PATH,))
                raise
            else:
                raise RuntimeError("%s is not a cgroup" % (pid,))
        raise
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
