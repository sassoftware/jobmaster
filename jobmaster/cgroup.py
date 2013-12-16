#
# Copyright (c) SAS Institute Inc.
#

import logging
import errno
import os

log = logging.getLogger(__name__)

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
    _write(pid, 'devices.deny', 'a *:* rwm\n')


def addDeviceCap(pid, kinds='a', major='*', minor='*', perms='rwm'):
    if kinds == 'a':
        addDeviceCap(pid, 'b', major, minor, perms)
        addDeviceCap(pid, 'c', major, minor, perms)
    else:
        _write(pid, 'devices.allow',
                ' '.join((kinds, '%s:%s' % (major, minor), perms)))


def cleanup(pid):
    try:
        os.rmdir(os.path.join(CGROUP_PATH, str(pid)))
    except OSError, err:
        if err.errno == errno.ENOENT:
            return
        elif err.errno == errno.EBUSY:
            log.warning("Unable to cleanup cgroup for worker %s, "
                    "it is still in use", pid)
        else:
            log.exception("Unable to cleanup cgroup for worker %s:", pid)
    except:
        log.exception("Unable to cleanup cgroup for worker %s:", pid)
