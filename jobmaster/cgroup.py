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

def create(pid):
    cgdir = os.path.join(CGROUP_PATH, str(pid))
    os.mkdir(cgdir)
    file(os.path.join(cgdir, "tasks"), "a").write("%s\n" % pid)

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
