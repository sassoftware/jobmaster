#
# Copyright (c) 2007, 2009 rPath, Inc.
#
# All rights reserved
#

import os
import logging
import select
import subprocess
import sys

from conary import conarycfg, conaryclient


log = logging.getLogger(__name__)


class CommandError(RuntimeError):
    def __init__(self, cmd, rv, stdout, stderr):
        self.cmd = cmd
        self.rv = rv
        self.stdout = stdout
        self.stderr = stderr
        self.args = (cmd, rv, stdout, stderr)

    def __str__(self):
        return "Error executing command: %s (return code %d)" % (
                self.cmd, self.rv)


class OutOfSpaceError(RuntimeError):
    def __init__(self, required, free):
        self.required = required
        self.free = free
        self.args = (required, free)

    def __str__(self):
        return ("Not enough scratch space for build: %d extents required "
                "but only %d free" % (self.required, self.free))


def rewriteFile(template, target, data):
    if not os.path.exists(template):
        return
    f = open(template, 'r')
    templateData = f.read()
    f.close()
    f = open(target, 'w')
    f.write(templateData % data)
    f.close()
    os.unlink(template)


def _getLogger(levels=2):
    caller = sys._getframe(levels)
    name = caller.f_globals['__name__']
    return logging.getLogger(name)


def logCall(cmd, ignoreErrors=False, logCmd=True, captureOutput=True, **kw):
    """
    Run command C{cmd}, logging the command run and all its output.

    If C{cmd} is a string, it will be interpreted as a shell command.
    Otherwise, it should be a list where the first item is the program name and
    subsequent items are arguments to the program.

    @param cmd: Program or shell command to run.
    @type  cmd: C{basestring or list}
    @param ignoreErrors: If C{True}, don't raise an exception on a 
            non-zero return code.
    @type  ignoreErrors: C{bool}
    @param logCmd: If C{False}, don't log the command invoked.
    @type  logCmd: C{bool}
    @param kw: All other keyword arguments are passed to L{subprocess.Popen}
    @type  kw: C{dict}
    """
    logger = _getLogger()

    if logCmd:
        if isinstance(cmd, basestring):
            niceString = cmd
        else:
            niceString = ' '.join(repr(x) for x in cmd)
        env = kw.get('env', {})
        env = ''.join(['%s="%s" ' % (k,v) for k,v in env.iteritems()])
        logger.info("+ %s%s", env, niceString)

    kw.setdefault('close_fds', True)
    kw.setdefault('shell', isinstance(cmd, basestring))
    if 'stdin' not in kw:
        kw['stdin'] = open('/dev/null')

    pipe = captureOutput and subprocess.PIPE or None
    kw.setdefault('stdout', pipe)
    kw.setdefault('stderr', pipe)
    p = subprocess.Popen(cmd, **kw)

    stdout = stderr = ''
    if captureOutput:
        while p.poll() is None:
            rList = [x for x in (p.stdout, p.stderr) if x]
            rList, _, _ = select.select(rList, [], [])
            for rdPipe in rList:
                line = rdPipe.readline()
                if rdPipe is p.stdout:
                    which = 'stdout'
                    stdout += line
                else:
                    which = 'stderr'
                    stderr += line
                if logCmd and line.strip():
                    logger.info("++ (%s) %s", which, line.rstrip())

        # pylint: disable-msg=E1103
        stdout_, stderr_ = p.communicate()
        if stderr_ is not None:
            stderr += stderr_
            if logCmd:
                for x in stderr_.splitlines():
                    logger.info("++ (stderr) %s", x)
        if stdout_ is not None:
            stdout += stdout_
            if logCmd:
                for x in stdout_.splitlines():
                    logger.info("++ (stdout) %s", x)
    else:
        p.wait()

    if p.returncode and not ignoreErrors:
        raise CommandError(cmd, p.returncode, stdout, stderr)
    else:
        return p.returncode, stdout, stderr


def getIP():
    p = os.popen("""/sbin/ifconfig `/sbin/route | grep "^default" | sed "s/.* //"` | grep "inet addr" | awk -F: '{print $2}' | sed 's/ .*//'""")
    data = p.read().strip()
    p.close()
    return data

def getRunningKernel():
    # Get the current kernel version
    p = os.popen('uname -r')
    kernel_name = p.read().strip()
    p.close()

    cfg = conarycfg.ConaryConfiguration(True)
    cc = conaryclient.ConaryClient(cfg)

    # Determine paths for kernel and initrd
    kernel_path = '/boot/vmlinuz-' + kernel_name
    initrd_path = '/boot/initrd-' + kernel_name + '.img'
    ret = dict(uname=kernel_name, kernel=kernel_path, initrd=initrd_path)

    # Check if conary owns this as a standardized file in /boot
    if os.path.exists(kernel_path):
        # Easy! Conary should own this.
        troves = cc.db.iterTrovesByPath(kernel_path)
        if troves:
            kernel = troves[0].getNameVersionFlavor()
            logging.debug('Selected kernel %s=%s[%s] based on running '
                'kernel at %s', kernel[0], kernel[1], kernel[2], kernel_path)
            ret['trove'] = kernel
            return ret

    # Get the latest "kernel:runtime" trove instead and pray
    troveSpec = ('kernel:runtime', None, None)
    troves = cc.db.findTroves(None, [('kernel:runtime', None, None)])[troveSpec]
    max_version = max(x[1] for x in troves)
    kernel = [x for x in troves if x[1] == max_version][0]
    if kernel:
        logging.warning('Could not determine running kernel by file. '
            'Falling back to latest kernel: %s=%s[%s]', kernel[0],
            kernel[1], kernel[2])
        ret['trove'] = kernel
        return ret
    else:
        raise RuntimeError('Could not determine currently running kernel')


def allocateScratch(cfg, name, disks):
    # Determine how many free extents there are, and how big an extent is.
    ret = logCall(['/usr/sbin/lvm', 'vgs',
        '-o', 'extent_size,free_count', cfg.lvmVolumeName],
        logCmd=False)[1]
    ret = ret.splitlines()[1:]
    if not ret:
        raise RuntimeError("Volume group %s could not be read"
                % (cfg.lvmVolumeName,))
    extent_size, extents_free = ret[0].split()
    assert extent_size.endswith('M')
    extent_size = 1048576 * int(float(extent_size[:-1]))
    extents_free = int(extents_free)

    to_allocate = []
    extents_required = 0
    for suffix, bytes, fuzzy in disks:
        # Round up to the nearest extent
        extents = (int(bytes) + extent_size - 1) / extent_size

        extents_required += extents
        if extents_required > extents_free and fuzzy:
            # Shrink the disk to what's available.
            shrink = extents_required - extents_free
            log.warning("Shrinking disk %s-%s from %d to %d extents due to "
                    "scratch shortage", name, suffix,
                    extents, extents - shrink)
            extents -= shrink
            extents_required -= shrink

        to_allocate.append((suffix, extents))

    if extents_required > extents_free:
        raise OutOfSpaceError(extents_required, extents_free)

    for suffix, extents in to_allocate:
        diskName = '%s-%s' % (name, suffix)
        logCall(['/usr/sbin/lvm', 'lvcreate', '-l', str(extents),
            '-n', diskName, cfg.lvmVolumeName], stdout=null())


def null():
    return open('/dev/null', 'w')
