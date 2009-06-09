#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import hashlib
import os
import logging
import select
import subprocess
import sys
from conary import conarycfg, conaryclient

log = logging.getLogger(__name__)


def _getLogger(levels=2):
    """
    Get a logger for the function two stack frames up, e.g. the caller of the
    function calling this one.
    """
    caller = sys._getframe(levels)
    name = caller.f_globals['__name__']
    return logging.getLogger(name)


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


def call(cmd, ignoreErrors=False, logCmd=False, logLevel=logging.DEBUG,
        captureOutput=True, **kw):
    """
    Run command C{cmd}, optionally logging the invocation and output.

    If C{cmd} is a string, it will be interpreted as a shell command.
    Otherwise, it should be a list where the first item is the program name and
    subsequent items are arguments to the program.

    @param cmd: Program or shell command to run.
    @type  cmd: C{basestring or list}
    @param ignoreErrors: If C{False}, a L{CommandError} will be raised if the
            program exits with a non-zero return code.
    @type  ignoreErrors: C{bool}
    @param logCmd: If C{True}, log the invocation and its output.
    @type  logCmd: C{bool}
    @param captureOutput: If C{True}, standard output and standard error are
            captured as strings and returned.
    @type  captureOutput: C{bool}
    @param kw: All other keyword arguments are passed to L{subprocess.Popen}
    @type  kw: C{dict}
    """
    logger = _getLogger(kw.pop('_levels', 2))

    if logCmd:
        if isinstance(cmd, basestring):
            niceString = cmd
        else:
            niceString = ' '.join(repr(x) for x in cmd)
        env = kw.get('env', {})
        env = ''.join(['%s="%s" ' % (k,v) for k,v in env.iteritems()])
        logger.log(logLevel, "+ %s%s", env, niceString)

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
                    logger.log(logLevel, "++ (%s) %s", which, line.rstrip())

        # pylint: disable-msg=E1103
        stdout_, stderr_ = p.communicate()
        if stderr_ is not None:
            stderr += stderr_
            if logCmd:
                for x in stderr_.splitlines():
                    logger.log(logLevel, "++ (stderr) %s", x)
        if stdout_ is not None:
            stdout += stdout_
            if logCmd:
                for x in stdout_.splitlines():
                    logger.log(logLevel, "++ (stdout) %s", x)
    else:
        p.wait()

    if p.returncode and not ignoreErrors:
        raise CommandError(cmd, p.returncode, stdout, stderr)
    else:
        return p.returncode, stdout, stderr


def logCall(cmd, **kw):
    # This function logs by default.
    kw.setdefault('logCmd', True)

    # _getLogger() will need to go out an extra frame to get the original
    # caller's module name.
    kw['_levels'] = 3

    return call(cmd, **kw)


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
            log.debug('Selected kernel %s=%s[%s] based on running '
                'kernel at %s', kernel[0], kernel[1], kernel[2], kernel_path)
            ret['trove'] = kernel
            return ret

    # Get the latest "kernel:runtime" trove instead and pray
    troveSpec = ('kernel:runtime', None, None)
    troves = cc.db.findTroves(None, [('kernel:runtime', None, None)])[troveSpec]
    max_version = max(x[1] for x in troves)
    kernel = [x for x in troves if x[1] == max_version][0]
    if kernel:
        log.warning('Could not determine running kernel by file. '
            'Falling back to latest kernel: %s=%s[%s]', kernel[0],
            kernel[1], kernel[2])
        ret['trove'] = kernel
        return ret
    else:
        raise RuntimeError('Could not determine currently running kernel')


def setupLogging(logLevel=logging.INFO, toStderr=True, toFile=None):
    """
    Set up a root logger with default options and possibly a file to
    log to.
    """
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s')

    rootLogger = logging.getLogger()
    rootLogger.setLevel(logLevel)

    if toStderr:
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        rootLogger.addHandler(streamHandler)

    if toFile:
        fileHandler = logging.FileHandler(toFile)
        fileHandler.setFormatter(formatter)
        rootLogger.addHandler(fileHandler)


def specHash(troveTups):
    """
    Create a unique identifier for the troves C{troveTups}.
    """
    ctx = hashlib.sha1()
    for tup in sorted(troveTups):
        ctx.update('%s\0%s\0%s\0' % tup)
    return ctx.hexdigest()


def createDirectory(fsRoot, path, mode=0755):
    """
    Create a directory at C{fsRoot}/C{path} with mode C{mode} if it
    doesn't exist, creating intermediate directories as needed.
    """
    path = os.path.join(fsRoot, path)
    if not os.path.isdir(path):
        os.makedirs(path)
        os.chmod(path, mode)


def _writeContents(fObj, contents):
    """
    Write C{contents} to C{fObj}, stripping leading whitespace from
    each line.
    """
    for line in contents.splitlines():
        print >> fObj, line.lstrip()


def appendFile(fsRoot, path, contents):
    """
    Append C{contents} to the file at C{fsRoot}/C{path}.

    C{contents} may contain leading whitespace on each line, which will
    be stripped -- this is to allow the use of indented multiline
    strings.
    """
    path = os.path.join(fsRoot, path)
    fObj = open(path, 'a')
    _writeContents(fObj, contents)
    fObj.close()


def createFile(fsRoot, path, contents, mode=0644):
    """
    Create a file at C{fsRoot}/C{path} with contents C{contents} and
    mode C{mode}.

    C{contents} may contain leading whitespace on each line, which will
    be stripped -- this is to allow the use of indented multiline
    strings.
    """
    createDirectory(fsRoot, os.path.dirname(path))
    path = os.path.join(fsRoot, path)
    fObj = open(path, 'w')
    _writeContents(fObj, contents)
    fObj.close()
    os.chmod(path, mode)

