#
# Copyright (c) 2007, 2009 rPath, Inc.
#
# All rights reserved
#

import errno
import logging
import os
import select
import subprocess
import sys
import tempfile
from conary.lib import digestlib
from jobmaster.osutil import _close_fds

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


def devNull():
    return open('/dev/null', 'w+')


def bindMount(device, mount, **kwargs):
    return logCall(['/bin/mount', '-n', '--bind', device, mount], **kwargs) 


def call(cmd, ignoreErrors=False, logCmd=False, logLevel=logging.DEBUG,
        captureOutput=True, wait=True, **kw):
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
        kw['stdin'] = devNull()

    pipe = captureOutput and subprocess.PIPE or None
    kw.setdefault('stdout', pipe)
    kw.setdefault('stderr', pipe)
    p = subprocess.Popen(cmd, **kw)

    stdout = stderr = ''
    if captureOutput:
        while p.poll() is None:
            rList = [x for x in (p.stdout, p.stderr) if x]
            rList, _, _ = tryInterruptable(select.select, rList, [], [])
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
    elif wait:
        tryInterruptable(p.wait)

    if not wait:
        return p
    elif p.returncode and not ignoreErrors:
        raise CommandError(cmd, p.returncode, stdout, stderr)
    else:
        return p.returncode, stdout, stderr


def close_fds(exceptions=(0, 1, 2)):
    """
    Close all file descriptors, except for the ones listed in C{exceptions}.
    """
    _close_fds(sorted(exceptions))


def makeConstants(name, definition):
    """
    Given a string of space-separated names, create a class with those names as
    attributes assigned sequential integer values. The class also has C{names}
    and C{values} dictionaries to map from name to value and from value to name.
    """
    nameList = definition.split()
    valueList = range(len(nameList))
    typedict = {
            '__slots__': (),
            'names': dict(zip(nameList, valueList)),
            'values': dict(zip(valueList, nameList)),
            }
    typedict.update(typedict['names'])
    return type(name, (object,), typedict)


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


def logCall(cmd, **kw):
    # This function logs by default.
    kw.setdefault('logCmd', True)

    # _getLogger() will need to go out an extra frame to get the original
    # caller's module name.
    kw['_levels'] = 3

    return call(cmd, **kw)


def mount(device, mount, fstype, options='', **kwargs):
    args = ['/bin/mount', '-n', '-t', fstype, device, mount]
    if options:
        args.extend(('-o', options))
    return logCall(args, **kwargs)


def setupLogging(logLevel=logging.INFO, toStderr=True, toFile=None):
    """
    Set up a root logger with default options and possibly a file to
    log to.
    """
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s')

    if isinstance(logLevel, basestring):
        logLevel = logging.getLevelName(logLevel.upper())

    rootLogger = logging.getLogger()
    rootLogger.setLevel(logLevel)
    rootLogger.handlers = []

    if toStderr:
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        rootLogger.addHandler(streamHandler)

    if toFile:
        fileHandler = logging.FileHandler(toFile)
        fileHandler.setFormatter(formatter)
        rootLogger.addHandler(fileHandler)


def specHash(troveTups, buildTimes=None):
    """
    Create a unique identifier for the troves C{troveTups}.
    """
    if buildTimes:
        assert len(troveTups) == len(buildTimes)
        troveTups = zip(troveTups, buildTimes)
    else:
        troveTups = [(x, None) for x in troveTups]

    items = []
    for (name, version, flavor), buildTime in sorted(troveTups):
        items.append(name)
        if buildTime:
            items.append(version.trailingRevision().version)
            items.append(long(buildTime))
        else:
            items.append(version.freeze())
        items.append(flavor.freeze())
    items.append('')
    return digestlib.sha1('\0'.join(str(x) for x in items)).hexdigest()


def tryInterruptable(func, *args, **kwargs):
    while True:
        try:
            return func(*args, **kwargs)
        except Exception, err:
            if getattr(err, 'errno', None) == errno.EINTR:
                continue
            else:
                raise


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


def createFile(fsRoot, path, contents='', mode=0644):
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


class AtomicFile(object):
    """
    Open a temporary file adjacent to C{path} for writing. When
    C{f.commit()} is called, the temporary file will be flushed and
    renamed on top of C{path}, constituting an atomic file write.
    """

    fObj = None

    def __init__(self, path, mode='w+b', chmod=0644):
        self.finalPath = os.path.realpath(path)
        self.finalMode = chmod

        fDesc, self.name = tempfile.mkstemp(dir=os.path.dirname(self.finalPath))
        self.fObj = os.fdopen(fDesc, mode)

    def __getattr__(self, name):
        return getattr(self.fObj, name)

    def commit(self, returnHandle=False):
        """
        C{flush()}, C{chmod()}, and C{rename()} to the target path.
        C{close()} afterwards.
        """
        if self.fObj.closed:
            raise RuntimeError("Can't commit a closed file")

        # Flush and change permissions before renaming so the contents
        # are immediately present and accessible.
        self.fObj.flush()
        os.chmod(self.name, self.finalMode)
        os.fsync(self.fObj)

        # Rename to the new location. Since both are on the same
        # filesystem, this will atomically replace the old with the new.
        os.rename(self.name, self.finalPath)

        # Now close the file.
        if returnHandle:
            fObj, self.fObj = self.fObj, None
            return fObj
        else:
            self.fObj.close()

    def close(self):
        if self.fObj and not self.fObj.closed:
            os.unlink(self.name)
            self.fObj.close()
    __del__ = close


def prettySize(num):
    for power, suffix in ((3, 'GiB'), (2, 'MiB'), (1, 'KiB')):
        if num >= 1024 ** power:
            return '%.01f %s' % (float(num) / (1024 ** power), suffix)
    else:
        return '%d B' % num
