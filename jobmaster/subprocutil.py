#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import errno
import fcntl
import logging
import os
import random
import signal
import time

log = logging.getLogger(__name__)


class LockError(RuntimeError):
    pass


class LockTimeoutError(LockError):
    pass


class Lockable(object):
    _lockFile = None
    _lockLevel = fcntl.LOCK_UN
    _lockPath = None

    @staticmethod
    def _sleep():
        time.sleep(random.uniform(0.1, 0.5))

    def _lock(self, mode=fcntl.LOCK_SH):
        assert self._lockPath

        # Short-circuit if we already have the lock
        if mode == self._lockLevel:
            return True

        if self._lockFile:
            lockFile = self._lockFile
        else:
            lockFile = self._lockFile = open(self._lockPath, 'w')

        try:
            try:
                fcntl.flock(self._lockFile.fileno(), mode | fcntl.LOCK_NB)
            except IOError, err:
                if err.errno in (errno.EACCES, errno.EAGAIN):
                    # Already locked, retry later.
                    raise LockError('Could not acquire lock')
                raise
            else:
                self._lockFile = lockFile
                self._lockLevel = mode

        finally:
            if mode == fcntl.LOCK_UN:
                # If we don't have any lock at the moment then close the file
                # so that if another process deletes the lockfile we don't end
                # up locking the now-nameless file. The other process *must*
                # hold an exclusive lock to delete the lockfile, so this
                # assures lock safety.
                self._lockFile.close()
                self._lockFile = None

        return True

    def _lockWait(self, mode=fcntl.LOCK_SH, timeout=600.0, breakIf=None):
        logged = False
        runUntil = time.time() + timeout
        while True:
            # First, try to lock.
            try:
                return self._lock(mode)
            except LockError:
                pass

            if breakIf and breakIf():
                return False

            if time.time() > runUntil:
                raise LockTimeoutError('Timed out waiting for lock')

            if not logged:
                logged = True
                log.debug("Waiting for lock")

            self._sleep()

    def _deleteLock(self):
        self._lock(fcntl.LOCK_EX)
        os.unlink(self._lockPath)
        self._lock(fcntl.LOCK_UN)

    def _close(self):
        if self._lockFile:
            self._lockFile.close()
            self._lockFile = None


class Pipe(object):
    def __init__(self):
        readFD, writeFD = os.pipe()
        self.reader = os.fdopen(readFD, 'rb')
        self.writer = os.fdopen(writeFD, 'wb')

    def closeReader(self):
        self.reader.close()

    def closeWriter(self):
        self.writer.close()

    def close(self):
        self.closeReader()
        self.closeWriter()

    def read(self):
        self.reader.read()

    def write(self, data):
        self.writer.write(data)


class Subprocess(object):
    procName = "subprocess"
    setsid = False

    exitStatus = -1
    pid = None

    @property
    def exitCode(self):
        if self.exitStatus < 0:
            return self.exitStatus
        elif os.WIFEXITED(self.exitStatus):
            return os.WEXITSTATUS(self.exitStatus)
        else:
            return -2

    def start(self):
        self.pid = os.fork()
        if not self.pid:
            #pylint: disable-msg=W0702,W0212
            try:
                try:
                    if self.setsid:
                        os.setsid()
                    ret = self.run()
                    if not isinstance(ret, (int, long)):
                        ret = bool(ret)
                    os._exit(ret)
                except SystemExit, err:
                    os._exit(err.code)
                except:
                    log.exception("Unhandled exception in %s:", self.procName)
            finally:
                os._exit(70)
        return self.pid

    def run(self):
        raise NotImplementedError

    def _subproc_wait(self, flags):
        if not self.pid:
            return False
        while True:
            try:
                pid, status = os.waitpid(self.pid, flags)
            except OSError, err:
                if err.errno == errno.EINTR:
                    # Interrupted by signal so wait again.
                    continue
                elif err.errno == errno.ECHILD:
                    # Process doesn't exist.
                    self.pid = None
                    self.exitStatus = -1
                    return False
                else:
                    raise
            else:
                if pid:
                    # Process exists and is no longer running.
                    self.pid = None
                    self.exitStatus = status
                    return False
                else:
                    # Process exists and is still running.
                    return True

    def check(self):
        """
        Return C{True} if the subprocess is running.
        """
        return self._subproc_wait(os.WNOHANG)

    def wait(self):
        """
        Wait for the process to exit, then return. Returns the exit code if the
        process exited normally, -2 if the process exited abnormally, or -1 if
        the process does not exist.
        """
        self._subproc_wait(0)
        return self.exitCode

    def kill(self):
        """
        Kill the subprocess and wait for it to exit.
        """
        if not self.pid:
            return
        # Try SIGTERM first, but don't wait for longer than 1 second.
        try:
            os.kill(self.pid, signal.SIGTERM)
        except OSError, err:
            if err.errno != errno.ESRCH:
                raise
            # Process doesn't exist (or is a zombie)
        start = time.time()
        while time.time() - start < 1.0:
            if not self.check():
                break
            time.sleep(0.1)
        else:
            # If it's still going, use SIGKILL and wait indefinitely.
            os.kill(self.pid, signal.SIGKILL)
            self.wait()
