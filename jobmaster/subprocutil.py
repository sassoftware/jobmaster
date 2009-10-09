#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import errno
import logging
import os
import signal
import time

log = logging.getLogger(__name__)


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
    pid = None
    procName = "subprocess"
    setsid = False

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
                except:
                    log.exception("Unhandled exception in %s:", self.procName)
            finally:
                os._exit(70)
        return self.pid

    def run(self):
        raise NotImplementedError

    def check(self):
        """
        Return C{True} if the subprocess is running.
        """
        if not self.pid:
            return False
        if os.waitpid(self.pid, os.WNOHANG)[0]:
            self.pid = None
            return False
        return True

    def wait(self):
        """
        Wait for the process to exit, then return. Returns C{True} if the
        process was actually waited on, or C{False} if it didn't exist.
        """
        if not self.pid:
            return False
        while True:
            try:
                os.waitpid(self.pid, 0)
            except OSError, err:
                if err.errno == errno.EINTR:
                    # Interrupted -- keep waiting.
                    continue
                elif err.errno == errno.ECHILD:
                    # Process doesn't exist.
                    return False
                else:
                    raise
            else:
                # Process found and waited on.
                self.pid = None
                return True

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
