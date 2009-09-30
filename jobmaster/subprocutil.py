#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import os
import signal
import time


class Subprocess(object):
    pid = None

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
        os.kill(self.pid, signal.SIGTERM)
        start = time.time()
        while time.time() - start < 1.0:
            if not self.check():
                break
            time.sleep(0.1)
        else:
            # If it's still going, use SIGKILL and wait indefinitely.
            os.kill(self.pid, signal.SIGKILL)
            self.wait()
