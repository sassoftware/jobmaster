#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import os
import logging
import subprocess
log = logging

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



def logCall(cmd, ignoreErrors = False, **kwargs):
    log.info("+ " + cmd)
    p = subprocess.Popen(cmd, shell = True,
        stdout = subprocess.PIPE, stderr = subprocess.PIPE, **kwargs)
    while p.poll() is None:
        err = p.stdout.read()
        if err:
            [log.info("++ " + errLine) for errLine in err.split("\n")]
        err = p.stderr.readline()
        if err:
            [log.debug("++ " + errLine) for errLine in err.split("\n")]

    if p.returncode and not ignoreErrors:
        raise RuntimeError("Error executing command: %s (return code %d)" % (cmd, p.returncode))
    else:
        return p.returncode
