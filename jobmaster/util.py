#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import os
import logging
log = logging
import popen2

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


def logCall(cmd, ignoreErrors = False):
    log.debug("+ " + cmd)
    p = popen2.Popen4(cmd)
    if not ignoreErrors:
        code = p.wait()
        err = p.fromchild.read()
        [log.debug("++ " + errLine) for errLine in err.split("\n")]
        raise RuntimeError("Error executing command: %s (return code %d)" % (cmd, code))
    else:
        p.wait()
        err = p.fromchild.read()
        [log.debug("++ " + errLine) for errLine in err.split("\n")]
