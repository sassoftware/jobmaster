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
        rList, junk, junk = select.select([p.stdout, p.stderr], [], [])
        for rdPipe in rList:
            action = (rdPipe is p.stdout) and log.info or log.debug
            msg = rdPipe.readline().strip()
            if msg:
                action("++ " + msg)

    stdout, stderr = p.communicate()
    [log.info("++ " + outLine) for outLine in stdout.splitlines()]
    [log.debug("++ " + errLine) for errLine in stderr.splitlines()]
    if p.returncode and not ignoreErrors:
        raise RuntimeError("Error executing command: %s (return code %d)" % (cmd, p.returncode))
    else:
        return p.returncode

def getIP():
    p = os.popen("""/sbin/ifconfig `/sbin/route | grep "^default" | sed "s/.* //"` | grep "inet addr" | awk -F: '{print $2}' | sed 's/ .*//'""")
    data = p.read().strip()
    p.close()
    return data

