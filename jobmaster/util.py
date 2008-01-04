#
# Copyright (c) 2007 rPath, Inc.
#
# All rights reserved
#

import os
import logging
import select
import subprocess
log = logging

from conary import conarycfg, conaryclient

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

def logCall(cmd, ignoreErrors = False, logLevel=logging.DEBUG, **kwargs):
    log.log(logLevel, "+ " + cmd)
    p = subprocess.Popen(cmd, shell = True,
        stdout = subprocess.PIPE, stderr = subprocess.PIPE, **kwargs)
    while p.poll() is None:
        rList, junk, junk = select.select([p.stdout, p.stderr], [], [])
        for rdPipe in rList:
            action = (rdPipe is p.stdout) and log.info or log.debug
            msg = rdPipe.readline().strip()
            if msg:
                log.log(logLevel, "++ " + msg)

    stdout, stderr = p.communicate()
    [log.log(logLevel, "++ " + outLine) for outLine in
        stderr.splitlines() + stdout.splitlines()]
    if p.returncode and not ignoreErrors:
        raise RuntimeError("Error executing command: %s (return code %d)" % (cmd, p.returncode))
    else:
        return p.returncode

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

