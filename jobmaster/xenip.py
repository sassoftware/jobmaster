#!/usr/bin/python

#
# Copyright (c) 2006 rPath, Inc.
#
# All rights reserved
#

import socket
import os, sys
import fcntl

sequencePath = os.path.join(os.path.sep, 'var', 'run', 'xenip.seq')

START_SEQ = 1
MAX_SEQ = 254

# ensure the value of MAX_SEQ is clamped. it's a divisor so it cannot be zero.
# it represents the limit of 1 octet so it cannot be greater than 256
def setMaxSeq(x):
    sys.modules[__name__].MAX_SEQ = min(max(1, x), 256)

class SuperUser(Exception):
    def __str__(self):
        return "You must be superuser to use this function"
class NoIPAddressAvailable(Exception):
    def __str__(self):
        return "An IP address for the upcoming slave could not be obtained."

def checkIP(ip):
    p = os.popen("ping -q -c 3 -w 2 -i 0.3 %s" % ip)
    r = p.close()
    return bool(r)


def genIP(ipPrefix = '10.5.6'):
    if os.geteuid():
        raise SuperUser

    f = open(sequencePath, 'a+')
    try:
        fcntl.lockf(f.fileno(), fcntl.LOCK_EX)
        f.seek(0)
        oneUp = f.read()
        if not len(oneUp):
            oneUp = START_SEQ
        else:
            oneUp = (int(oneUp) + 1) % MAX_SEQ
        done = False

        tries = 0
        while not done:
            f.seek(0)
            f.truncate()
            f.write(str(oneUp))
            mac = ipPrefix + '.' + str(oneUp)
            done = checkIP(mac)
            oneUp = (int(oneUp) + 1) % MAX_SEQ

            tries += 1
            if tries > 15:
                raise NoIPAddressAvailable
        return mac
    finally:
        f.close()
