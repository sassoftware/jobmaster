#!/usr/bin/python

#
# Copyright (c) 2006 rPath, Inc.
#
# All rights reserved
#

import socket
import os, sys
import fcntl

sequencePath = os.path.join(os.path.sep, 'var', 'run', 'xenmac.seq')

MAX_SEQ = 1

# ensure the value of MAX_SEQ is clamped. it's a divisor so it cannot be zero.
# it represents the limit of 1 octet so it cannot be greater than 256
def setMaxSeq(x):
    sys.modules[__name__].MAX_SEQ = min(max(1, x), 256)

class SuperUser(Exception):
    def __str__(self):
        return "You must be superuser to use this function"

class NetworkInterface(Exception):
    def __str__(self):
        return "IP address cannot be determined. There must be a default " \
               "route associated with an active interface."

def readPipe(command):
    stderr = os.dup(sys.stderr.fileno())
    stdout = os.dup(sys.stdout.fileno())
    fd = os.open(os.devnull, os.W_OK)
    os.dup2(fd, sys.stdout.fileno())
    os.dup2(fd, sys.stderr.fileno())
    os.close(fd)
    try:
        f = os.popen(command)
        return f.read()
    finally:
        f.close()
        os.dup2(stderr, sys.stderr.fileno())
        os.dup2(stdout, sys.stdout.fileno())

def checkMac(mac):
    data = readPipe('xm list --long')
    return mac not in data

def genMac():
    if os.geteuid():
        raise SuperUser
    # obtain full IP address
    IP = readPipe('/sbin/ifconfig `/sbin/route | grep default | sed "s/.* //"` | ' \
                 'grep "inet addr" | sed "s/.*addr://" | sed "s/ .*//"')
    IP = IP.strip()

    if not IP:
        raise NetworkInterface

    # strip the first two octets and format as hex
    IP = [hex(int(x))[2:] for x in IP.split('.')][2:]
    # ensure the octets are two digits long
    IP = [(len(x) == 1 and '0' + x or x) for x in IP]
    # format the IP portion.
    IP = ':'.join(IP)

    xenPrefix = '00:16:3e'
    f = open(sequencePath, 'a+')
    try:
        fcntl.lockf(f.fileno(), fcntl.LOCK_EX)
        f.seek(0)
        oneUp = f.read()
        if not len(oneUp):
            oneUp = 0
        else:
            oneUp = (int(oneUp) + 1) % MAX_SEQ
        done = False
        while not done:
            f.seek(0)
            f.truncate()
            f.write(str(oneUp))
            strOneUp = hex(oneUp)[2:]
            if len(strOneUp) == 1:
                strOneUp = '0' + strOneUp
            mac = ':'.join((xenPrefix, IP, strOneUp))
            done = checkMac(mac)
            oneUp = (int(oneUp) + 1) % MAX_SEQ
        return mac
    finally:
        f.close()

if __name__ == '__main__':
    try:
        print genMac()
    except (SuperUser, NetworkInterface), e:
        print e
