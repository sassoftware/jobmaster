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
ipSequencePath = os.path.join(os.path.sep, 'var', 'run', 'xenip.seq')

MAX_SEQ = 256

class SuperUser(Exception):
    def __str__(self):
        return "You must be superuser to use this function"

class NetworkInterface(Exception):
    def __str__(self):
        return "IP address cannot be determined. There must be a default " \
               "route associated with an active interface."

class NoMACAddressAvailable(Exception):
    def __str__(self):
        return "A MAC address for the upcoming slave could not be obtained."

def readPipe(command):
    f = os.popen(command)
    return f.read()

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

        tries = 0
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
            tries += 1
            if tries > MAX_SEQ:
                raise NoMACAddressAvailable
        return mac
    finally:
        f.close()
