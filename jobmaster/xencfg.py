#!/usr/bin/python

#
# Copyright (c) 2006 rPath, Inc.
#
# All rights reserved
#

import xenmac, xenip
import os
import copy
import sys

class KernelMissing(Exception):
    def __str__(self):
        return "No suitable kernel found"

class UnrecognizedImage(Exception):
    def __str__(self):
        return self.msg
    def __init__(self, msg = 'Unrecognized Image'):
        self.msg = msg

class XenCfg(object):
    def __init__(self, imgPath, cfg = {}):
        self.cfg = copy.deepcopy(cfg)
        self.cfg.setdefault('memory', 64)

        if 'kernel' not in self.cfg:
            self.cfg.setdefault('bootloader', '/usr/bin/pygrub')

        # genMac has an effect that is global to the entire system.
        if 'vif' not in self.cfg or not self.cfg['vif'] \
               or self.cfg['vif'] == ['']:
            mac = xenmac.genMac()
            self.ip = xenip.getIP()
            self.cfg['vif'] = [ 'ip=%s, mac=%s' % (self.ip, mac) ]

        self.cfg.setdefault('name', 'slave%s' % \
                            self.cfg['vif'][0].split(':')[-1])

        p = os.popen('file -k %s' % imgPath)
        data = p.read()
        p.close()
        if 'filesystem' in data:
            suffix = '1'
        elif 'boot sector' in data:
            suffix = ''
        else:
            suffix = '1'
            #raise UnrecognizedImage('%s is not a recognized image' % imgPath)
        self.cfg.setdefault('disk', ['tap:aio:%s,xvda%s,w' % (imgPath, suffix)])

    def write(self, f = sys.stdout):
        for key, val in self.cfg.iteritems():
            if isinstance(val, list):
                f.write("%s = %s\n" % (key, str([str(x) for x in val])))
            else:
                f.write("%s = \"%s\"\n" % (key, val))

if __name__ == '__main__':
    def usage(out = sys.stderr):
        print >> out, "usage: %s /path/to/image" % os.path.basename(sys.argv[0])
        sys.exit(1)

    if len(sys.argv) == 1:
        usage()

    imgFile = sys.argv[1]

    import tempfile
    fd, fn = tempfile.mkstemp()
    os.close(fd)
    f = open(fn, 'w')
    try:
        cfg = XenCfg(imgFile)
        cfg.write(f)
    finally:
        f.close()
    print fn
