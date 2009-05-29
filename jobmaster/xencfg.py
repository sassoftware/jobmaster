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
    def __init__(self, imgPath, cfg):
        self.cfg = copy.deepcopy(cfg)
        self.cfg.setdefault('memory', 64)

        if 'kernel' not in self.cfg:
            self.cfg.setdefault('bootloader', '/usr/bin/pygrub')

        # genMac has an effect that is global to the entire system.
        if 'vif' not in self.cfg or not self.cfg['vif'] \
               or self.cfg['vif'] == ['']:
            mac = xenmac.genMac()
            self.ip = xenip.genIP()
            self.cfg['vif'] = [ 'ip=%s, mac=%s' % (self.ip, mac) ]

        self.cfg.setdefault('name', 'slave%s' % \
                            self.cfg['vif'][0].split(':')[-1])

        diskPath = os.path.join(imgPath, self.cfg['name'])
        self.cfg.setdefault('disk', [
                'phy:%s,xvda1,w' % (diskPath + '-base',),
                'phy:%s,xvda2,w' % (diskPath + '-scratch',),
                'phy:%s,sdb,w' % (diskPath + '-swap',),
                ])
        self.cfg.setdefault('root', '/dev/xvda1 ro')

    def write(self, f = sys.stdout):
        for key, val in self.cfg.iteritems():
            if isinstance(val, list):
                f.write("%s = %s\n" % (key, str([str(x) for x in val])))
            else:
                f.write("%s = \"%s\"\n" % (key, val))
