#!/usr/bin/python
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All Rights Reserved
#

import os, sys
import math
import md5
import re
import urllib
import tempfile

from conary import conarycfg
from conary import conaryclient
from conary.conaryclient import cmdline
from conary.lib import util

SWAP_SIZE = 268435456 # 256 MB in bytes
TAGSCRIPT_GROWTH = 20971520 # 20MB in bytes
CYLINDERSIZE = 516096
SECTORS         = 63
HEADS           = 16

###########
# all image building related functions are not class members to
# enforce prevention of side effects
###########

def md5sum(s):
    m = md5.new()
    m.update(s)
    return m.hexdigest()

def roundUpSize(size):
    # 13% accounts for reserved block and inode consumption
    size = int(math.ceil((size + TAGSCRIPT_GROWTH + SWAP_SIZE) / 0.87))
    # now round up to next cylinder size
    return size + ((CYLINDERSIZE - (size % CYLINDERSIZE)) % CYLINDERSIZE)

def createDir(d):
    if not os.path.exists(d):
        createDir(os.path.split(d)[0])
        os.mkdir(d)

def mkBlankFile(fn, size, sparse = True):
    createDir(os.path.split(fn)[0])
    f = open(fn, 'w')
    if sparse:
        f.seek(SWAP_SIZE - 1)
        f.write(chr(0))
    else:
        for i in range(size / 512):
            f.write(512 * chr(0))
        f.write((size % 512) * chr(0))
    f.close()

def createFile(fn, contents):
    createDir(os.path.split(fn)[0])
    f = open(fn, 'w')
    f.write(contents)
    f.close()

def writeConaryRc(d, mirrorUrl = ''):
        # write the conaryrc file
        conaryrcFile = open(os.path.join(d, 'etc', 'conaryrc'), "w")
        if mirrorUrl:
            type, url = urllib.splittype(mirrorUrl)
            relativeLink = ''
            if not type:
                type = 'http'
            if not url.startswith('//'):
                url = '//' + url
            if not urllib.splithost(url)[1]:
                relativeLink = '/conaryrc'
            mirrorUrl = type + ':' + url + relativeLink
            print >> conaryrcFile, 'includeConfigFile ' + mirrorUrl
        print >> conaryrcFile, "pinTroves kernel.*"
        print >> conaryrcFile, "includeConfigFile /etc/conary/config.d/*"
        conaryrcFile.close()

def createTemporaryRoot(fakeRoot):
    for d in ('etc', 'etc/sysconfig', 'etc/sysconfig/network-scripts',
              'boot/grub', 'tmp', 'proc', 'sys', 'root', 'var'):
        util.mkdirChain(os.path.join(fakeRoot, d))

def fsOddsNEnds(d):
    createTemporaryRoot(d)
    createFile(os.path.join(d, 'etc', 'fstab'),
               '\n'.join(('LABEL=/ / ext3 defaults 1 1',
                          'none /dev/pts devpts gid=5,mode=620 0 0',
                          'none /dev/shm tmpfs defaults 0 0',
                          'none /proc proc defaults 0 0',
                          'none /sys sysfs defaults 0 0',
                          '/var/swap swap swap defaults 0 0\n')))
    #create a swap file
    mkBlankFile(os.path.join(d, 'var', 'swap'), SWAP_SIZE, sparse = False)
    os.system('/sbin/mkswap %s >/dev/null 2>&1' % \
                  os.path.join(d, 'var', 'swap'))

    util.copytree(os.path.join(d, 'usr', 'share', 'grub', '*', '*'), \
                      os.path.join(d, 'boot', 'grub'))

    #copy the files needed by grub and set up the links
    grubContents = \
        '\n'.join(('default=0',
                   'timeout=0',
                   'title rBuilder Job Slave (template)',
                   '    root (hd0,0)',
                   '    kernel /boot/vmlinuz-template ro root=LABEL=/ quiet',
                   '    initrd /boot/initrd-template.img\n'))
    createFile(os.path.join(d, 'boot', 'grub', 'grub.conf'), grubContents)
    os.symlink('grub.conf', os.path.join(d, 'boot', 'grub', 'menu.1st'))

    #Add the other miscellaneous files needed
    createFile(os.path.join(d, 'etc', 'hosts'),
               '127.0.0.1       localhost.localdomain   localhost\n')
    createFile(os.path.join(d, 'etc', 'sysconfig', 'network'),
               '\n'.join(('NETWORKING=yes',
                          'HOSTNAME=localhost.localdomain\n')))
    createFile(os.path.join(d, 'etc', 'sysconfig', 'network-scripts',
                            'ifcfg-eth0'),
               '\n'.join(('DEVICE=eth0',
                          'BOOTPROTO=dhcp',
                          'ONBOOT=yes',
                          'TYPE=Ethernet\n')))
    createFile(os.path.join(d, 'etc', 'sysconfig', 'keyboard'),
               '\n'.join(('KEYBOARDTYPE="pc"',
                          'KEYTABLE="us"\n')))

def getRunningKernel():
    p = os.popen('uname -r')
    data = p.read()
    p.close()

    m = re.match('[\d.-]*', data)
    ver = m.group()[:-1]
    p = os.popen('conary q kernel --full-versions --flavors | grep %s' % ver)
    return p.read()

class ImageCache(object):
    def __init__(self, cachePath):
        self.cachePath = cachePath
        util.mkdirChain(self.cachePath)

        self.conarycfg = conarycfg.ConaryConfiguration(True)

        self.cc = conaryclient.ConaryClient(self.conarycfg)
        self.nc = self.cc.getRepos()

    def deleteAllImages(self):
        # this is for clearing the cache, eg. needed if entitlements changed
        for image in os.listdir(self.cachePath):
            os.unlink(os.path.join(self.cachePath, image))

    def haveImage(self, troveSpec):
        return os.path.exists(self.imagePath(troveSpec))

    def imagePath(self, troveSpec):
        hash = md5sum(troveSpec)
        return os.path.join(self.cachePath, hash)

    def getImage(self, troveSpec):
        hash = md5sum(troveSpec)
        if hash in os.listdir(self.cachePath):
            return os.path.join(self.cachePath, hash)
        else:
            return self.makeImage(troveSpec, hash)

    def makeImage(self, troveSpec, hash):
        n, v, f = cmdline.parseTroveSpec(troveSpec)
        NVF = self.nc.findTrove(None, (n, v, f), self.conarycfg.flavor)[0]

        trv = self.nc.getTrove(NVF[0], NVF[1], NVF[2], withFiles = False)

        size = trv.getSize()
        size = roundUpSize(size)

        fd, fn = tempfile.mkstemp()
        os.close(fd)

        try:
            mkBlankFile(fn, size)

            # run mke2fs on blank image
            os.system('mkfs -t ext2 -F -L / %s %d' % \
                          (fn, size / 1024))
            os.system('tune2fs -i 0 -c 0 %s' % fn)

            mntDir = tempfile.mkdtemp()
            os.system('mount -o loop %s %s' % (fn, mntDir))

            # lay fs odds and ends *before* group update for tag scripts
            fsOddsNEnds(mntDir)

            writeConaryRc(mntDir)
            os.system('mount -t proc none %s' % os.path.join(mntDir, 'proc'))
            os.system('mount -t sysfs none %s' % os.path.join(mntDir, 'sys'))

            # FIXME: noted complaints about needing an installLabelPath
            os.system("conary update '%s' --root %s --replace-files" % \
                          (troveSpec, mntDir))

            p = os.popen('conary q mkinitrd --full-versions --flavors')
            mkinitrdVer = p.read().strip()
            p.close()

            os.system("conary update '%s' --root %s" % (mkinitrdVer, mntDir))

            kernelSpec = getKernelVersion()
            os.system("conary update '%s' --root %s" % (kernelSpec, mntDir))

            # FIXME: long term this code would be needed for remote slaves
            #os.system("conary update --sync-to-parents kernel:runtime "
            #          "--root %s" % mntDir)

            os.system("chroot %s /usr/bin/authconfig --kickstart --enablemd5 --enableshadow --disablecache" % mntDir)
            os.system("chroot %s /usr/sbin/usermod -p '' root" % mntDir)
            os.system('grubby --remove-kernel=/boot/vmlinuz-template --config-file=%s' % os.path.join(mntDir, 'boot', 'grub', 'grub.conf'))
        finally:
            os.system('umount %s' % os.path.join(mntDir, 'proc'))
            os.system('umount %s' % os.path.join(mntDir, 'sys'))
            os.system('sync')
            os.system('umount %s' % mntDir)
        os.rename(fn, os.path.join(self.cachePath, hash))
        return os.path.join(self.cachePath, hash)

if __name__ == '__main__':
    imgCache = ImageCache()
    print imgCache.getImage(sys.argv[1])
