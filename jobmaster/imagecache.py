#!/usr/bin/python
#
# Copyright (c) 2004-2006 rPath, Inc.
#
# All Rights Reserved
#

import errno
import logging
import os, sys
import math
import md5
import re
import urllib
import shutil
import tempfile
import time

from conary import conarycfg
from conary import conaryclient
from conary.conaryclient import cmdline
from conary.lib import util

from jobmaster.util import logCall

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
        f.seek(size - 1)
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
    createFile(os.path.join(d, 'etc', 'fstab'),
               '\n'.join(('LABEL=/ / ext3 defaults 1 1',
                          'none /dev/pts devpts gid=5,mode=620 0 0',
                          'none /dev/shm tmpfs defaults 0 0',
                          'none /proc proc defaults 0 0',
                          'none /sys sysfs defaults 0 0',
                          '/var/swap swap swap defaults 0 0\n')))
    #create a swap file
    mkBlankFile(os.path.join(d, 'var', 'swap'), SWAP_SIZE, sparse = False)
    logCall('/sbin/mkswap %s >/dev/null 2>&1' % \
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
                            'ifcfg-eth0.template'),
               '\n'.join(('DEVICE=eth0',
                          'BOOTPROTO=static',
                          'IPADDR=%(ipaddr)s',
                          'GATEWAY=%(masterip)s',
                          'ONBOOT=yes',
                          'TYPE=Ethernet\n')))
    createFile(os.path.join(d, 'etc', 'sysconfig', 'keyboard'),
               '\n'.join(('KEYBOARDTYPE="pc"',
                          'KEYTABLE="us"\n')))
    writeConaryRc(d)


def getRunningKernel():
    p = os.popen('uname -r')
    data = p.read()
    p.close()

    m = re.match('[\d.-]*', data)
    ver = m.group()[:-1]
    p = os.popen('conary q kernel:runtime --full-versions --flavors | grep %s' % ver)
    return p.read().strip()

class ImageCache(object):
    def __init__(self, cachePath, masterCfg):
        self.cachePath = cachePath
        self.masterCfg = masterCfg
        util.mkdirChain(self.cachePath)

        self.conarycfg = conarycfg.ConaryConfiguration(True)

        self.cc = conaryclient.ConaryClient(self.conarycfg)
        self.nc = self.cc.getRepos()

        self.tmpPath = os.path.join(os.path.split(cachePath)[0], 'tmp')

    def startBuildingImage(self, hash, output = True):
        # this function is designed to block if an image is being built already
        lockPath = os.path.join(self.cachePath, hash + '.lock')
        done = False
        while not done:
            while os.path.exists(lockPath):
                if output:
                    logging.info('Waiting for building lock: %s' % lockPath)
                    output = False
                time.sleep(1)
            try:
                os.mkdir(lockPath)
                logging.info('Acquired slave building lock: %s' % lockPath)
                done = True
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise

    def stopBuildingImage(self, hash):
        lockPath = os.path.join(self.cachePath, hash + '.lock')
        logging.info('Releasing slave building lock: %s' % lockPath)
        os.rmdir(lockPath)

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
        imageFile = os.path.join(self.cachePath, hash)
        if hash in os.listdir(self.cachePath):
            logging.info("Found image in cache for %s" % troveSpec)
            return imageFile
        else:
            logging.info("Image not cached, creating image for %s" % \
                    troveSpec)
            self.startBuildingImage(hash)
            try:
                if os.path.exists(imageFile):
                    return imageFile
                return self.makeImage(troveSpec, hash)
            finally:
                self.stopBuildingImage(hash)

    def makeImage(self, troveSpec, hash):
        n, v, f = cmdline.parseTroveSpec(troveSpec)
        NVF = self.nc.findTrove(None, (n, v, f), self.conarycfg.flavor)[0]

        trv = self.nc.getTrove(NVF[0], NVF[1], NVF[2], withFiles = False)

        size = trv.getSize()
        size = roundUpSize(size)

        fd, fn = tempfile.mkstemp(dir = self.tmpPath)
        os.close(fd)

        fd, tagScript = tempfile.mkstemp(prefix = "tagscript",
                                         dir = self.tmpPath)
        os.close(fd)

        fd, kernelTagScript = tempfile.mkstemp(prefix = "kernel-tagscript",
                                               dir = self.tmpPath)
        os.close(fd)

        mntDir = tempfile.mkdtemp(dir = self.tmpPath)
        try:
            mkBlankFile(fn, size)

            # run mke2fs on blank image
            logCall('mkfs -t ext2 -F -L / %s %d' % \
                          (fn, size / 1024))
            logCall('tune2fs -m 0 -i 0 -c 0 %s' % fn)

            logCall('mount -o loop %s %s' % (fn, mntDir))

            createTemporaryRoot(mntDir)
            logCall('mount -t proc none %s' % os.path.join(mntDir, 'proc'))
            logCall('mount -t sysfs none %s' % os.path.join(mntDir, 'sys'))

            conaryProxy = (self.masterCfg.conaryProxy or "")
            logCall(("conary update '%s' --root %s --replace-files " \
                           "--tag-script=%s" %s) % \
                          (troveSpec, mntDir, tagScript, conaryProxy))

            shutil.move(tagScript, os.path.join(mntDir, 'root',
                'conary-tag-script.in'))

            kernelSpec = getRunningKernel()
            logCall(("conary update '%s' --root %s --resolve " \
                       "--keep-required --tag-script=%s %s" ) \
                          % (kernelSpec, mntDir, kernelTagScript, conaryProxy))

            shutil.move(kernelTagScript, os.path.join(mntDir, 'root',
                                        'conary-tag-script-kernel'))
            fsOddsNEnds(mntDir)

            outScript = os.path.join(mntDir, 'root', 'conary-tag-script')
            inScript = outScript + '.in'
            logCall('echo "/sbin/ldconfig" > %s; cat %s | sed "s|/sbin/ldconfig||g" | grep -vx "" >> %s' % (outScript, inScript, outScript))
            os.unlink(os.path.join(mntDir, 'root', 'conary-tag-script.in'))

            for tagScript in ('conary-tag-script', 'conary-tag-script-kernel'):
                tagPath = util.joinPaths(os.path.sep, 'root', tagScript)
                if os.path.exists(util.joinPaths(mntDir, tagPath)):
                    util.execute("chroot %s bash -c 'sh -x %s > %s 2>&1'" % \
                                     (mntDir, tagPath, tagPath + '.output'))

            # FIXME: long term this code would be needed for remote slaves
            #os.system("conary update --sync-to-parents kernel:runtime "
            #          "--root %s" % mntDir)

            # the preload wrapper isn't working yet, work around until we know why
            #safeEnv = {"LD_PRELOAD": "/usr/lib/jobmaster/chrootsafe_wrapper.so"}

            # authconfig can whack the domainname in certain circumstances
            oldDomainname = os.popen('domainname').read().strip() # save old domainname
            logCall("chroot %s /usr/sbin/authconfig --kickstart --enablemd5 --enableshadow --disablecache" % mntDir)
            logCall("domainname %s" % oldDomainname) # restore it

            logCall("chroot %s /usr/sbin/usermod -p '' root" % mntDir)
            logCall('grubby --remove-kernel=/boot/vmlinuz-template --config-file=%s' % os.path.join(mntDir, 'boot', 'grub', 'grub.conf'))
        finally:
            logCall('umount %s' % os.path.join(mntDir, 'proc'))
            logCall('umount %s' % os.path.join(mntDir, 'sys'))
            logCall('sync')
            logCall('umount %s' % mntDir)
            logCall('sync')
            util.rmtree(mntDir, ignore_errors = True)
        shutil.move(fn, os.path.join(self.cachePath, hash))
        return os.path.join(self.cachePath, hash)
