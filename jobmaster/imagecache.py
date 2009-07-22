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
import signal
import shutil
import tempfile
import time
import traceback

from conary import callbacks
from conary import conarycfg
from conary import conaryclient
from conary.conaryclient import cmdline
from conary.lib import util

from jobmaster.util import AtomicFile, logCall


TAGSCRIPT_GROWTH = 80 * 1048576 # 80 MiB

CYLINDERSIZE    = 516096
SECTORS         = 63
HEADS           = 16


# Increment this if the image generation process changes, so that the hash
# will change and old cached images will be discarded.
IMAGE_SERIAL = 3


def roundUpSize(size):
    # 13% accounts for reserved block and inode consumption
    size = int(math.ceil((size + TAGSCRIPT_GROWTH) / 0.87))
    # now round up to next cylinder size
    return size + ((CYLINDERSIZE - (size % CYLINDERSIZE)) % CYLINDERSIZE)


def mkBlankFile(fn, size, sparse = True):
    util.mkdirChain(os.path.dirname(fn))
    f = open(fn, 'w')
    if sparse:
        f.seek(size - 1)
        f.write(chr(0))
    else:
        for i in range(size / 512):
            f.write(512 * chr(0))
        f.write((size % 512) * chr(0))


def createFile(root, path, contents):
    fn = os.path.join(root, path)
    util.mkdirChain(os.path.dirname(fn))
    open(fn, 'w').write(contents)


def appendFile(root, path, contents):
    fn = os.path.join(root, path)
    util.mkdirChain(os.path.dirname(fn))
    open(fn, 'a').write(contents)


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

def preScript(d):
    # NB: /tmp is mounted by the jobslave initscript, so the scratch disk
    # will always be /dev/xvda2 .
    createFile(d, 'etc/fstab',
               '\n'.join(('/dev/xvda1 / ext3 defaults 1 1',
                          'none /dev/pts devpts gid=5,mode=620 0 0',
                          'none /dev/shm tmpfs defaults 0 0',
                          'none /proc proc defaults 0 0',
                          'none /sys sysfs defaults 0 0',
                          '')))

    util.copytree(os.path.join(d, 'usr', 'share', 'grub', '*', '*'), \
                      os.path.join(d, 'boot', 'grub'))

    #copy the files needed by grub and set up the links
    createFile(d, 'boot/grub/grub.conf',
        '\n'.join(('default=0',
                   'timeout=0',
                   'title rBuilder Job Slave (template)',
                   '    root (hd0,0)',
                   '    kernel /boot/vmlinuz-template ro root=LABEL=/ quiet',
                   '    initrd /boot/initrd-template.img\n')))
    os.symlink('grub.conf', os.path.join(d, 'boot', 'grub', 'menu.1st'))

    #Add the other miscellaneous files needed
    createFile(d, 'etc/hosts',
               '127.0.0.1       localhost.localdomain   localhost\n')
    createFile(d, 'etc/sysconfig/network',
               '\n'.join(('NETWORKING=yes',
                          'HOSTNAME=localhost.localdomain\n')))
    createFile(d, 'etc/sysconfig/network-scripts/ifcfg-eth0.template',
               '\n'.join(('DEVICE=eth0',
                          'BOOTPROTO=static',
                          'IPADDR=%(ipaddr)s',
                          'GATEWAY=%(masterip)s',
                          'ONBOOT=yes',
                          'TYPE=Ethernet\n')))
    createFile(d, 'etc/sysconfig/keyboard',
               '\n'.join(('KEYBOARDTYPE="pc"',
                          'KEYTABLE="us"\n')))
    writeConaryRc(d)

    # Set up a TTY on xvc0 as a debugging aid
    appendFile(d, 'etc/inittab', 'xvc:2345:respawn:/sbin/mingetty xvc0\n')
    appendFile(d, 'etc/securetty', 'xvc0\n')

    # Turn TX checksum offloading off (for TCP & UDP)
    appendFile(d, 'etc/rc.local', 'ethtool -K eth0 tx off')

    # symlink /var/tmp -> /tmp to avoid running out of space on / (RBL-4202)
    util.mkdirChain(os.path.join(d, 'var'))
    util.rmtree(os.path.join(d, 'var/tmp'), ignore_errors=True)
    os.symlink('../tmp', os.path.join(d, 'var/tmp'))


def postScript(d):
    # authconfig can whack the domainname in certain circumstances
    oldDomainname = os.popen('domainname').read().strip() # save old domainname
    logCall("chroot %s /usr/sbin/authconfig --kickstart --enablemd5 --enableshadow --disablecache" % d)
    # Only restore the domain if it was set in the first place.
    if oldDomainname != "" and oldDomainname != "(none)":
        logCall("domainname %s" % oldDomainname)

    logCall("chroot %s /usr/sbin/usermod -p '' root" % d)
    logCall('grubby --remove-kernel=/boot/vmlinuz-template --config-file=%s' % os.path.join(d, 'boot', 'grub', 'grub.conf'))

    # Add swap to fstab after tag scripts, as libatamigrate seems to mess it
    # up if we do it earlier.
    appendFile(d, 'etc/fstab', '/dev/sdb swap swap defaults 0 0\n')


def signalHandler(*args, **kwargs):
    # change signals into exceptions
    raise RuntimeError('process killed')

class ImageCache(object):
    def __init__(self, cachePath, masterCfg):
        self.cachePath = cachePath
        self.masterCfg = masterCfg
        util.mkdirChain(self.cachePath)

        self.tmpPath = os.path.join(os.path.split(cachePath)[0], 'tmp')

    def startBuildingImage(self, hash, output=True, statusHook=None):
        # this function is designed to block if an image is being built already
        lockPath = os.path.join(self.cachePath, hash + '.lock')
        done = False
        lastStatus = None
        statusPath = os.path.join(self.cachePath, hash + '.status')
        while not done:
            while os.path.exists(lockPath):
                if output:
                    logging.info('Waiting for building lock: %s' % lockPath)
                    output = False
                if statusHook and os.path.exists(statusPath):
                    status = open(statusPath).read().strip()
                    if status != lastStatus:
                        statusHook(status)
                        lastStatus = status
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

    def haveImage(self, troveSpec, kernelData):
        return os.path.exists(self.imagePath(troveSpec, kernelData))

    def imageHash(self, troveSpec, kernelData):
        ctx = md5.new()
        ctx.update('%d\0%s\0' % (IMAGE_SERIAL, troveSpec))
        ctx.update('%s=%s[%s]\0' % kernelData['trove'])
        return ctx.hexdigest()

    def imagePath(self, troveSpec, kernelData):
        imageFile = os.path.join(self.cachePath,
            self.imageHash(troveSpec, kernelData))
        return imageFile

    def getImage(self, troveSpec, kernelData, debugMode=False,
            statusHook=None):
        hash = self.imageHash(troveSpec, kernelData)
        imageFile = self.imagePath(troveSpec, kernelData)
        if os.path.exists(imageFile):
            logging.info("Found image in cache for %s" % troveSpec)
            return imageFile
        else:
            logging.info("Image not cached, creating image for %s" % \
                    troveSpec)
            signal.signal(signal.SIGTERM, signalHandler)
            signal.signal(signal.SIGINT, signalHandler)
            self.startBuildingImage(hash, statusHook=statusHook)
            try:
                if os.path.exists(imageFile):
                    return imageFile
                return self.makeImage(troveSpec, kernelData, hash, statusHook)
            finally:
                self.stopBuildingImage(hash)

    def makeImage(self, troveSpec, kernelData, hash, statusHook=None):
        logging.info('Building image')

        statusPath = os.path.join(self.cachePath, hash + '.status')
        def sendStatus(msg):
            if statusHook:
                statusHook(msg)
            fObj = AtomicFile(statusPath)
            fObj.write(msg)
            fObj.commit()

        ccfg = conarycfg.ConaryConfiguration(True)
        cc = conaryclient.ConaryClient(ccfg)
        nc = cc.getRepos()

        # Look up which troves we'll be installing
        spec_n, spec_v, spec_f = cmdline.parseTroveSpec(troveSpec)
        n, v, f = nc.findTrove(None, (spec_n, spec_v, spec_f), ccfg.flavor)[0]
        trv = nc.getTrove(n, v, f, withFiles = False)
        size = roundUpSize(trv.getSize())

        k_n, k_v, k_f = kernelData['trove']

        # Create temporary paths
        #  jobslave root
        fd, filesystem = tempfile.mkstemp(dir = self.tmpPath)
        os.close(fd)
        #  group & kernel tagscripts
        fd, tagScript = tempfile.mkstemp(prefix = "tagscript",
                                         dir = self.tmpPath)
        os.close(fd)
        #  mount point
        mntDir = tempfile.mkdtemp(dir = self.tmpPath)
        mounted = []

        # XXX: This can probably go away if modprobe is loaded on startup
        logCall('modprobe loop')

        client = job = None
        try:
            logging.info('Creating filesystem')
            mkBlankFile(filesystem, size)

            # run mke2fs on blank image
            logCall('mkfs -t ext2 -F -q -L / %s %d' % \
                          (filesystem, size / 1024))
            logCall('tune2fs -m 0 -i 0 -c 0 %s >/dev/null' % filesystem)

            logCall('mount -o loop %s %s' % (filesystem, mntDir))
            mounted.append(mntDir)

            createTemporaryRoot(mntDir)
            logCall('mount -t proc none %s' % os.path.join(mntDir, 'proc'))
            mounted.append(os.path.join(mntDir, 'proc'))
            logCall('mount -t sysfs none %s' % os.path.join(mntDir, 'sys'))
            mounted.append(os.path.join(mntDir, 'sys'))

            # Prepare conary client
            cfg = conarycfg.ConaryConfiguration(True)
            if self.masterCfg.conaryProxy:
                cfg.conaryProxy['http']  = self.masterCfg.conaryProxy
                cfg.conaryProxy['https'] = self.masterCfg.conaryProxy
            cfg.root = mntDir
            client = conaryclient.ConaryClient(cfg)
            client.setUpdateCallback(UpdateCallback(sendStatus))

            # Install jobslave root and kernel
            job = client.newUpdateJob()
            logging.info('Preparing update job')
            sendStatus("preparing to install")
            client.prepareUpdateJob(job, (
                (n,   (None, None), (v,   f),    True), # root
                (k_n, (None, None), (k_v, k_f),  True), # kernel
                ))
            logging.info('Applying update job')
            client.applyUpdateJob(job, tagScript=tagScript)

            # Create various filesystem pieces
            logging.info('Preparing filesystem')
            sendStatus("finalizing")
            preScript(mntDir)

            # Assemble tag script and run it
            outScript = os.path.join(mntDir, 'root', 'conary-tag-script')
            outScriptInRoot = os.path.join('', 'root', 'conary-tag-script')
            outScriptOutput = os.path.join('', 'root',
                'conary-tag-script.output')

            tagScriptFile = open(tagScript, 'r')
            outScriptFile = open(outScript, 'w')
            outScriptFile.write('/sbin/ldconfig\n')
            for line in tagScriptFile:
                if line.startswith('/sbin/ldconfig'):
                    continue
                outScriptFile.write(line)
            tagScriptFile.close()
            outScriptFile.close()

            os.unlink(tagScript)

            logging.info('Running tag scripts')
            util.execute("chroot %s bash -c 'sh -x %s > %s 2>&1'" % (
                    mntDir, outScriptInRoot, outScriptOutput))

            postScript(mntDir)

            logging.info('Image built')
        finally:
            try:
                if client:
                    client.close()
                    del job
                for path in reversed(mounted):
                    logCall('umount %s' % (path,), ignoreErrors=True)
                os.rmdir(mntDir)
            except:
                logging.error('Unhandled exception while finalizing '
                    'jobslave:\n' + traceback.format_exc())
        shutil.move(filesystem, os.path.join(self.cachePath, hash))
        os.unlink(statusPath)
        return os.path.join(self.cachePath, hash)

class UpdateCallback(callbacks.UpdateCallback):
    def __init__(self, status):
        callbacks.UpdateCallback.__init__(self)
        self.status = status

    def eatMe(self, *P, **K):
        pass

    tagHandlerOutput = troveScriptOutput = troveScriptFailure = eatMe

    def setUpdateHunk(self, hunk, total):
        logging.info('Applying %d of %d', hunk, total)
        percent = 100 * (hunk - 1) / total
        self.status("installing (%02d%%)" % (percent,))
