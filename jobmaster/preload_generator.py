#!/usr/bin/python
#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#


import logging
import os
import signal
import subprocess
import sys
import tarfile
import tempfile
from conary import conarycfg
from conary import conaryclient
from conary import trove
from conary.conaryclient.cmdline import parseTroveSpec
from conary.lib import digestlib
from conary.lib.util import copyfileobj, rmtree
from jobmaster import archiveroot
from jobmaster import buildroot
from jobmaster.util import setupLogging, specHash

log = logging.getLogger(__name__)


def main(args):
    setupLogging(logLevel=logging.DEBUG)

    if len(args) < 2:
        sys.exit("usage: %s <basename> <trovespec> | [<directory> <paths>+]"
                % sys.argv[0])
    baseName, troveSpec = args[:2]

    jsRootDir = None
    if os.path.isdir(troveSpec):
        sysRootDir = troveSpec

        manifest = open(baseName, 'w')
        splitter = Splitter(baseName, manifest)
        tar = tarfile.open(fileobj=splitter, mode='w|gz')
        for relpath in args[2:]:
            abspath = os.path.join(sysRootDir, relpath)
            info = tar.gettarinfo(abspath, relpath)
            info.uid = info.gid = 48
            info.uname = info.gname = 'apache'
            tar.addfile(info, open(abspath, 'rb'))
        tar.close()
        splitter.close()
        manifest.close()
    else:
        troveSpec = parseTroveSpec(troveSpec)

        cfg = conarycfg.ConaryConfiguration(True)
        cli = conaryclient.ConaryClient(cfg)
        repos = cli.getRepos()

        matches = repos.findTrove(None, troveSpec)
        troveTups = [ sorted(matches)[-1] ]

        buildTimes = [x() for x in repos.getTroveInfo(
            trove._TROVEINFO_TAG_BUILDTIME, troveTups)]
        hash = specHash(troveTups, buildTimes)

        jsRootDir = tempfile.mkdtemp()
        sysRootDir = tempfile.mkdtemp()
        try:
            buildroot.buildRoot(cfg, troveTups, jsRootDir)

            log.info("Creating root archive")
            relArchivePath = 'srv/rbuilder/jobmaster/archive/%s.tar.xz' % hash
            fullArchivePath = os.path.join(sysRootDir, relArchivePath)
            os.makedirs(os.path.dirname(fullArchivePath))
            archiveroot.archiveRoot(jsRootDir, fullArchivePath)

            targets = [relArchivePath]

            log.info("Creating preload tarball")
            manifest = open(baseName, 'w')
            splitter = Splitter(baseName, manifest)
            proc = subprocess.Popen("/bin/tar -cC '%s' %s "
                    "--exclude var/lib/conarydb/rollbacks/\\* "
                    "--exclude var/log/conary "
                    "| /bin/gzip -9c" % (sysRootDir, ' '.join(targets)),
                    shell=True, stdout=subprocess.PIPE)
            try:
                copyfileobj(proc.stdout, splitter)
            except:
                os.kill(proc.pid, signal.SIGTERM)
                proc.wait()
                raise
            proc.wait()
            splitter.close()
            manifest.close()
        finally:
            rmtree(jsRootDir)
            rmtree(sysRootDir)


class Splitter(object):
    def __init__(self, base, manifest, sizeLimit=10485760):
        self.base = base
        self.manifest = manifest
        self.sizeLimit = sizeLimit
        self.index = self.lastChunk = 0
        self.lastFile = self.lastName = self.lastDigest = None
        self.lastDigest = None

    def write(self, data):
        while data:
            self._startFile()

            toWrite = min(len(data), self.sizeLimit - self.lastChunk)
            self.lastFile.write(data[:toWrite])
            self.lastDigest.update(data[:toWrite])
            data = data[toWrite:]
            self.lastChunk += toWrite

            self._finishFile()

    def close(self):
        self._finishFile(True)

    def _startFile(self):
        if self.lastFile:
            return

        self.lastName = self.base + '.%02d' % self.index
        self.lastFile = open(self.lastName, 'wb')
        self.lastDigest = digestlib.sha1()
        self.lastChunk = 0
        self.index += 1

    def _finishFile(self, force=False):
        if not force and self.lastChunk < self.sizeLimit:
            return

        self.lastFile.close()
        self.lastFile = None
        digest = self.lastDigest.hexdigest()
        print >> self.manifest, self.lastName, self.lastChunk, 1, digest


def copyChunks(fromObj, base, manifest, sizeLimit=10485760):
    index = 0
    while True:
        name = '%s.%d' % (base, index)
        index += 1

        digest = digestlib.sha1()
        fObj = open(name + '.tmp', 'wb')
        size = copyfileobj(fromObj, fObj, digest=digest, sizeLimit=sizeLimit)
        fObj.close()
        if size:
            os.rename(name + '.tmp', name)
        else:
            os.unlink(name + '.tmp')
            break
        print >> manifest, name, size, 1, digest.hexdigest()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
