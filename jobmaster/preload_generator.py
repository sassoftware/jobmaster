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

    if len(args) != 2:
        sys.exit("usage: %s <basename> <trovespec>" % sys.argv[0])
    baseName, troveSpec = args

    jsRootDir = None
    if os.path.isdir(troveSpec):
        sysRootDir = troveSpec
        targets = ['srv/rbuilder/repos']
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

        except:
            if jsRootDir:
                rmtree(sysRootDir)
                rmtree(jsRootDir)
            raise

    try:
        log.info("Creating preload tarball")
        proc = subprocess.Popen("/bin/tar -cC '%s' %s "
                "--exclude var/lib/conarydb/rollbacks/\\* "
                "--exclude var/log/conary "
                "| /bin/gzip -9c" % (sysRootDir, ' '.join(targets)),
                shell=True, stdout=subprocess.PIPE)
        manifest = open(baseName, 'w')
        try:
            copyChunks(proc.stdout, baseName, manifest)
        except:
            os.kill(proc.pid, signal.SIGTERM)
            proc.wait()
            raise
        proc.wait()
    finally:
        if jsRootDir:
            rmtree(jsRootDir)
            rmtree(sysRootDir)


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
