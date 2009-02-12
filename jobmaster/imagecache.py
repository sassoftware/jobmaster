#!/usr/bin/python
#
# Copyright (c) 2004-2009 rPath, Inc.
#
# All Rights Reserved
#

import copy
import hashlib
import logging
import os
import subprocess
import sys
import tempfile
from conary import callbacks
from conary import conarycfg
from conary import conaryclient
from conary import updatecmd
from conary.conaryclient import cmdline
from conary.lib import util
from jobmaster.util import setupLogging, createFile

log = logging.getLogger('jobmaster.imagecache')


SERIAL = 1


def main(args):
    if len(args) < 2:
        sys.exit('Usage: %s <path> <trovespec>+' % sys.argv[0])

    setupLogging()

    cachePath = args.pop(0)
    specTups = [cmdline.parseTroveSpec(x) for x in args]

    ccfg = conarycfg.ConaryConfiguration(True)
    ccfg.downloadFirst = False
    ccfg.initializeFlavors()
    cli = conaryclient.ConaryClient(ccfg)
    repos = cli.getRepos()

    results = repos.findTroves(None, specTups, ccfg.flavor)
    troveTups = sorted(max(x) for x in results.values())
    print 'In:', '%s=%s[%s]' % troveTups[0]
    for tup in troveTups[1:]:
        print '    %s=%s[%s]' % tup
    outPath = getImage(ccfg, cachePath, troveTups)


def getImage(ccfg, cachePath, troveTups):
    """
    Get the path to a LZMAball of the troves C{troveTups} from the
    cache at C{cachePath}, building it first if necessary using the
    configuration C{ccfg}.
    
    Verifies the SHA-512 digest before returning a tuple
    C{(path, metadata)} where C{metadata} is a dictionary of simple
    items stored alongside the image.
    """
    imageHash = specHash(troveTups)
    imagePath = os.path.abspath(os.path.join(cachePath,
        imageHash + '.tar.lzma'))
    metaPath = imagePath + '.metadata'

    if os.path.exists(imagePath) and os.path.exists(metaPath):
        metadata = dict(x.strip().split(' ', 1) for x in open(metaPath))

        log.info("Using jobslave image at path %s", imagePath)
        digest = copySHA512(open(imagePath))
        expected = metadata['sha512_digest']
        if digest.lower() == expected.lower():
            return imagePath, metadata
        log.warning("Digest mismatch on image; rebuilding.")

    metadata = buildRoot(ccfg, troveTups, imagePath)
    return imagePath, metadata


def buildRoot(ccfg, troveTups, destPath):
    """
    Build a LZMAball of the troves C{troveTups} using the configuration
    C{ccfg} and save the result at C{destPath} along with a SHA-512
    digest.
    """
    fsRoot = tempfile.mkdtemp(prefix='temproot-')

    rootCfg = copy.deepcopy(ccfg)
    rootCfg.root = fsRoot
    rootCfg.autoResolve = False

    rootClient = conaryclient.ConaryClient(rootCfg)
    try:
        os.mkdir(os.path.join(fsRoot, 'root'))

        log.info("Preparing update job")
        rootClient.setUpdateCallback(UpdateCallback())
        job = rootClient.newUpdateJob()
        jobTups = [(n, (None, None), (v, f), True) for (n, v, f) in troveTups]
        rootClient.prepareUpdateJob(job, jobTups)

        rootClient.applyUpdateJob(job,
                tagScript=os.path.join(fsRoot, 'root/conary-tag-script'))

        log.info("Running tag scripts")
        preTagScripts(fsRoot)
        util.execute("/usr/sbin/chroot '%s' bash -c '"
                "sh -x /root/conary-tag-script "
                ">/root/conary-tag-script.output 2>&1'" % (fsRoot,))
        postTagScripts(fsRoot)

        log.info("Compressing image")
        metadata = {'troves': '; '.join('%s=%s[%s]' % (n, v, f)
            for (n, _, (v, f), _) in sorted(job.getPrimaryJobs()))}
        metadata = compressRoot(fsRoot, destPath, metadata)

        log.info("Image written to %s", destPath)
    finally:
        rootClient.close()
        job = None
        util.rmtree(fsRoot)

    return metadata


def preTagScripts(fsRoot):
    """
    Prepare the image root for running tag scripts.
    """
    # Fix up rootdir permissions as tar actually restores them when
    # extracting.
    os.chmod(fsRoot, 0755)

    # Create system configuration files
    createFile(fsRoot, 'etc/fstab',
        """
        LABEL=jsroot    /           ext3    defaults        1 1
        LABEL=jsswap    swap        swap    defaults        0 0
        none            /dev/pts    devpts  gid=5,mode=620  0 0
        none            /dev/shm    tmpfs   defaults        0 0
        none            /proc       proc    defaults        0 0
        none            /sys        sysfs   defaults        0 0
        """, 0644)


def postTagScripts(fsRoot):
    """
    Clean up after running tag scripts.
    """


def getTreeSize(path):
    """
    Return the on-disk size in bytes of a tree of files at C{path}.
    """
    proc = subprocess.Popen(["/usr/bin/du", "-s", "--block-size=1", path],
            shell=False, stdout=subprocess.PIPE)
    retcode = proc.wait()
    if retcode:
        raise RuntimeError("du exited with status %d" % retcode)
    return proc.stdout.read().strip().split()[0]


def compressRoot(fsRoot, destPath, metadata=None):
    """
    Build a LZMAball from the filesystem root at C{fsRoot} and save it
    at C{destPath}. A SHA-512 digest will be written alongside.
    """

    # Add tree size to metadata
    metadata = metadata and dict(metadata) or {}
    metadata['tree_size'] = getTreeSize(fsRoot)

    metaPath = destPath + '.metadata'
    try:
        proc = subprocess.Popen("/bin/tar -cC '%s' "
                "--exclude var/lib/conarydb --exclude var/log/conary . "
                "| /usr/bin/lzma" % (fsRoot,),
                shell=True, stdout=subprocess.PIPE)

        # Copy the compressed image to disk and compute the digest
        # as we do so.
        outObj = open(destPath + '.tmp', 'wb')
        metadata['sha512_digest'] = copySHA512(proc.stdout, outObj)
        outObj.close()

        code = proc.wait()
        if code:
            raise RuntimeError("Compressor exited with status %d" % code)

        # Write metadata
        fObj = open(metaPath, 'w')
        for key in sorted(metadata):
            print >> fObj, key, metadata[key]
        fObj.close()

    except:
        if os.path.exists(destPath + '.tmp'):
            os.unlink(destPath + '.tmp')
        if os.path.exists(metaPath):
            os.unlink(metaPath)
        raise

    # Rename the image to its final location
    os.rename(destPath + '.tmp', destPath)

    return metadata


def specHash(troveTups):
    """
    Create a unique identifier for the troves C{troveTups}.
    """
    ctx = hashlib.sha1()
    ctx.update('%d\0' % (SERIAL,))
    for tup in sorted(troveTups):
        ctx.update('%s=%s[%s]\0' % tup)
    return ctx.hexdigest()


def copySHA512(inFile, outFile=None):
    """
    Copy all data from C{inFile} and return the SHA-512 digest of its
    contents in hexadecimal form. If C{outFile} is not C{None}, data
    will be copied to that file was it is digested.
    """
    ctx = hashlib.sha512()
    while True:
        data = inFile.read(16384)
        if not data:
            break
        ctx.update(data)
        if outFile:
            outFile.write(data)
    return ctx.hexdigest()


class UpdateCallback(callbacks.UpdateCallback):
    def eatMe(self, *P, **K):
        pass

    tagHandlerOutput = troveScriptOutput = troveScriptFailure = eatMe

    def setUpdateHunk(self, hunk, total):
        logging.info('Applying update job %d of %d', hunk, total)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
