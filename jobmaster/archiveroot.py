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
import shutil
import subprocess
import sys
import tempfile
from conary import callbacks
from conary import conarycfg
from conary import conaryclient
from conary import updatecmd
from conary.conaryclient import cmdline
from jobmaster.util import setupLogging, createFile

log = logging.getLogger(__name__)


def archiveRoot(fsRoot, destPath, metadata=None):
    metadata = metadata and dict(metadata) or {}

    metaPath = destPath + '.metadata'
    try:
        proc = subprocess.Popen("/bin/tar -cC '%s' . "
                #"--exclude var/lib/conarydb "
                "--exclude var/lib/conarydb/rollbacks "
                "--exclude var/log/conary "
                "| /usr/bin/lzma -c2" % (fsRoot,),
                shell=True, stdout=subprocess.PIPE)

        try:
            # Copy the compressed image to disk and compute the digest
            # as we do so.
            outObj = open(destPath + '.tmp', 'wb')
            metadata['sha512_digest'] = copySHA512(proc.stdout, outObj)
            outObj.close()
        except:
            proc.terminate()
            proc.wait()
            raise

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


def unpackRoot(archivePath, destRoot):
    destRoot = os.path.realpath(destRoot)

    metaPath = archivePath + '.metadata'
    metadata = dict(x.strip().split(' ', 1) for x in open(metaPath))

    tmpRoot = tempfile.mkdtemp(prefix='temproot-',
            dir=os.path.dirname(destRoot))
    try:
        proc = subprocess.Popen("/usr/bin/lzma -dc "
                "| /bin/tar -xC '%s'" % (tmpRoot,),
                shell=True, stdin=subprocess.PIPE)

        try:
            inObj = open(archivePath, 'rb')
            digest = copySHA512(inObj, proc.stdin)
            inObj.close()
            proc.stdin.close()
        except:
            proc.terminate()
            proc.wait()
            raise

        code = proc.wait()
        if code:
            raise RuntimeError("Decompressor exited with status %d" % code)

        if 'sha512_digest' in metadata:
            if metadata['sha512_digest'] != digest:
                raise RuntimeError("Cached root failed SHA-512 check")
        else:
            log.warning("SHA-512 missing from archive %s ; continuing",
                    archivePath)

    except:
        shutil.rmtree(tmpRoot)
        raise

    os.rename(tmpRoot, destRoot)

    return metadata



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


def main(args):
    rootPath, archivePath = args
    rootPath = os.path.realpath(rootPath)
    archivePath = os.path.realpath(archivePath)

    compressRoot(rootPath, archivePath)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
