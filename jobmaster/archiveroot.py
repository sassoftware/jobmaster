#
# Copyright (c) 2009 rPath, Inc.
#
# All Rights Reserved
#

import logging
import os
import signal
import shutil
import subprocess
import sys
import tempfile
from conary.lib.util import copyfileobj

log = logging.getLogger(__name__)


def archiveRoot(fsRoot, destPath):
    """
    Archive the contents of the directory C{fsRoot} to the xzball C{destPath}.
    """
    try:
        proc = subprocess.Popen("/bin/tar -cC '%s' . "
                #"--exclude var/lib/conarydb "
                "--exclude var/lib/conarydb/rollbacks/\\* "
                "--exclude var/log/conary "
                "| /usr/bin/xz -9c" % (fsRoot,),
                shell=True, stdout=subprocess.PIPE)

        try:
            # Copy the compressed image to disk and compute the digest
            # as we do so.
            outObj = open(destPath + '.tmp', 'wb')
            copyfileobj(proc.stdout, outObj)
            outObj.close()
        except:
            os.kill(proc.pid, signal.SIGTERM)
            proc.wait()
            raise

        code = proc.wait()
        if code:
            raise RuntimeError("Compressor exited with status %d" % code)

    except:
        if os.path.exists(destPath + '.tmp'):
            os.unlink(destPath + '.tmp')
        raise

    # Rename the image to its final location
    os.rename(destPath + '.tmp', destPath)


def unpackRoot(archivePath, destRoot):
    """
    Unpack the xzball at C{archivePath} to the target directory C{destRoot}.
    C{archivePath} may also be a file-like object from which the archive is to
    be read.
    """
    destRoot = os.path.realpath(destRoot)

    if hasattr(archivePath, 'fileno'):
        inObj = archivePath
    else:
        inObj = open(archivePath, 'rb')

    tmpRoot = tempfile.mkdtemp(prefix='temproot-',
            dir=os.path.dirname(destRoot))
    try:
        proc = subprocess.Popen("/usr/bin/xz -dc "
                "| /bin/tar -xC '%s'" % (tmpRoot,),
                shell=True, stdin=subprocess.PIPE)

        try:
            copyfileobj(inObj, proc.stdin)
            proc.stdin.close()
        except:
            os.kill(proc.pid, signal.SIGTERM)
            proc.wait()
            raise

        code = proc.wait()
        if code:
            raise RuntimeError("Decompressor exited with status %d" % code)

        os.rename(tmpRoot, destRoot)

    except:
        shutil.rmtree(tmpRoot)
        raise


def main(args):
    if len(args) not in (1, 2):
        sys.exit("Usage: %s <root> [target.tar.xz]" % sys.argv[0])
    root = args.pop(0)
    if args:
        target, = args
    else:
        target = os.path.basename(root) + '.tar.xz'

    if os.path.exists(target):
        sys.exit("error: target exists: %s" % target)

    archiveRoot(root, target)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
