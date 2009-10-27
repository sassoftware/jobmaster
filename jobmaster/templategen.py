#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import copy
import cPickle
import errno
import fcntl
import logging
import os
import subprocess
import sys
import tempfile
from conary.conaryclient import ConaryClient
from conary.lib import digestlib
from conary.lib import util
from jobmaster.subprocutil import Lockable, LockError
from jobmaster.util import AtomicFile, call, logCall, setupLogging, specHash

log = logging.getLogger(__name__)


class TemplateGenerator(Lockable):
    def __init__(self, troveTup, conaryCfg, workDir):
        self._troveTup = troveTup
        self._cfg = copy.deepcopy(conaryCfg)

        self._hash = specHash([troveTup])
        self._basePath = os.path.join(os.path.abspath(workDir), self._hash)
        self._outputPath = self._basePath + '.tar'
        self._lockPath = self._basePath + '.lock'

        self._workDir = self._contentsDir = self._outputDir = None

        self._log = logging.getLogger(__name__ + '.' + self._hash[:4])

    def __del__(self):
        self._close()

    def _exists(self):
        return os.path.exists(self._outputPath)

    def get(self):
        try:
            return open(self._outputPath, 'rb')
        except IOError, err:
            if err.errno != errno.ENOENT:
                raise

        # Acquire an exclusive lock to prevent others from wasting effort.
        self._log.debug("Acquiring exclusive lock on %s", self._outputPath)
        try:
            self._lock(fcntl.LOCK_EX)
        except LockError:
            return None

        try:
            self._workDir = tempfile.mkdtemp(prefix='tempdir-',
                    dir=os.path.dirname(self._outputPath))
            self._contentsDir = self._workDir + '/root'
            self._outputDir = self._workDir + '/output'
            fObj = self._generate()
        finally:
            self._lock(fcntl.LOCK_UN)
            util.rmtree(self._workDir)
            self._workDir = self._contentsDir = self._outputDir = None

        self._deleteLock()
        return fObj

    def _installContents(self):
        self._cfg.root = self._contentsDir
        self._cfg.autoResolve = False
        self._cfg.updateThreshold = 0

        cli = ConaryClient(self._cfg)
        try:
            self._log.debug("Preparing template update job")
            job = cli.newUpdateJob()
            troveName, troveVersion, troveFlavor = self._troveTup
            cli.prepareUpdateJob(job, [(troveName,
                (None, None), (troveVersion, troveFlavor), True)],
                resolveDeps=False)

            self._log.debug("Applying template update job")
            cli.applyUpdateJob(job)

        finally:
            job = None
            cli.close()

    def _generate(self):
        self._log.info("Generating template %s from trove %s=%s[%s]",
                self._hash, *self._troveTup)

        self._installContents()

        # Copy "unified" directly into the output.
        os.mkdir(self._outputDir)
        util.copytree(self._contentsDir + '/unified', self._outputDir + '/')

        # Process the MANIFEST file.
        for line in open(self._contentsDir + '/MANIFEST'):
            args = line.rstrip().split(',')
            command = args.pop(0)
            commandFunc = getattr(self, '_DO_' + command, None)
            if not commandFunc:
                raise RuntimeError("Unknown command %r in MANIFEST"
                        % (command,))
            commandFunc(args)

        # Archive the results.
        self._log.info("Creating archive for template %s", self._hash)
        digest = digestlib.sha1()
        outFile = AtomicFile(self._outputPath)

        proc = call(['/bin/tar', '-cC', self._outputDir, '.'],
                stdout=subprocess.PIPE, captureOutput=False, wait=False)
        util.copyfileobj(proc.stdout, outFile, digest=digest)
        proc.wait()

        # Write metadata.
        metaFile = AtomicFile(self._outputPath + '.metadata')
        cPickle.dump({
            'sha1sum': digest.hexdigest(),
            'trovespec': '%s=%s[%s]' % self._troveTup,
            # Right now, we are going to hardcode this to an older version
            # of Netclient Protocol to hint to the Conary installed on the
            # jobslave to generate old filecontainers that are compatible
            # with all versions of Conary. (See RBL-1552.)
            'netclient_protocol_version': '38',
            }, metaFile)

        outFile.commit()
        metaFile.commit()

        self._log.info("Template %s created", self._hash)

    def _DO_image(self, args):
        command = args.pop(0)
        commandFunc = getattr(self, '_RUN_' + command, None)
        if not commandFunc:
            raise RuntimeError("Unknown image command %r in MANIFEST"
                    % (command,))

        if len(args) == 3:
            inputName, outputName, mode = args
            mode = int(mode, 8)
        elif len(args) == 2:
            inputName, outputName = args
            mode = 0644
        else:
            raise RuntimeError("Can't handle image command %r" % (args,))

        inputPath = os.path.abspath(
                os.path.join(self._contentsDir, inputName))
        outputPath = os.path.abspath(
                os.path.join(self._contentsDir, outputName))
        finalPath = os.path.abspath(
                os.path.join(self._outputDir, outputName))
        assert inputPath.startswith(self._contentsDir + '/')
        assert outputPath.startswith(self._contentsDir + '/')
        assert finalPath.startswith(self._outputDir + '/')

        if not os.path.exists(inputPath):
            raise RuntimeError("Input file %r for image command %r is missing"
                    % (inputName, command))

        util.mkdirChain(os.path.dirname(outputPath))
        util.mkdirChain(os.path.dirname(finalPath))
        commandFunc(inputPath, outputPath)

        os.chmod(outputPath, mode)
        os.link(outputPath, finalPath)

    def _RUN_cpiogz(self, inputDir, output):
        oldCwd = os.getcwd()
        os.chdir(inputDir)
        try:
            logCall("find . | cpio --quiet -c -o | gzip -9 > %s" % output)
        finally:
            try:
                os.chdir(oldCwd)
            except:
                pass

    def _RUN_mkisofs(self, inputDir, output):
        logCall(['/usr/bin/mkisofs',
            '-quiet',
            '-o', output,
            '-b', 'isolinux/isolinux.bin',
            '-c', 'isolinux/boot.cat',
            '-no-emul-boot',
            '-boot-load-size', '4',
            '-boot-info-table',
            '-R', '-J', '-T',
            '-V', 'rPath Linux',
            inputDir])

    def _RUN_mkcramfs(self, inputDir, output):
        logCall(['/bin/mkcramfs', inputDir, output])

    def _RUN_mkdosfs(self, inputDir, output):
        out = call(['du', '-ms', inputDir])[1]
        diskSize = int(out.split()[0]) + 4
        logCall(['/bin/dd', 'if=/dev/zero', 'of=' + output,
            'bs=1M', 'count=%d' % diskSize])
        logCall(['/sbin/mkdosfs', output])

        files = [os.path.join(inputDir, x) for x in os.listdir(inputDir)]
        logCall(['/usr/bin/mcopy', '-i', output] + files + ['::'])
        logCall(['/usr/bin/syslinux', output])


def main(args):
    from conary import conarycfg
    from conary.conaryclient.cmdline import parseTroveSpec

    setupLogging(logLevel=logging.DEBUG)

    if len(args) == 1:
        troveSpec, workDir = args[0], '.'
    elif len(args) == 2:
        troveSpec, workDir = args
    else:
        sys.exit("Usage: %s <troveSpec> [<workDir>]" % sys.argv[0])

    cfg = conarycfg.ConaryConfiguration(True)
    cli = ConaryClient(cfg)
    repos = cli.getRepos()

    troveSpec = parseTroveSpec(troveSpec)
    matches = repos.findTrove(None, troveSpec)
    troveTup = sorted(matches)[-1]

    generator = TemplateGenerator(troveTup, cfg, workDir)
    print generator.get()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
