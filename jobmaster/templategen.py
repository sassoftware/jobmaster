#
# Copyright (c) 2010 rPath, Inc.
#
# All rights reserved.
#

import copy
import cPickle
import errno
import fcntl
import logging
import os
import re
import subprocess
import sys
import tempfile
from conary.conaryclient import ConaryClient
from conary.lib import digestlib
from conary.lib import util
from conary.deps import deps
from jobmaster.subprocutil import Lockable, LockError, Subprocess
from jobmaster.util import (call, logCall, makeConstants,
        setupLogging, specHash)

log = logging.getLogger(__name__)


TemplateStatus = makeConstants('TemplateStatus', 'IN_PROGRESS NOT_FOUND DONE')


class TemplateGenerator(Lockable, Subprocess):
    procName = 'template generator'

    Status = TemplateStatus

    def __init__(self, troveTup, kernelTup, conaryCfg, workDir):
        self._troveTup = troveTup
        self._kernelTup = kernelTup
        self._cfg = conaryCfg

        self._hash = specHash([troveTup] + (kernelTup and [kernelTup] or []))
        self._basePath = os.path.join(os.path.abspath(workDir), self._hash)
        self._outputPath = self._basePath + '.tar'
        self._lockPath = self._basePath + '.lock'

        self._workDir = self._contentsDir = self._outputDir = None

        self._log = logging.getLogger(__name__ + '.' + self._hash[:4])

    def __del__(self):
        self._close()

    def _exists(self):
        return os.path.exists(self._outputPath)

    def getTemplate(self, start=True):
        # First try to open the file and return it. Opening ensures that the
        # atime is touched, thus preventing tmpwatch from deleting the file
        # between now and when the jobslave retrieves it.
        try:
            open(self._outputPath, 'rb')
        except IOError, err:
            if err.errno != errno.ENOENT:
                raise
        else:
            return self.Status.DONE, self._outputPath

        # Now we know the template doesn't exist. Get an exclusive lock to
        # prevent others from starting a build, then fork a child process to do
        # the build.
        try:
            self._lock(fcntl.LOCK_EX)
        except LockError:
            # Looks like a build is already underway.
            return self.Status.IN_PROGRESS, self._outputPath

        # If requested, start the build.
        if start:
            self.start()
            ret = self.Status.IN_PROGRESS
        else:
            ret = self.Status.NOT_FOUND

        # Release the lockfile now that the subprocess is running. Use close()
        # instead of flock() because the latter will also affect the
        # subprocess -- it inherited the same file description.
        self._close()
        return ret, self._outputPath

    def generate(self):
        assert self._lockLevel == fcntl.LOCK_EX
        self._lock(fcntl.LOCK_EX)
        try:
            self._workDir = tempfile.mkdtemp(prefix='tempdir-',
                    dir=os.path.dirname(self._outputPath))
            self._contentsDir = self._workDir + '/root'
            self._outputDir = self._workDir + '/output'
            self._kernelDir = self._workDir + '/kernel'
            self._generate()
            self._deleteLock()
        finally:
            self._lock(fcntl.LOCK_UN)
            util.rmtree(self._workDir)
            self._workDir = self._contentsDir = self._outputDir = None
    run = generate

    def _installContents(self, root, troves):
        cfg = copy.deepcopy(self._cfg)
        cfg.root = root
        cfg.autoResolve = False
        cfg.updateThreshold = 0

        cli = ConaryClient(cfg)
        try:
            self._log.debug("Preparing update job")
            job = cli.newUpdateJob()
            jobList = [(x[0], (None, None), (x[1], x[2]), True)
                    for x in troves]
            cli.prepareUpdateJob(job, jobList, resolveDeps=False)

            self._log.debug("Applying update job")
            cli.applyUpdateJob(job, noScripts = True)

        finally:
            job = None
            cli.close()

    def _generate(self):
        self._log.info("Generating template %s from trove %s=%s[%s]",
                self._hash, *self._troveTup)

        self._installContents(self._contentsDir, [self._troveTup])

        # Copy "unified" directly into the output.
        os.mkdir(self._outputDir)
        util.copytree(self._contentsDir + '/unified', self._outputDir + '/')

        # Process the MANIFEST file.
        for line in open(self._contentsDir + '/MANIFEST'):
            line = line.rstrip()
            if not line or line[0] == '#':
                continue
            args = line.rstrip().split(',')
            command = args.pop(0)
            commandFunc = getattr(self, '_DO_' + command, None)
            if not commandFunc:
                raise RuntimeError("Unknown command %r in MANIFEST"
                        % (command,))
            commandFunc(args)

        # Archive the results.
        digest = digestlib.sha1()
        outFile = util.AtomicFile(self._outputPath)

        proc = call(['/bin/tar', '-cC', self._outputDir, '.'],
                stdout=subprocess.PIPE, captureOutput=False, wait=False)
        util.copyfileobj(proc.stdout, outFile, digest=digest)
        proc.wait()

        # Write metadata.
        metaFile = util.AtomicFile(self._outputPath + '.metadata')
        cPickle.dump({
            'sha1sum': digest.hexdigest(),
            'trovespec': '%s=%s[%s]' % self._troveTup,
            'kernel': (self._kernelTup and ('%s=%s[%s]' % self._kernelTup)
                or '<none>'),
            # Right now, we are going to hardcode this to an older version
            # of Netclient Protocol to hint to the Conary installed on the
            # jobslave to generate old filecontainers that are compatible
            # with all versions of Conary. (See RBL-1552.)
            'netclient_protocol_version': '38',
            }, metaFile)

        metaFile.commit()
        outFile.commit()

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
        logCall(['/sbin/mkfs.cramfs', inputDir, output])

    def _RUN_mkdosfs(self, inputDir, output):
        out = call(['du', '-ms', inputDir])[1]
        diskSize = int(out.split()[0]) + 4
        logCall(['/bin/dd', 'if=/dev/zero', 'of=' + output,
            'bs=1M', 'count=%d' % diskSize])
        logCall(['/sbin/mkdosfs', output])

        files = [os.path.join(inputDir, x) for x in os.listdir(inputDir)]
        logCall(['/usr/bin/mcopy', '-i', output] + files + ['::'])
        logCall(['/usr/bin/syslinux', output])

    def _RUN_mkfs_squashfs(self, inputDir, output):
        logCall(['/sbin/mksquashfs', inputDir, output,
            '-no-fragments'])

    def _RUN_cp(self, inPath, outPath):
        util.copyfile(inPath, outPath)

    def _RUN_ln(self, inPath, outPath):
        os.link(inPath, outPath)

    def _DO_kernel(self, args):
        if not self._kernelTup:
            raise RuntimeError("Encountered 'kernel' manifest command but "
                    "jobslave didn't provide a kernel")
        
        command = args.pop(0)
        if command == 'install':
            self._installContents(self._kernelDir, [self._kernelTup])
            # XXX - SLES 11 needs kernel-base
            kernels = []
            if os.path.exists(os.path.join(self._kernelDir, 'boot')):
                kernels = [ x for x in 
                            os.listdir(os.path.join(self._kernelDir, 'boot')) if
                            x.startswith('vmlinuz') ]               
            if len(kernels) == 0:
                self._installContents(self._kernelDir,
                    [ ('kernel-base', self._kernelTup[1], self._kernelTup[2]) ])
            return
        commandFunc = getattr(self, '_KERNEL_' + command, None)
        if not commandFunc:
            raise RuntimeError("Unknown kernel command %r in MANIFEST"
                    % (command,))

        """
        KERNEL, CONTENTS, and ARCH are treated as macros.
        Extend the list as needed.  This can probably be
        combined into one big RegEx.
        """
        commandArgs = []
        kernelFlavor = deps.formatFlavor(self._kernelTup[2])
        arch = (kernelFlavor.find('x86_64') == -1) and 'i386' or 'x86_64'
        contentsRE = re.compile('(^|/)CONTENTS(/|$)')
        kernelRE = re.compile('(^|/)KERNEL(/|$)')
        archRE = re.compile('(^|/)ARCH(/|$)')
        while len(args) > 0:
            nextArg = archRE.sub(r'\1%s\2' % arch,
                      kernelRE.sub(r'\1%s\2' % self._kernelDir,
                      contentsRE.sub(r'\1%s\2' % self._contentsDir, 
                      args.pop(0))))
            commandArgs.append(nextArg)

        # anaconda scripts may take different args, so just pass them
        if command == 'anacondaScript':
            self._KERNEL_anacondaScript(commandArgs)
            return

        # This is only for "copy" right now
        if len(commandArgs) == 3:
            inputName, outputName, mode = commandArgs
            mode = int(mode, 8)
        elif len(commandArgs) == 2:
            inputName, outputName = commandArgs
            mode = 0644
        else:
            raise RuntimeError("Can't handle kernel command %r" % (command,))
        commandFunc(inputName, outputName, mode)
        

    def _KERNEL_copy(self, inputSpec, outputFile, mode):
        inputDir = os.path.abspath(os.path.dirname(inputSpec))
        outputFile = os.path.abspath(outputFile)
        finalFile = os.path.abspath(outputFile).replace(self._contentsDir, self._outputDir)
        inputFileSpec = os.path.basename(inputSpec)
        if (not inputDir.startswith(self._workDir) or
            not outputFile.startswith(self._contentsDir)):
            raise RuntimeError("Can't copy outside contents directory")

        # We only expect one match.  If there are more, they
        # should be identical, anyway
        match = [ x for x in os.listdir(inputDir) if x.startswith(inputFileSpec) ][0]
        util.mkdirChain(os.path.dirname(outputFile))
        util.mkdirChain(os.path.dirname(finalFile))
        self._log.info("copying %s to %s", os.path.join(inputDir, match), outputFile)
        util.copyfile(os.path.join(inputDir, match), outputFile)
        os.chmod(outputFile, mode)   
        self._log.info("linking %s to %s", outputFile, finalFile)
        os.link(outputFile, finalFile)
        
    def _KERNEL_anacondaScript(self, argList):
        """
        This function was written to run mk-images in "modules only"
        mode, but maybe it can be flexible enough to run other
        anaconda scripts.
        """

        scriptName = argList.pop(0)
        scriptPath = "%s/instrootgr/usr/lib/anaconda-runtime/%s" % \
                     (self._contentsDir, scriptName)
        if not os.path.exists(scriptPath):
            raise RuntimeError("Script %s does not exist" % scriptName)
        
        logCall([ "bash", "-x", scriptPath ] + argList, stderr=open(self._basePath + '.log', 'w'))

def main(args):
    import time
    from conary import conarycfg
    from conary.conaryclient.cmdline import parseTroveSpec

    setupLogging(logLevel=logging.DEBUG)

    if len(args) == 2:
        troveSpec, kernelSpec, workDir = args[0], args[1], '.'
    elif len(args) == 3:
        troveSpec, kernelSpec, workDir = args
    else:
        sys.exit("Usage: %s <troveSpec> <kernelSpec> [<workDir>]" % sys.argv[0])

    cfg = conarycfg.ConaryConfiguration(True)
    cli = ConaryClient(cfg)
    repos = cli.getRepos()

    troveTup = sorted(repos.findTrove(None, parseTroveSpec(troveSpec)))[-1]
    kernelTup = sorted(repos.findTrove(None, parseTroveSpec(kernelSpec)))[-1]

    generator = TemplateGenerator(troveTup, kernelTup, cfg, workDir)
    generator.getTemplate(start=True)
    while True:
        status, path = generator.getTemplate(start=False)
        if status == generator.Status.NOT_FOUND:
            print 'Failed!'
            break
        elif status == generator.Status.DONE:
            print 'Done:', path
            break
        elif status == generator.Status.IN_PROGRESS:
            print 'working'
        time.sleep(1)

    generator.wait()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
