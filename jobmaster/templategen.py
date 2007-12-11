#
# Copyright (c) 2005-2007 rPath, Inc.
#
# All rights reserved
#

from conary import callbacks
from conary import conaryclient
from conary import conarycfg
from conary.errors import TroveNotFound
from conary.lib import sha1helper
from conary.lib import util
from conary.conaryclient.cmdline import parseTroveSpec

import cPickle
import errno
import os
import optparse
import signal
import sys
import subprocess
import tempfile
import time

MSG_INTERVAL = 1 # second (for Update Callbacks posted to logs)

def call(cmds, env=None, logPath=None, statusPath=None):
    msg = "Running " + " ".join(cmds)
    kwargs = {'env': env}
    log(msg, logPath, statusPath)
    subprocess.call(cmds, **kwargs)

def log(msg, logPath=None, statusPath=None, truncateLog=False):
    statF = logF = None
    msgWithTimestamp = "[%s] %s" % \
            (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
    try:
        if truncateLog:
            logOpt = 'w+'
        else:
            logOpt = 'a+'

        if statusPath:
            statF = open(statusPath, 'w+')
            print >> statF, msg

        if logPath:
            logF = open(logPath, logOpt)
        else:
            logF = sys.stderr

        print >> logF, msgWithTimestamp
        logF.flush()

    finally:
        for f in (statF, logF):
            if f and (f.fileno() > sys.stderr.fileno()):
                f.close()


class AnacondaTemplate(object):

    _fullTroveSpec = None
    _fullTroveSpecHash = None
    _conaryClient = None
    _uJob = None
    _callback = None
    logPath = None
    statusPath = None

    def _call(self, cmd, **kwargs):
        return call(cmd, logPath=self.logPath, statusPath=self.statusPath,
                **kwargs)

    def _lock(self):
        def _writeLockfile(lockPath):
            lfd = 0
            locked = False
            try:
                try:
                    lfd = os.open(lockPath,
                            os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                    os.write(lfd, str(os.getpid()))
                    locked = True
                except OSError, e:
                    if e.errno != errno.EEXIST:
                        raise
            finally:
                if lfd:
                    os.close(lfd)
            return locked

        locked = False
        lfd = 0

        if _writeLockfile(self.lockPath):
            return True
        else:
            oldpidfile = open(self.lockPath)
            stalepid = oldpidfile.read().strip()
            oldpidfile.close()
            if stalepid not in os.listdir('/proc'):
                if self._unlock():
                    return _writeLockfile(self.lockPath)
        return False

    def _unlock(self):
        if (os.path.exists(self.lockPath)):
            os.unlink(self.lockPath)
            return True
        return False

    def _getTroveSpecs(self, uJob):
        assert(self._getUpdateJob())
        ts = []
        for job in self._getUpdateJob().getPrimaryJobs():
            trvName, trvVersion, trvFlavor = job[0], str(job[2][0]), str(job[2][1])
            ts.append("%s=%s[%s]" % (trvName, trvVersion, trvFlavor))
        return ts

    def _getConaryClient(self):
        if not self._conaryClient:
            assert(self.tmpRoot)
            cfg = conarycfg.ConaryConfiguration()
            cfg.root = self.tmpRoot
            cfg.tmpDir = self.tmpDir
            if self.conaryProxy:
                cfg.conaryProxy['http']  = self.conaryProxy
                cfg.conaryProxy['https'] = self.conaryProxy
            cfg.configLine('includeConfigFile %sconaryrc' % self.conaryProxy)
            self._conaryClient = conaryclient.ConaryClient(cfg)

        return self._conaryClient

    def _getUpdateJob(self):
        if not self._uJob:
            assert(self._getConaryClient())
            trvName, trvVersion, trvFlavor = parseTroveSpec(self.troveSpec)
            log("Finding update job for %s" % self.troveSpec)
            itemList = [ (trvName, (None, None),
                                   (trvVersion, trvFlavor), True) ]
            self._callback = TemplateUpdateCallback(self.logPath,
                    self.statusPath)
            self._callback.setChangeSet(trvName)
            self._getConaryClient().setUpdateCallback(self._callback)
            self._uJob, _ = self._getConaryClient().updateChangeSet(itemList,
                resolveDeps=False)
        return self._uJob

    def _generateHash(self):
        # Get the full trovespec (with branch and flavor)
        # so we can hash it and make a unique template name
        self._fullTroveSpec = self._getTroveSpecs(self._getUpdateJob())[0]
        self._fullTroveSpecHash = \
                sha1helper.md5ToString(sha1helper.md5String(self._fullTroveSpec))

    def __init__(self, version, flavor, cacheDir, tmpDir='/var/tmp',
      conaryProxy=None):
        self.troveSpec = 'anaconda-templates=%s[%s]' % (version, flavor)
        self.conaryProxy = conaryProxy
        self.cacheDir = cacheDir
        self.tmpDir = tmpDir
        self.tmpRoot = tempfile.mkdtemp(dir=self.tmpDir)
        self.templatePath = os.path.join(self.cacheDir,
            '%s.tar' % self.getFullTroveSpecHash())
        self.metadataPath = os.path.join(self.cacheDir,
            '.%s.metadata' % self.getFullTroveSpecHash())
        self.statusPath = os.path.join(self.cacheDir,
            '.%s.status' % self.getFullTroveSpecHash())
        self.lockPath = os.path.join(self.cacheDir,
            '.%s.lock' % self.getFullTroveSpecHash())
        self.logPath = os.path.join(self.cacheDir,
            '.%s.log' % self.getFullTroveSpecHash())

    def __del__(self):
        # XXX workaround for CNY-1834
        if self._conaryClient:
            if self._conaryClient.db:
                self._conaryClient.db.close()
            del self._conaryClient
        if self.tmpRoot:
            util.rmtree(self.tmpRoot, ignore_errors=True)

    def getFullTroveSpec(self):
        if not self._fullTroveSpec:
            self._generateHash()
        return self._fullTroveSpec

    def getFullTroveSpecHash(self):
        if not self._fullTroveSpecHash:
            self._generateHash()
        return self._fullTroveSpecHash

    def exists(self):
        return os.path.exists(self.templatePath) and \
               os.path.exists(self.metadataPath)

    def status(self):
        isRunning = False
        status = ''
        f = None
        try:
            try:
                f = open(self.statusPath, 'r')
                isRunning = True
                status = f.read()
            except:
                pass
        finally:
            if f:
                f.close()
        return (isRunning, status)

    def getMetadata(self):
        metadata = {}
        f = None
        try:
            try:
                f = open(self.metadataPath, 'r')
                metadata = cPickle.load(f)
            except (OSError, IOError), e:
                log("ERROR: Failed to read metadata file %s (%s)" % \
                        (self.metadataPath, str(e)))
        finally:
            if f:
                f.close()
        return metadata

    def _cleanup(self):
        if self.templateWorkDir:
            log("Cleaning up template working directory %s" % \
                    self.templateWorkDir, self.logPath, self.statusPath)
            util.rmtree(self.templateWorkDir, ignore_errors=True)
        if self.statusPath:
            os.unlink(self.statusPath)
        self._unlock()

    def _signaled(self, sig, frame):
        log("Caught signal %d, cleaning up" % sig)
        self._cleanup()

    def generate(self):

        signal.signal(signal.SIGINT, self._signaled)
        signal.signal(signal.SIGTERM, self._signaled)

        rc = 0
        templateData = {}
        if self.exists():
            log("Found a cached template based on %s in %s; exiting" % \
                        (self.getFullTroveSpec(), self.cacheDir),
                        self.logPath, self.statusPath)
            return 2

        util.mkdirChain(self.cacheDir)

        if not self._lock():
            log("Someone else is building a matching anaconda template",
                    self.logPath, self.statusPath)
            return 3

        # Create our logfile and status file
        log("Caching anaconda-templates based upon %s" % \
                self.getFullTroveSpec(),
                self.logPath, self.statusPath, truncateLog = True)
        self.templateWorkDir = self.templatePath + ".tmpdir"

        # Create a new callback to use the logfile and status file
        # (The previous one was created too early in the process.)
        oldcallback = None
        if self._callback:
            oldcallback = self._callback
        self._callback = TemplateUpdateCallback(self.logPath,
                    self.statusPath)
        self._callback.setChangeSet('anaconda-templates')
        self._getConaryClient().setUpdateCallback(self._callback)
        if oldcallback:
            del oldcallback

        try:
            try:
                # Remove the stale and/or broken template to start
                # fresh, ignoring errors
                for f in (self.templatePath, self.logPath, self.metadataPath):
                    try:
                        os.unlink(f)
                    except OSError, e:
                        # Let "file not found" exceptions pass
                        if e.errno != errno.ENOENT:
                            raise

                # Create the working dir
                util.mkdirChain(self.templateWorkDir)

                # Download the changeset and install it in the temproot
                log("Applying changeset %s" % self.getFullTroveSpec(),
                        self.logPath, self.statusPath)
                self._getConaryClient().applyUpdate(self._getUpdateJob())
                util.copytree(os.path.join(self.tmpRoot, 'unified'),
                    self.templateWorkDir + os.path.sep)

                # Get the maximum netclient protocol version supported by this
                # Anaconda Template and stash it away for later.

                # XXX Right now, we are going to hardcode this to an older
                #     version of Netclient Protocol to hint to the
                #     Conary installed on the jobslave to generate old
                #     filecontainers that are compatible with all versions of
                #     Conary. (See RBL-1552.)
                templateData['netclient_protocol_version'] = '38'
                templateData['trovespec'] = self.getFullTroveSpec()

                # Process the Anaconda template using the instructions in the
                # MANIFEST file.
                #
                # XXX We need to process shell lines and make sure that they 
                #     are reasonably safe (i.e. look for joins, like ';' or '||'
                #     '&&' and error out of they are found). This is to guard
                #     against a user attacking via any commands run through
                #     os.system().
                manifest = open(os.path.join(self.tmpRoot, "MANIFEST"), 'r')
                for line in manifest.xreadlines():
                    args = [ x.strip() for x in line.split(',') ]
                    cmd = args.pop(0)
                    try:
                        func = self.__getattribute__("_DO_" + cmd)
                    except AttributeError:
                        raise RuntimeError, "Invalid command in MANIFEST: %s" % (cmd)
                    ret = func(*args)
                manifest.close()

                # Tar up the resultant templates
                log("Tarring up anaconda-templates", self.logPath,
                        self.statusPath)
                tarCmd = ['tar', '-c', '-O', '.']
                teeCmd = ['tee', self.templatePath]
                sha1Cmd = ['sha1sum']
                p1 = subprocess.Popen(tarCmd, cwd=self.templateWorkDir,
                        stdout=subprocess.PIPE)
                p2 = subprocess.Popen(teeCmd, stdin=p1.stdout,
                        stdout=subprocess.PIPE)
                p3 = subprocess.Popen(sha1Cmd, stdin=p2.stdout,
                        stdout=subprocess.PIPE)
                sha1sum = p3.communicate()[0][0:40]
                templateData['sha1sum'] = sha1sum

                log("Writing metadata", self.logPath,
                        self.statusPath)
                f = open(self.metadataPath, 'w')
                f.write(cPickle.dumps(templateData))
                f.close()

                log("Template created", self.logPath, self.statusPath)
            except Exception, e:
                rc = 1
                log("Fatal error %s occurred while creating template %s" % \
                        (str(e), self.getFullTroveSpec()),
                        self.logPath, self.statusPath)
        finally:
            self._cleanup()

        return rc


    def _DO_image(self, cmd, *args):
        args = list(args)
        try:
            func = self.__getattribute__("_RUN_" + cmd)
        except AttributeError:
            raise RuntimeError, \
                    "Invalid manifest image command: %s" % cmd

        if len(args) == 3:
            mode = int(args.pop(-1), 8)
        else:
            mode = 0644

        input = os.path.join(self.tmpRoot, args[0])
        output = os.path.join(self.templateWorkDir, args[1])

        util.mkdirChain(os.path.dirname(output))
        retcode = func(input, output)
        os.chmod(output, mode)

        # copy the resulting file back to the source area in
        # case it is used elsewhere in the manifest
        util.copyfile(output, os.path.join(self.tmpRoot, args[1]))
        return retcode

    def _RUN_cpiogz(self, inputDir, output):
        oldCwd = os.getcwd()
        os.chdir(inputDir)
        try:
            os.system("find . | cpio --quiet -c -o | gzip -9 > %s" % output)
        finally:
            try:
                os.chdir(oldCwd)
            except:
                pass

    def _RUN_mkisofs(self, inputDir, output):
        cmd = ['mkisofs', '-quiet', '-o', output,
            '-b', 'isolinux/isolinux.bin',
            '-c', 'isolinux/boot.cat',
            '-no-emul-boot',
            '-boot-load-size', '4',
            '-boot-info-table',
            '-R', '-J', '-T',
            '-V', 'rPath Linux',
            inputDir]
        self._call(cmd)

    def _RUN_mkcramfs(self, inputDir, output):
        cmd = ['mkcramfs', inputDir, output]
        self._call(cmd)

    def _RUN_mkdosfs(self, inputDir, output):
        self._call(['dd', 'if=/dev/zero', 'of=%s' % output, 'bs=1M', 'count=8'])
        self._call(['/sbin/mkdosfs', output])

        files = [os.path.join(inputDir, x) for x in os.listdir(inputDir)]
        cmds = ['mcopy', '-i', output] + files + ['::']
        self._call(cmds)
        self._call(['syslinux', output])


class TemplateUpdateCallback(callbacks.UpdateCallback):
    def requestingChangeSet(self):
        self._update('Requesting %s from repository')

    def downloadingChangeSet(self, got, need):
        if need != 0:
            self._update('Downloading %%s from repository (%d%%%% of %dk)'
                         %((got * 100) / need, need / 1024))

    def downloadingFileContents(self, got, need):
        if need != 0:
            self._update('Downloading files for %%s from repository '
                         '(%d%%%% of %dk)' %((got * 100) / need, need / 1024))

    def restoreFiles(self, size, totalSize):
        if totalSize != 0:
            self.restored += size
            self._update("Writing %%s %dk of %dk (%d%%%%)"
                        % (self.restored / 1024 , totalSize / 1024,
                           (self.restored * 100) / totalSize))

    def _update(self, msg):
        curTime = time.time()
        if self.msg != msg and (curTime - self.timeStamp) > MSG_INTERVAL:
            self.msg = msg
            log(self.prefix + msg % self.changeset,
                    self.logPath, self.statusPath)
            self.timeStamp = curTime

    def setChangeSet(self, name):
        self.changeset = name

    def setPrefix(self, prefix):
        self.prefix = prefix

    def __init__(self, logPath, statusPath):
        self.exceptions = []
        self.abortEvent = None
        self.restored = 0
        self.msg = ''
        self.changeset = ''
        self.prefix = ''
        self.timeStamp = 0
        self.logPath = logPath
        self.statusPath = statusPath

        callbacks.UpdateCallback.__init__(self)

class AnacondaTemplateGenerator(object):
    _at = None
    _args = None
    _options = None

    def handle_args(self, args):
        usage = "%prog [options] output_dir"
        op = optparse.OptionParser(usage=usage)
        op.add_option("-V", "--version",
                dest = "version", default = "conary.rpath.com@rpl:1",
                help = "which version of anaconda templates to generate")
        op.add_option("-F", "--flavor",
                dest = "flavor", default = "is: x86",
                help = "which flavor of anaconda templates to generate")
        (self._options, self._args) = op.parse_args(args)
        if len(self._args) < 1:
            op.error("missing output directory")
            return False
        self._outdir = self._args.pop()
        return True

    def run(self):
        try:
            self._at = AnacondaTemplate(self._options.version,
                    self._options.flavor, self._outdir)
            return(self._at.generate())
        except TroveNotFound:
            print >> sys.stderr, "anaconda-templates not found with version %s, flavor %s; exiting" % (self._options.version, self._options.flavor)
            return 1

    def __init__(self, args = sys.argv):
        self.handle_args(args)

if __name__ == '__main__':
    atg = AnacondaTemplateGenerator()
    sys.exit(atg.run())
