#
# Copyright (c) 2005-2007, 2009 rPath, Inc.
#
# All rights reserved.
#

import cPickle
import errno
import logging
import optparse
import os
import signal
import subprocess
import sys
import tempfile
import time

from conary import callbacks
from conary import conarycfg
from conary import conaryclient
from conary.conaryclient.cmdline import parseTroveSpec
from conary.deps import deps
from conary.errors import TroveNotFound
from conary.lib import digestlib
from conary.lib import sha1helper
from conary.lib import util

from jobmaster.util import logCall

log = logging.getLogger(__name__)


MSG_INTERVAL = 1 # second (for Update Callbacks posted to logs)


class AnacondaTemplate(object):
    def __init__(self, name, version, flavor, cacheDir, tmpDir='/var/tmp',
            conaryProxy=None):
        flavor = deps.parseFlavor(flavor)
        self.troveSpec = (name, version, flavor)
        self.conaryProxy = conaryProxy
        self.cacheDir = cacheDir
        self.tmpDir = tmpDir
        self.tmpRoot = tempfile.mkdtemp(dir=self.tmpDir)

        self._client = self._getConaryClient()
        self.troveTup = self._findTrove()
        self.hash = self._getHash()

        basePath = os.path.join(self.cacheDir, self.hash)
        dotPath = os.path.join(self.cacheDir, '.' + self.hash)

        self.templatePath = basePath + '.tar'
        self.metadataPath = basePath + '.metadata'
        self.statusPath = dotPath + '.status'
        self.lockPath = dotPath + '.lock'

        self.logger = logging.getLogger('template-' + self.hash[:12])

    def __del__(self):
        if self._conaryClient:
            self._conaryClient.close()
            self._conaryClient = None
        if self.tmpRoot:
            util.rmtree(self.tmpRoot, ignore_errors=True)

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

    def _getConaryClient(self):
        cfg = conarycfg.ConaryConfiguration()
        cfg.root = self.tmpRoot
        cfg.tmpDir = self.tmpDir
        if self.conaryProxy:
            cfg.conaryProxy['http']  = self.conaryProxy
            cfg.conaryProxy['https'] = self.conaryProxy
        return conaryclient.ConaryClient(cfg)

    def _getUpdateJob(self):
        trvName, trvVersion, trvFlavor = parseTroveSpec(self.troveSpec)
        log("Finding update job for %s" % self.troveSpec)
        itemList = [ (trvName, (None, None),
                               (trvVersion, trvFlavor), True) ]
        self._callback = TemplateUpdateCallback(self.logPath,
                self.statusPath)
        self._callback.setChangeSet(trvName)
        self._client.setUpdateCallback(self._callback)
        return self._client.updateChangeSet(itemList, resolveDeps=False)[0]

    def _findTrove(self):
        """
        Find the requested templates trove and return the trove tuple.
        """
        matches = self._client.getSearchSource().findTrove(self.troveSpec)
        return sorted(matches)[0]

    def _getHash(self):
        """
        Hash the trove tuple to get the name where the templates will be
        cached.
        """
        return digestlib.md5('%s=%s[%s]' % self.troveTup).hexdigest()

    def exists(self):
        """
        Return C{True} if a cached copy of the template exists already.
        """
        return (os.path.exists(self.templatePath) and
                os.path.exists(self.metadataPath))

    def status(self):
        try:
            fObj = open(self.statusPath, 'r')
        except IOError, err:
            if err.errno == errno.ENOENT:
                return False, ''
            raise
        else:
            return True, fObj.read()

    def getMetadata(self):
        try:
            fObj = open(self.metadataPath, 'rb')
        except IOError, err:
            if err.errno == errno.ENOENT:
                self.logger.warning("Metadata file does not exist.")
                return {}
            raise
        else:
            return cPickle.load(fObj)

    def _cleanup(self):
        if self.templateWorkDir:
            self.logger.info("Cleaning up template working directory")
            util.rmtree(self.templateWorkDir, ignore_errors=True)
        if self.statusPath:
            os.unlink(self.statusPath)
        self._unlock()

    def _signaled(self, sig, frame):
        self.logger.info("Caught signal %d, cleaning up", sig)
        self._cleanup()

    def generate(self):

        signal.signal(signal.SIGINT, self._signaled)
        signal.signal(signal.SIGTERM, self._signaled)

        rc = 0
        templateData = {}
        if self.exists():
            self.logger.info("Serving cached template")
            return 2

        util.mkdirChain(self.cacheDir)

        if not self._lock():
            self.logger.info("Another process is already building "
                    " this template")
            return 3

        # Create our logfile and status file
        self.logger.info("Caching anaconda-templates based upon %s=%s[%s]",
                self.troveTup)
        self.templateWorkDir = self.templatePath + ".tmpdir"

        # Create a new callback to use the logfile and status file
        # (The previous one was created too early in the process.)
        oldcallback = None
        if self._callback:
            oldcallback = self._callback
        self._callback = TemplateUpdateCallback(self.logPath,
                    self.statusPath)
        self._callback.setChangeSet('anaconda-templates')
        self._client.setUpdateCallback(self._callback)
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
                self._client.applyUpdate(self._getUpdateJob())
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
                raise
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

    def __init__(self, args = sys.argv[1:]):
        self.handle_args(args)

if __name__ == '__main__':
    atg = AnacondaTemplateGenerator()
    sys.exit(atg.run())
