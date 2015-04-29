#!/usr/bin/python
#
# Copyright (c) 2011 rPath, Inc.
#

import logging
import optparse
import os
import json
import sys
from conary import conarycfg
from conary.lib.log import setupLogging
from conary.lib.util import rmtree
from mcp import jobstatus
from mcp.messagebus import bus_node
from mcp.messagebus import messages
from mcp.messagebus import nodetypes
from mcp.messagebus.logger import MessageBusLogger
from rmake.lib import procutil

from jobmaster import config
from jobmaster import jobhandler
from jobmaster import util
from jobmaster.networking import AddressGenerator
from jobmaster.proxy import ProxyServer
from jobmaster.resources.devfs import LoopManager
from jobmaster.resources.block import get_scratch_lvs
from jobmaster.response import ResponseProxy
from jobmaster.subprocutil import setDebugHook

# Register image job message type with rMake
from mcp import image_job
image_job = image_job

log = logging.getLogger(__name__)


class JobMaster(bus_node.BusNode):
    sessionClass = 'image_master'
    subscriptions = [
            '/image_command',
            ]
    timerPeriod = 5

    def __init__(self, cfg):
        node = nodetypes.MasterNodeType(cfg.slaveLimit,
                procutil.MachineInformation())
        buslogger = MessageBusLogger.new(__name__ + '.messagebus')
        bus_node.BusNode.__init__(self, (cfg.queueHost, cfg.queuePort),
                nodeInfo=node, logger=buslogger)
        self.cfg = cfg
        self.handlers = {}
        self.subprocesses = []
        self._cfgCache = {}
        self._map = self.bus.session._map

    def getConaryConfig(self, rbuilderUrl, cache=True):
        if cache and rbuilderUrl in self._cfgCache:
            ccfg = self._cfgCache[rbuilderUrl]
        else:
            if not rbuilderUrl.endswith('/'):
                rbuilderUrl += '/'
            ccfg = conarycfg.ConaryConfiguration(True)
            ccfg.initializeFlavors()
            # Don't inherit proxy settings from the system
            ccfg.configLine('proxyMap []')
            ccfg.configLine('includeConfigFile %sconaryrc' % rbuilderUrl)
            if cache:
                self._cfgCache[rbuilderUrl] = ccfg
        return ccfg

    def pre_start(self):
        self.addressGenerator = AddressGenerator(self.cfg.pairSubnet)
        self.loopManager = LoopManager(
                os.path.join(self.cfg.basePath, 'locks/loop'))
        self.proxyServer = ProxyServer(self.cfg.masterProxyPort, self._map,
                self)

    def run(self):
        log.info("Started with pid %d.", os.getpid())
        setDebugHook()
        try:
            self.serve_forever()
        finally:
            self.killHandlers()

    def killHandlers(self):
        handlers, self.handlers = self.handlers, {}
        for handler in handlers.values():
            handler.kill()

    # Node client machinery and entry points
    def onTimer(self):
        """
        Send jobmaster status to the dispatcher every 5 seconds.
        """
        self.nodeInfo.machineInfo.update()
        msg = messages.MasterStatusMessage()
        msg.set(self.nodeInfo)
        self.bus.sendMessage('/image_event', msg)

    def doResetCommand(self, msg):
        """
        Terminate all jobs, esp. after a dispatcher restart.
        """
        log.info("Terminating all jobs per dispatcher request.")
        self.killHandlers()

    def doJobCommand(self, msg):
        """
        Run a new image job.
        """
        job = msg.payload.job
        try:
            handler = jobhandler.JobHandler(self, job)
            self.proxyServer.addTarget(handler.network.slaveAddr, job.rbuilder_url)
            handler.start()
            self.handlers[job.uuid] = handler
        except:
            log.exception("Unhandled exception while starting job handler")
            self.removeJob(job, failed=True)

    def doStopCommand(self, msg):
        """Stop one running job."""
        uuid = msg.getUUID()
        if uuid in self.handlers:
            log.info("Stopping job %s", uuid)
            self.handlers[uuid].stop()
        else:
            log.info("Ignoring request to stop unknown job %s", uuid)

    def doSetSlotsCommand(self, msg):
        """Set the number of slots."""
        self.nodeInfo.slots = self.cfg.slaveLimit = int(msg.getSlots())
        log.info("Setting slot limit to %d.", self.cfg.slaveLimit)

        # Write the new value to file so it is preserved across restarts.
        cfgDir = os.path.join(self.cfg.basePath, 'config.d')
        if os.access(cfgDir, os.W_OK):
            fObj = open(cfgDir + '/99_runtime.conf', 'w')
            self.cfg.storeKey('slaveLimit', fObj)
            fObj.close()
        else:
            log.warning("Could not write new config in %s.", cfgDir)

    def handleRequestIfReady(self, sleepTime=1.0):
        bus_node.BusNode.handleRequestIfReady(self, sleepTime)
        # Check on all our subprocesses to make sure they are alive and reap
        # them if they are not.
        for handler in self.handlers.values():
            if not handler.check():
                self.handlerStopped(handler)
        for proc in self.subprocesses[:]:
            if not proc.check():
                self.subprocesses.remove(proc)

    def handlerStopped(self, handler):
        """
        Clean up after a handler has exited.
        """
        uuid = handler.job.uuid

        # If the handler did not exit cleanly, notify the rBuilder that the job
        # has failed.
        if handler.exitCode:
            log.error("Handler for job %s terminated unexpectedly", uuid)
            self.removeJob(handler.job, failed=True)
        else:
            self.removeJob(handler.job, failed=False)

        self.proxyServer.removeTarget(handler.network.slaveAddr)
        del self.handlers[uuid]

    def removeJob(self, job, failed=False):
        if failed:
            try:
                response = ResponseProxy(job.rbuilder_url,
                        json.loads(job.job_data))
                response.sendStatus(jobstatus.FAILED,
                        "Error creating build environment")
            except:
                log.exception("Unable to report failure for job %s", job.uuid)

        msg = messages.JobCompleteMessage()
        msg.set(job.uuid)
        self.bus.sendMessage('/image_event', msg)

    # Utility methods
    def clean_mounts(self):
        last = None
        while True:
            mounts = open('/proc/mounts').read().splitlines()
            tried = set()
            for mount in mounts:
                mount = mount.split()[1]
                for prefix in ('devfs', 'rootfs'):
                    if mount.startswith('/tmp/%s-' % prefix):
                        try:
                            util.call('umount ' + mount)
                            log.info("Unmounted %s", mount)
                            os.rmdir(mount)
                        except:
                            pass
                        tried.add(mount)
                        break

            if not tried:
                break

            if tried == last:
                log.warning("Failed to unmount these points: %s",
                        ' '.join(tried))
                break
            last = tried

        for lv_name in get_scratch_lvs(self.cfg.lvmVolumeName):
            log.info("Deleting LV %s/%s", self.cfg.lvmVolumeName, lv_name)
            util.call('lvremove -f %s/%s' % (self.cfg.lvmVolumeName, lv_name))

    def clean_roots(self):
        # Contents roots are no longer used; delete everything
        root = os.path.join(self.cfg.basePath, 'roots')
        for name in os.listdir(root):
            path = os.path.join(root, name)
            log.info("Deleting old contents root %s", name)
            rmtree(path)


def main(args):
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config-file', default=config.CONFIG_PATH)
    parser.add_option('-n', '--no-daemon', action='store_true')
    parser.add_option('--clean-mounts', action='store_true',
            help='Clean up stray mount points and logical volumes')
    parser.add_option('--clean-roots', action='store_true',
            help='Clean up old jobslave roots')
    options, args = parser.parse_args(args)

    cfg = config.MasterConfig()
    cfg.read(options.config_file)

    if options.clean_mounts or options.clean_roots:
        options.no_daemon = True

    level = cfg.getLogLevel()
    setupLogging(logPath=cfg.logPath, fileLevel=level, consoleFormat='file',
            consoleLevel=level if options.no_daemon else None)
    master = JobMaster(cfg)

    if options.clean_mounts:
        return master.clean_mounts()
    elif options.clean_roots:
        return master.clean_roots()
    elif options.no_daemon:
        master.pre_start()
        master.run()
        return 0
    else:
        master.pre_start()
        # Double-fork to daemonize
        pid = os.fork()
        if pid:
            return

        pid = os.fork()
        if pid:
            os._exit(0)

        try:
            os.setsid()
            devNull = os.open(os.devnull, os.O_RDWR)
            os.dup2(devNull, sys.stdout.fileno())
            os.dup2(devNull, sys.stderr.fileno())
            os.dup2(devNull, sys.stdin.fileno())
            os.close(devNull)

            fObj = open(cfg.pidFile, 'w')
            fObj.write(str(os.getpid()))
            fObj.close()

            master.run()

        finally:
            try:
                os.unlink(cfg.pidFile)
            finally:
                os._exit(0)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
