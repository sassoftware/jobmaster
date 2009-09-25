#/usr/bin/python
#
# Copyright (c) 2005-2009 rPath, Inc.
#
# All rights reserved.
#

import logging
import os
import sys
from conary import conarycfg
from conary import conaryclient
from mcp import image_job
from mcp.messagebus import bus_node
from mcp.messagebus import messages
from mcp.messagebus import nodetypes
from rmake.lib import procutil

from jobmaster import config
from jobmaster import jobhandler
from jobmaster import util

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
        log.close = lambda: None
        bus_node.BusNode.__init__(self, (cfg.queueHost, cfg.queuePort),
                nodeInfo=node, logger=log)
        self.cfg = cfg
        self.handlers = {}

        self.conaryCfg = conarycfg.ConaryConfiguration(True)
        self.conaryCfg.initializeFlavors()
        self.conaryCfg.configLine('conaryProxy http %sconary/'
                % cfg.rbuilderUrl)
        self.conaryCfg.configLine('conaryProxy https %sconary/'
                % cfg.rbuilderUrl)
        self.conaryClient = conaryclient.ConaryClient(self.conaryCfg)

        log.info("Jobmaster %s started with pid %d.", self.bus.getSessionId(),
                os.getpid())

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
        for handler in self.handlers.values():
            handler.kill()
        self.handlers = {}

    def doJobCommand(self, msg):
        """
        Run a new image job.
        """
        job = msg.payload.job
        handler = self.handlers[job.uuid] = jobhandler.JobHandler(self, job)
        handler.start()

    def handleRequestIfReady(self, sleepTime):
        bus_node.BusNode.handleRequestIfReady(self, sleepTime)
        for handler in self.handlers.values():
            if not handler.check():
                self.handlerStopped(handler)

    def handlerStopped(self, handler):
        """
        Clean up after a handler has exited.
        """
        # Notify the dispatcher that the job is done.
        uuid = handler.job.uuid
        msg = messages.JobCompleteMessage()
        msg.set(uuid)
        self.bus.sendMessage('/image_event', msg)

        del self.handlers[uuid]


def main(cfg=None):
    if not cfg:
        cfg = config.MasterConfig()
    util.setupLogging(cfg.logLevel)
    master = JobMaster(cfg)
    master.serve_forever()

def runDaemon():
    cfg = config.MasterConfig()
    cfg.read(config.CONFIG_PATH)

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

        main(cfg)

        os.unlink(cfg.pidFile)
    finally:
        os._exit(0)


if __name__ == '__main__':
    main()
