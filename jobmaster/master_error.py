#
# Copyright (c) 2005-2006 rPath, Inc.
#
# All rights reserved
#

class MCPError(Exception):
    pass

class ProtocolError(MCPError):
    def __init__(self, msg = "Protocol Error"):
        self.msg = msg
    def __str__(self):
        return self.msg
