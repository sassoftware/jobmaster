#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved
#

"""
Defines several "resources" -- objects that are closed in LIFO order on
error or at the end of the task.

Typical resources include LVM volumes, mount points, and virtual machines.
These need to be cleaned up in reverse order, e.g. first stop the VM, then
unmount its disk, then destroy the disk. So, one would create a stack of
resources (e.g. via C{ResourceStack}), push each resource onto the stack as
it is allocated, and pop each resource on shutdown to free it.
"""

import logging

log = logging.getLogger(__name__)


class Resource(object):
    """
    Base class for some sort of "resource" that must be freed both
    when done and when unwinding the stack (on exception).

    Typically, one would keep a stack of these, and close them in
    reverse order at the end of the section.
    """
    closed = False

    def close(self):
        """
        Close the resource if it is not already closed.
        """
        if not self.closed:
            self._close()
            self.closed = True
    __del__ = close

    def _close(self):
        "Override this to add cleanup functionality."

    def release(self):
        """
        Release the resource by marking it as closed without actually
        destroying it, e.g. after a sucessful preparatory section.
        """
        if not self.closed:
            self._release()
            self.closed = True

    def _release(self):
        "Override this to add extra on-release handling."


class ResourceStack(Resource):
    """
    A stack of resources that itself acts as a resource.
    """
    resources = None

    def __init__(self, resources=None):
        Resource.__init__(self)
        if resources:
            self.resources = resources
        else:
            self.resources = []

    def append(self, resource):
        """
        Add a new C{resource} to the top of the stack.
        """
        self.resources.append(resource)

    def _close(self):
        """
        Close each resource in LIFO order.
        """
        while self.resources:
            self.resources.pop().close()

    def _release(self):
        """
        Release each resource in LIFO order.
        """
        while self.resources:
            self.resources.pop().release()
