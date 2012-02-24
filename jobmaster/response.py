#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

"""
Communicate status and artifacts back to the parent rBuilder.
"""

import logging
import restlib.client
try:
    from xml.etree import ElementTree as ET
except ImportError:
    from elementtree import ElementTree as ET

log = logging.getLogger(__name__)


class ResponseProxy(object):
    def __init__(self, masterUrl, jobData):
        self.imageBase = '%sapi/v1/images/%d' % (masterUrl, jobData['buildId'])
        self.outputToken = jobData['outputToken']

    def _post(self, method, path, contentType='application/xml', body=None):
        headers = {
                'Content-Type': contentType,
                'X-rBuilder-OutputToken': self.outputToken,
                }
        if path is None:
            url = self.imageBase
        else:
            url = "%s/%s" % (self.imageBase.rstrip('/'), path)

        client = restlib.client.Client(url, headers)
        client.connect()
        return client.request(method, body)

    def sendStatus(self, code, message):
        root = ET.Element('image')
        ET.SubElement(root, "status").text = str(code)
        ET.SubElement(root, "status_message").text = message
        try:
            self._post('PUT', path=None, body=ET.tostring(root))
        except:
            log.exception("Failed to send status upstream")
