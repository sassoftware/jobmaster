#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
