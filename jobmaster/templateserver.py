import BaseHTTPServer
import cgi
import cPickle
import fcntl
import os
import SimpleHTTPServer
import socket
import SocketServer
import subprocess
import tempfile
import threading
import urlparse

from conary.errors import TroveNotFound

from jobmaster import templategen

LISTEN_PORT = 8000
TIMEOUT = 3

class ServerStopped(Exception):
    pass

class TemplateServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    templateRoot = os.getcwd()
    hostname = ''
    port = 0
    tmpDir = '/var/tmp'

    def do_POST(self):
        if self.path == '/makeTemplate':
            self.makeTemplate()
        else:
            self.send_error(400) # HTTP 1.x / Bad Request

    def makeTemplate(self):
        try:
            length = self.headers.getheader('content-length')
            paramStr = self.rfile.read(int(length))
            v = cgi.parse_qs(paramStr).get('v')
            f = cgi.parse_qs(paramStr).get('f')
            if not (v and f):
                self.send_error(400) # HTTP 1.x / Bad Request

            v = v[0]
            f = f[0]

            try:
                at = templategen.AnacondaTemplate(v, f, self.templateRoot,
                        self.tmpDir)
            except TroveNotFound:
                self.send_error(404) # HTTP 1.x / Not Found
                return

            statusURI = "http://%s:%s/status?h=%s" % \
                    (self.hostname, self.port, at.getFullTroveSpecHash())
            templateURI = "http://%s:%s/%s" % \
                    (self.hostname, self.port, at.getFullTroveSpecHash())

            isRunning, status = at.status()
            entityBody = None
            if at.exists():
                self.send_response(303) # HTTP 1.x / See Also
                self.send_header("Location", templateURI)
            elif isRunning:
                self.send_response(303) # HTTP 1.x / See Also
                self.send_header("Location", statusURI)
            else:
                pid = os.fork()
                if not pid:
                    # close the socket as the child won't be talking
                    # back to the webserver
                    os.close(self.rfile.fileno())

                    # kill the old anaconda template object reference
                    # (the parent will be dumping his ref, and you need
                    # one with a fresh conaryclient, anyways)
                    del at

                    # get a new anaconda template object (with a new tmpdir)
                    at = templategen.AnacondaTemplate(v, f,
                            self.templateRoot, self.tmpDir)

                    # start the generation
                    os._exit(at.generate())

                # parent sends back a response
                self.send_response(202) # HTTP 1.x / Accepted
                self.send_header("X-Full-Trove-Spec-Hash",
                        at.getFullTroveSpecHash())
                self.send_header("Location", statusURI)

                entityBody = "<html><head><title>Template Generator</title></head><body><p>Template: %s</p><p>See <a href=\"%s\">%s</a> for generation status.</p></body></html>" % (at.getFullTroveSpec(), statusURI, statusURI)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", len(entityBody))

            self.end_headers()
            if entityBody:
                self.wfile.write(entityBody)

        except Exception, e:
            self.send_error(500, str(e))
            return

    def status(self):
        paramStr = urlparse.urlparse(self.path)[4]
        h = cgi.parse_qs(paramStr).get('h')
        if not h:
            self.send_error(400) # HTTP 1.x / Bad Request
            return

        h = h[0]

        statusPath = os.path.join(self.templateRoot,
                '.%s.status' % h)
        tarballPath = os.path.join(self.templateRoot,
                '%s.tar' % h)
        metadataPath = os.path.join(self.templateRoot,
                '.%s.metadata' % h)

        try:
            f = open(statusPath, 'rb')
        except IOError:
            # If the status file isn't there, check to see if the
            # template and metadata exist. If so, instruct the client
            # to go get the damn template.
            if os.path.exists(tarballPath) and os.path.exists(metadataPath):
                self.send_response(303) # HTTP 1.x / See Also
                self.send_header("Location", "http://%s:%s/%s" % \
                    (self.hostname, self.port, h))
                self.end_headers()
            else:
                self.send_error(404, "No such template")
            return None

        # Status will be plain text
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header("Content-Length", str(os.fstat(f.fileno())[6]))
        self.end_headers()

        return f

    def send_head(self):
        f = None

        # different handler for status requests
        if urlparse.urlparse(self.path)[2] == '/status':
            return self.status()

        hash = self.path.split('/')[-1]
        tarballPath = os.path.join(self.templateRoot, '%s.tar' % hash)
        metadataPath = os.path.join(self.templateRoot, '.%s.metadata' % hash)

        try:
            # Always read in binary mode. Opening files in text mode may cause
            # newline translations, making the actual size of the content
            # transmitted *less* than the content-length!
            f = open(tarballPath, 'rb')
        except IOError:
            self.send_error(404, "Template not found")
            return None

        if self.command.upper() == 'HEAD':
            self.send_response(204) # OK, no entity body
        else:
            self.send_response(200)

        # We will *always* stream a tarball back
        self.send_header("Content-Type", "application/x-tar")
        self.send_header("Content-Length", str(os.fstat(f.fileno())[6]))

        # Emit headers from metadata file
        m = None
        try:
            m = open(metadataPath, 'r')
            metadata = cPickle.load(m)
            for k, v in metadata.items():
                h = 'X-%s' % k.replace('_','-')
                self.send_header(h, str(v))
        finally:
            if m:
                m.close()

        self.end_headers()

        return f

class TemplateServer(threading.Thread, SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer):

    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self)
        BaseHTTPServer.HTTPServer.__init__(self, *args, **kwargs)
        self.running = True
        self.lock = threading.RLock()
        self.socket.settimeout(TIMEOUT)

    def get_request(self):
        running = True
        while running:
            # Using exceptions like this is tacky. if there were a better method
            # we'd be using it. that's why the socket timeout is in seconds.
            try:
                return self.socket.accept()
            except socket.timeout:
                self.lock.acquire()
                running = self.running
                self.lock.release()
        # we implement a custom defined exception so that we're guaranteed to
        # skip the entire call stack and safely abort in the thread's run method
        raise ServerStopped

    def run(self):
        try:
            while True:
                self.handle_request()
        except ServerStopped:
            pass

    def stop(self):
        self.lock.acquire()
        self.running = False
        self.lock.release()
        self.join()

def getServer(templateRoot, hostname='127.0.0.1', port=LISTEN_PORT,
        tmpDir='/var/tmp'):
    # due to the roundabout mechanisms here we need to modify the actual class
    # definition. This means only one imgserver instance can exist at a time.
    # unless all imgservers can agree on what the base path is.
    TemplateServerHandler.templateRoot = templateRoot
    TemplateServerHandler.hostname = hostname
    TemplateServerHandler.port = port
    TemplateServerHandler.tmpDir = tmpDir
    for port in range(port, port + 100):
        try:
            server = TemplateServer(('', port), TemplateServerHandler)
        except socket.error, e:
            if e.args[0] != 98:
                raise
        else:
            break
    return server


if __name__ == '__main__':
    import time
    try:
        foo = getServer('/tmp/anaconda-templates2')
        foo.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        foo.stop()
