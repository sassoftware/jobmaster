#!/usr/bin/python

"""
Set the firewall to allow access to configured HTTP(S) proxies.
This is only necessary until rBuilder handles the EC2 image posting
and registration process.
"""
import os, sys, urllib, urlparse
from conary.conarycfg import ConaryConfiguration


def main(args):
    cfg = ConaryConfiguration(False)
    cfg.read('/etc/conaryrc', exception=False)

    holes = set()
    for schema, uri in cfg.proxy.items():
        userhostport = urlparse.urlsplit(uri)[1]
        hostport = urllib.splituser(userhostport)[1]
        host, port = urllib.splitport(hostport)
        if not port:
            if schema == 'https':
                port = '443'
            else:
                port = '80'
        holes.add((host, port))

    if os.system('/sbin/iptables -F FORWARD-PROXY'):
        print >> sys.stderr, "Failed to flush existing proxy chain"
        return 1

    for host, port in holes:
        os.system('/sbin/iptables -A FORWARD-PROXY -m state --state NEW '
                '-m tcp -p tcp --dport %s -d %s -j ACCEPT' % (port, host))


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
