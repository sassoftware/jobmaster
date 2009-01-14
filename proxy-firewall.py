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
    for schema, uri in cfg.proxy.items():
        hostpart = urlparse.urlsplit(uri)[1]
        host, port = urllib.splitport(hostpart)
        if not port:
            if schema == 'https':
                port = '443'
            else:
                port = '80'
        os.system('/sbin/iptables -A FORWARD-SLAVE -m state --state NEW '
                '-m tcp -p tcp --dport %s -d %s -j ACCEPT 2>/dev/null'
                % (port, host))


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
