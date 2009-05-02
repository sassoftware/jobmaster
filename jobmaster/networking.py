#!/usr/bin/python
#
# Copyright (c) 2009 rPath, Inc.
#
# All rights reserved.
#

import random
import struct
import sys


class AddressGenerator(object):
    def __init__(self, subnet=None):
        if not subnet:
            subnet = self.generateSubnet()
        self.network, self.mask = subnet

    @staticmethod
    def generateSubnet():
        """
        Generate a random 48-bit network prefix in the RFC 4193
        (Unique Local Address) space.
        """
        address = 0xFD << 120
        address |= random.getrandbits(40) << 80
        return address, 48

    def generateHostPair(self):
        """
        Generate a pair of addresses on a /127 subnet.
        """
        randPart = 128 - self.mask
        subnet = self.network | random.getrandbits(randPart)
        parent = subnet & ~0x01
        child = subnet | 0x01
        return (parent, 127), (child, 127)


def parseIPv6(val):
    """
    Parse an IPv6 address C{val} into a long integer. The address may
    contain a mask in CIDR notation.

    Returns a tuple C{(address, mask)} where mask is assumed to be 128
    if no mask is given.
    """
    if '/' in val:
        address, mask = val.split('/')
        mask = int(mask)
        if not (0 <= mask <= 128):
            raise ValueError("Invalid IPv6 mask %d" % mask)
    else:
        address, mask = val, 128

    def _explode(substr):
        chunks = substr.split(':')
        if chunks == ['']:
            return []
        return chunks

    if address == '::':
        return 0L, mask
    elif address.count('::') > 1:
        raise ValueError("Too many omitted sections in address %r" % address)
    elif '::' in address:
        before, after = address.split('::')
        before = _explode(before)
        after = _explode(after)
        missing = 8 - len(before) - len(after)
        if missing < 0:
            raise ValueError("Too many sections in address %r" % address)
        chunks = before + ['0'] * missing + after
    else:
        chunks = _explode(address)
        if len(chunks) != 8:
            raise ValueError("Wrong number of sections in address %r"
                    % address)

    val = 0L
    for chunk in chunks:
        val <<= 16
        if len(chunk) > 4:
            raise ValueError("Chunk %r too large in address %r"
                    % (chunk, address))
        chunk = int(chunk, 16)
        assert (chunk & 0xFFFF) == chunk
        val |= chunk

    return val, mask


def formatIPv6(address, mask=None):
    """
    Format a long integer IPv6 C{address} as a string, possibly with a
    CIDR C{mask}.
    """
    chunks = []
    for n in range(8):
        chunks.insert(0, '%x' % (address & 0xFFFF))
        address >>= 16

    candidates = []
    i = 0
    while i < 8:
        # Count how many consecutive zero chunks there are
        start = i
        while i < 8 and chunks[i] == '0':
            i += 1
        end = i
        candidates.append((end - start, start))
        i += 1

    # Remove whichever run is the longest
    count, start = max(candidates)
    if count:
        if start == 0:
            chunks[:count] = ['', '']
        elif start + count == 8:
            chunks[start:] = ['', '']
        else:
            chunks[start:start+count] = ['']

    out = ':'.join(chunks)
    if mask is not None:
        out += '/%d' % mask
    return out


def main(args):
    if len(args) not in (0, 1, 2):
        sys.exit('Usage: %s [ count { subnet | - } ]]' % sys.argv[0])

    count = 1
    if args:
        count = int(args.pop(0))

    subnet = None
    if args:
        subnet = args.pop(0)
        if subnet == '-':
            for n in range(count):
                print formatIPv6(*AddressGenerator.generateSubnet())
            return
        subnet = parseIPv6(subnet)

    random.seed()
    generator = AddressGenerator(subnet)
    for n in range(count):
        one, two = generator.generateHostPair()
        print formatIPv6(*one)
        print formatIPv6(*two)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
