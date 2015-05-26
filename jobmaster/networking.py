#!/usr/bin/python
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


import random


class AddressGenerator(object):
    def __init__(self, subnet=None):
        if isinstance(subnet, basestring):
            self.subnet = Address.parse(subnet)
        elif isinstance(subnet, Address):
            self.subnet = subnet
        elif subnet is not None:
            raise TypeError("subnet must be a string or Address object")
        else:
            self.subnet = subnet or self.generateSubnet()
        assert self.subnet.mask <= 48

    @staticmethod
    def generateSubnet():
        """
        Generate a random 48-bit network prefix in the RFC 4193
        (Unique Local Address) space.
        """
        address = 0xFD << 120
        address |= random.getrandbits(40) << 80
        return Address(address, 48)

    def generateHostPair(self):
        """
        Generate a pair of addresses on a /127 subnet.
        """
        randPart = 128 - self.subnet.mask
        subnet = self.subnet.address | random.getrandbits(randPart)
        parent = subnet & ~0x01
        child = subnet | 0x01
        return Address(parent, 127), Address(child, 127)


class Address(object):
    def __init__(self, address, mask):
        self.address = address
        self.mask = mask

    def __repr__(self):
        return 'Address(%s, %s)' % (self.address, self.mask)

    def __str__(self):
        return self.format(True)

    @classmethod
    def parse(cls, val):
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
            return cls(0L, mask)
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

        return cls(val, mask)

    def format(self, useMask=True):
        """
        Format the address a string, possibly with a CIDR mask.
        """
        address = self.address
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
        if useMask:
            out += '/%d' % self.mask
        return out


def test():
    import sys
    args = sys.argv[1:]
    if args:
        subnet = args.pop(0)
        if subnet == '-':
            print AddressGenerator.generateSubnet()
            return
        subnet = Address.parse(subnet)
    else:
        subnet = None

    gen = AddressGenerator(subnet)
    if args:
        count = int(args.pop(0))
    else:
        count = 1
    for n in range(count):
        print gen.generateHostPair()[0]


if __name__ == '__main__':
    test()
