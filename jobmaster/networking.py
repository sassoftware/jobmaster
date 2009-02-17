import random
import struct


class AddressGenerator(object):
    def __init__(self, subnet):
        if isinstance(subnet, basestring):
            subnet = parseIPv6(subnet)
        network, mask = subnet
        if mask > 64:
            raise RuntimeError("Subnet mask of %d bits is too high; "
                    "need 64 or less." % mask)
        elif mask < 0:
            raise RuntimeError("Invalid subnet mask %d" % mask)

        # Make sure the hostpart of the address is zeroed
        self.network = network & ~((1 << (128 - mask)) - 1)
        self.mask = mask

    def generateHost(self):
        """
        Generate a random MAC-48/IPv6 address pair from the selected
        subnet.
        """
        mac = self.generateMAC()
        ipv6 = self.network | self.mac48ToEUI64(mac)
        return formatMAC(mac), formatIPv6(ipv6, self.mask)

    @staticmethod
    def generateSubnet():
        """
        Generate a random 48-bit network prefix in the RFC 4193
        (Unique Local Address) space.
        """
        address = 0xFD << 120
        address |= random.getrandbits(40) << 80
        return address, 48

    @staticmethod
    def generateMAC():
        """Generate a random MAC-48 in the Locally Administered space."""
        val = random.getrandbits(48)
        # Clear the LSB of the first octet to indicate that this is an
        # individual address (as opposed to a group address like
        # broadcast).
        val &= 0xFEFFFFFFFFFF
        # Set the second-LSB of the first octet to indicate that this
        # is a locally-administered address, and therefore has no OUI.
        val |= 0x020000000000
        return val

    @staticmethod
    def mac48ToEUI64(val):
        """Convert a MAC-48 to a EUI-64 as per RFC 4291 appx. A."""
        # Insert 0xFFFE in the middle of the address
        val = ((val & 0xFFFFFF000000) << 16) | (val & 0xFFFFFF)
        val |= 0xFFFE << 24
        # Invert the universal/local bit
        localMask = 0x020000000000
        return (val & ~localMask) | (~(val & localMask) & localMask)


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


def formatMAC(address, bits=48):
    """
    Format a long integer MAC or EUI C{address} in the standard
    notation.
    """
    out = []
    for n in range(0, bits / 8):
        out.insert(0, '%02x' % (address & 0xFF))
        address >>= 8
    return ':'.join(out)
