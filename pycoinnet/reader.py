import binascii
import ipaddress
import struct

from pycoin.block import Block
from pycoin.serialize import b2h_rev
from pycoin.serialize.bitcoin_streamer import parse_struct, stream_struct, BITCOIN_STREAMER
from pycoin.tx.Tx import Tx


class InvItem(object):
    def __init__(self, item_type, data):
        self.item_type = item_type
        self.data = data

    def __str__(self):
        INV_TYPES = [None, "Tx", "Block"]
        return "%s [%s]" % (INV_TYPES[self.item_type], b2h_rev(self.data))

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.data)

    def stream(self, f):
        stream_struct("L#", f, self.item_type, self.data)

    @classmethod
    def parse(self, f):
        return self(*parse_struct("L#", f))


IP4_HEADER = binascii.unhexlify("00000000000000000000FFFF")


class PeerAddress(object):
    def __init__(self, services, ip_int_or_str, port):
        ip_address = ipaddress.ip_address(ip_int_or_str)
        self.services = services
        self.ip_address = ip_address
        self.port = port

    def __repr__(self):
        return "%s/%d" % (self.ip_address, self.port)

    def stream(self, f):
        f.write(struct.pack("<Q", self.services))
        ip_bin = self.ip_address.packed
        if len(ip_bin) < 16:
            f.write(IP4_HEADER)
        f.write(ip_bin)
        f.write(struct.pack("!H", self.port))

    @classmethod
    def parse(self, f):
        services, ip_bin, port = parse_struct("Q@h", f)
        if ip_bin.startswith(IP4_HEADER):
            ip_bin = ip_bin[len(IP4_HEADER):]
        ip_int = int.from_bytes(ip_bin, byteorder="big")
        return self(services, ip_int, port)


def init_bitcoin_streamer():

    EXTRA_PARSING = [
        ("A", (PeerAddress.parse, lambda f, peer_addr: peer_addr.stream(f))),
        ("v", (InvItem.parse, lambda f, inv_item: inv_item.stream(f))),
        ("T", (Tx.parse, lambda f, tx: tx.stream(f))),
        ("B", (Block.parse, lambda f, block: block.stream(f))),
        ("b", (lambda f: struct.unpack("?", f.read(1))[0], lambda f, b: f.write(struct.pack("?", b)))),
    ]

    BITCOIN_STREAMER.register_functions(EXTRA_PARSING)
