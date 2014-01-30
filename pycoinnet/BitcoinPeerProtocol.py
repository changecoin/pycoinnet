import asyncio
import binascii
import logging
import os
import struct
import time

from pycoin import encoding
from pycoin.serialize import bitcoin_streamer

from pycoinnet.message import parse_from_data, pack_from_data
from pycoinnet.message import MESSAGE_STRUCTURES
from pycoinnet.PeerAddress import PeerAddress


ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)


class BitcoinProtocolError(Exception):
    pass

"""
class BitcoinPeerProtocol(asyncio.Protocol):

    ## BRAIN DAMAGE: automate these
    def send_msg_version(
            self, version, subversion, services, current_time, remote_address,
            remote_listen_port, local_address, local_listen_port, nonce, last_block_index, want_relay):
        remote = PeerAddress(1, remote_address, remote_listen_port)
        local = PeerAddress(1, local_address, local_listen_port)
        the_bytes = bitcoin_streamer.pack_struct(
            "LQQAAQSL", version, services, current_time,
            remote, local, nonce, subversion, last_block_index)
        self.send_message(b"version", the_bytes)

    def send_msg_verack(self):
        self.send_message(b"verack")

    def send_msg_mempool(self):
        self.send_message(b"mempool")

    def send_msg_getaddr(self):
        self.send_message(b"getaddr")

    def send_msg_getdata(self, items):
        the_bytes = bitcoin_streamer.pack_struct("I" + ("v" * len(items)), len(items), *items)
        self.send_message(b"getdata", the_bytes)

    def send_msg_ping(self, nonce):
        nonce_data = struct.pack("<Q", nonce)
        self.send_message(b"ping", nonce_data)

    def send_msg_pong(self, nonce):
        nonce_data = struct.pack("<Q", nonce)
        self.send_message(b"pong", nonce_data)
"""

HANDLE_MESSAGE_NAMES = ["msg_%s" % msg_name for msg_name in MESSAGE_STRUCTURES.keys()]
HANDLE_MESSAGE_NAMES.extend(["connection_made", "connection_lost"])

MAX_MESSAGE_SIZE = 20*1024*1024

class BitcoinPeerProtocol(asyncio.Protocol):

    def get_msg_version_parameters(self):
        # this must return a dictionary with:
        #  version (integer)
        #  subversion (bytes, like b"/Satoshi:0.7.2/")
        #  services (a mask, set to 1 for now)
        #  timestamp (seconds since epoch)
        #  remote_address
        #  remote_listen_port
        #  local_address
        #  local_listen_port
        #  nonce (32 bit)
        #  last_block_index
        #  want_relay
        remote_ip, remote_port = self.transport.get_extra_info("socket").getpeername()
        remote_addr = PeerAddress(1, remote_ip, remote_port)
        local_addr = PeerAddress(1, "127.0.0.1", 6111)
        d = dict(
            version=70001,
            subversion=b"/Notoshi/",
            services=1,
            timestamp=int(time.time()),
            remote_address=remote_addr,
            local_address=local_addr,
            nonce=struct.unpack("!Q", os.urandom(8))[0],
            last_block_index=0,
            want_relay=True
        )
        return d

    def __init__(self, magic_header=binascii.unhexlify('0B110907'), *args, **kwargs):
        super(BitcoinPeerProtocol, self).__init__(*args, **kwargs)
        self.magic_header = magic_header
        self.delegate_methods = dict((event, []) for event in HANDLE_MESSAGE_NAMES)

    def register_delegate(self, delegate):
        for event in HANDLE_MESSAGE_NAMES:
            method_name = "handle_%s" % event
            if hasattr(delegate, method_name):
                self.delegate_methods[event].append(getattr(delegate, method_name))

    def unregister_delegate(self, delegate):
        for event in HANDLE_MESSAGE_NAMES:
            method_name = "handle_%s" % event
            if hasattr(delegate, method_name):
                self.delegate_methods[event].remove(getattr(delegate, method_name))

    def connection_made(self, transport):
        self.transport = transport
        self.reader = asyncio.StreamReader()
        #self.writer = asyncio.StreamWriter() ## use this someday so we get flow control etc.
        self.last_message_timestamp = time.time()
        self._request_handle = asyncio.async(self.run())
        self.trigger_event("connection_made", dict(transport=transport))

    def connection_lost(self, exc):
        self._request_handle.cancel()
        self.trigger_event("connection_lost", dict(exc=exc))

    def data_received(self, data):
        self.reader.feed_data(data)

    def send_message(self, message_type, message_data=b''):
        message_type_padded = (message_type+(b'\0'*12))[:12]
        message_size = struct.pack("<L", len(message_data))
        message_checksum = encoding.double_sha256(message_data)[:4]
        packet = b"".join([self.magic_header, message_type_padded, message_size, message_checksum, message_data])
        logging.debug("sending message %s [%d bytes]", message_type.decode("utf8"), len(packet))
        self.transport.write(packet)

    def send_msg(self, message_name, **kwargs):
        data = pack_from_data(message_name, **kwargs)
        self.send_message(message_name.encode("utf8"), data)

    @asyncio.coroutine
    def run(self):
        try:
            # do handshake

            self.is_running = True

            ## BRAIN DAMAGE: this should be better
            d = self.get_msg_version_parameters()
            self.send_msg("version", **d)

            message_name, data = yield from self.parse_next_message()
            if message_name != 'version':
                raise BitcoinProtocolError("missing version")
            self.trigger_event("msg_%s" % message_name, data)
            self.send_msg("verack")

            message_name, data = yield from self.parse_next_message()
            if message_name != 'verack':
                raise BitcoinProtocolError("missing verack")
            self.trigger_event("msg_%s" % message_name, data)

            while self.is_running:
                message_name, data = yield from self.parse_next_message()
                self.trigger_event("msg_%s" % message_name, data)
                self.last_message_timestamp = time.time()

        except Exception:
            logging.exception("message parse failed")

        self.transport.close()

    @asyncio.coroutine
    def parse_next_message(self):

        # read magic header
        reader = self.reader
        blob = yield from reader.readexactly(len(self.magic_header))
        if blob != self.magic_header:
            s = "bad magic: got %s" % binascii.hexlify(blob)
            logging.error(s)
            raise BitcoinProtocolError(s)

        # read message name
        message_name_bytes = yield from reader.readexactly(12)
        message_name = message_name_bytes.replace(b"\0", b"").decode("utf8")

        # get size of message
        size_bytes = yield from reader.readexactly(4)
        size = int.from_bytes(size_bytes, byteorder="little")
        if size > MAX_MESSAGE_SIZE:
            raise BitcoinProtocolError("absurdly large message size %d" % size)

        # read the hash, then the message
        transmitted_hash = yield from reader.readexactly(4)
        message_data = yield from reader.readexactly(size)

        # check the hash
        actual_hash = encoding.double_sha256(message_data)[:4]
        if actual_hash == transmitted_hash:
            logging.debug("checksum is CORRECT")
        else:
            s = "checksum is WRONG: %s instead of %s" % (
                binascii.hexlify(actual_hash), binascii.hexlify(transmitted_hash))
            logging.error(s)
            raise BitcoinProtocolError(s)

        logging.debug("message: %s (%d byte payload)", message_name, len(message_data))

        # parse the blob into a BitcoinProtocolMessage object
        data = parse_from_data(message_name, message_data)
        return message_name, data

    def trigger_event(self, event, data):
        if event in self.delegate_methods:
            methods = self.delegate_methods.get(event)
            if methods:
                for m in methods:
                    m(self, **data)
        else:
            logging.error("unknown event %s", event)

    def stop(self):
        self.transport.close()
        ## is this necessary?
        #self._request_handle.cancel()

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "<Peer %s>" % str(self.transport.get_extra_info("socket").getpeername())
