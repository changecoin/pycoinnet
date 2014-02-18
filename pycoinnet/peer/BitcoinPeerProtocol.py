import asyncio
import binascii
import logging
import os
import struct
import time
import weakref

from pycoin import encoding

from pycoinnet.message import parse_from_data, pack_from_data
from pycoinnet.message import MESSAGE_STRUCTURES
from pycoinnet.PeerAddress import PeerAddress
from pycoinnet.util.Queue import Queue


class BitcoinProtocolError(Exception):
    pass


class BitcoinPeerProtocol(asyncio.Protocol):

    MAX_MESSAGE_SIZE = 2*1024*1024

    def __init__(self, magic_header, *args, **kwargs):
        super(BitcoinPeerProtocol, self).__init__(*args, **kwargs)
        self.magic_header = magic_header
        self.peername = "(unconnected)"
        self.connection_was_lost = asyncio.Future()
        self._request_handle = None
        self.message_queues = weakref.WeakSet()
        ## stats
        self.bytes_read = 0
        self.bytes_writ = 0
        self.connect_start_time = None

    def new_get_next_message_f(self, filter_f=lambda message_name, data: True, maxsize=0):
        @asyncio.coroutine
        def run(self):
            while True:
                try:
                    message_name, data = yield from self._parse_next_message()
                except Exception:
                    logging.exception("error in _parse_next_message")
                    message_name = None
                for q in self.message_queues:
                    if q.filter_f(message_name, data):
                        q.put_nowait((message_name, data))
                if message_name is None:
                    break

        q = Queue(maxsize=maxsize)
        q.filter_f = filter_f
        self.message_queues.add(q)
        def get_next_message():
            msg_name, data = yield from q.get()
            if msg_name == None:
                raise self.connection_was_lost.exception()
            return msg_name, data
        if not self._request_handle:
            self._request_handle = asyncio.Task(run(self))
        return get_next_message

    def send_msg(self, message_name, **kwargs):
        message_data = pack_from_data(message_name, **kwargs)
        message_type = message_name.encode("utf8")
        message_type_padded = (message_type+(b'\0'*12))[:12]
        message_size = struct.pack("<L", len(message_data))
        message_checksum = encoding.double_sha256(message_data)[:4]
        packet = b"".join([
            self.magic_header, message_type_padded, message_size, message_checksum, message_data
        ])
        logging.debug("sending message %s [%d bytes]", message_type.decode("utf8"), len(packet))
        self.bytes_writ += len(packet)
        self.transport.write(packet)

    def connection_made(self, transport):
        self.transport = transport
        self.reader = asyncio.StreamReader()
        self._is_writable = True
        self.peername = transport.get_extra_info("socket").getpeername()
        self.connect_start_time = time.time()

    def connection_lost(self, exc):
        self.connection_was_lost.set_exception(exc)
        self._request_handle.cancel()

    def data_received(self, data):
        self.bytes_read += len(data)
        self.reader.feed_data(data)

    def pause_writing(self):
        self._is_writable = False

    def resume_writing(self):
        self._is_writable = True

    def is_writable(self):
        return self._is_writable

    @asyncio.coroutine
    def _parse_next_message(self):

        # read magic header
        reader = self.reader
        blob = yield from reader.readexactly(len(self.magic_header))
        if blob != self.magic_header:
            s = "bad magic: got %s" % binascii.hexlify(blob)
            raise BitcoinProtocolError(s)

        # read message name
        message_size_hash_bytes = yield from reader.readexactly(20)
        message_name_bytes = message_size_hash_bytes[:12]
        message_name = message_name_bytes.replace(b"\0", b"").decode("utf8")

        # get size of message
        size_bytes = message_size_hash_bytes[12:16]
        size = int.from_bytes(size_bytes, byteorder="little")
        if size > self.MAX_MESSAGE_SIZE:
            raise BitcoinProtocolError("absurdly large message size %d" % size)

        # read the hash, then the message
        transmitted_hash = message_size_hash_bytes[16:20]
        message_data = yield from reader.readexactly(size)

        # check the hash
        actual_hash = encoding.double_sha256(message_data)[:4]
        if actual_hash != transmitted_hash:
            s = "checksum is WRONG: %s instead of %s" % (
                binascii.hexlify(actual_hash), binascii.hexlify(transmitted_hash))
            raise BitcoinProtocolError(s)

        logging.debug("message %s: %s (%d byte payload)", self, message_name, len(message_data))

        # parse the blob into a BitcoinProtocolMessage object
        data = parse_from_data(message_name, message_data)
        return message_name, data

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "<Peer %s>" % str(self.peername)

    '''
    def default_msg_version_parameters(self):
        remote_ip, remote_port = self.peername
        remote_addr = PeerAddress(1, remote_ip, remote_port)
        local_addr = PeerAddress(1, "127.0.0.1", 6111)
        d = dict(
            version=70001, subversion=b"/Notoshi/", services=1, timestamp=int(time.time()),
            remote_address=remote_addr, local_address=local_addr,
            nonce=struct.unpack("!Q", os.urandom(8))[0],
            last_block_index=0, want_relay=True
        )
        return d

    def update_msg_version_parameters(self, d):
        """
        Use this method to override any parameters included in the initial
        version message (see messages.py). You obviously must call this before it's sent.
        """
        self.override_msg_version_parameters.update(d)

    def stop(self):
        self.transport.close()
        ## is this necessary?
        #self._request_handle.cancel()
    '''
