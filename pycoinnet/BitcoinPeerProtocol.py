import asyncio.queues
import binascii
import datetime
import io
import logging
import os
import struct
import time

from pycoin import encoding
from pycoin.serialize import bitcoin_streamer
from pycoinnet.BitcoinPeerStream import BitcoinPeerStreamReader
from pycoinnet.reader import init_bitcoin_streamer
from pycoinnet.reader.PeerAddress import PeerAddress

init_bitcoin_streamer()


class BitcoinPeerProtocol(asyncio.Protocol):

    def __init__(self, magic_header=binascii.unhexlify('0B110907'), *args, **kwargs):
        super(BitcoinPeerProtocol, self).__init__(*args, **kwargs)
        self.magic_header = magic_header

    def connection_made(self, transport):
        logging.debug("connection made %s", transport)
        self.transport = transport
        self.reader = BitcoinPeerStreamReader()
        self.messages = asyncio.queues.Queue()
        self._request_handle = asyncio.async(self.start())

    def data_received(self, data):
        self.reader.feed_data(data)

    def send_message(self, message_type, message_data=b''):
        message_type_padded = (message_type+(b'\0'*12))[:12]
        message_size = struct.pack("<L", len(message_data))
        message_checksum = encoding.double_sha256(message_data)[:4]
        packet = b"".join([self.magic_header, message_type_padded, message_size, message_checksum, message_data])
        logging.debug("sending message %s [%d bytes]", message_type.decode("utf8"), len(packet))
        self.transport.write(packet)

    @asyncio.coroutine
    def start(self):
        while True:
            try:
                yield from self.parse_next_message()
            except Exception:
                logging.exception("message parse failed")

    def parse_next_message(self):
        message_name, message_data = yield from self.reader.read_message(self.magic_header)

        logging.debug("message: %s (%d byte payload)", message_name, len(message_data))

        def version_supplement(d):
            d["when"] = datetime.datetime.fromtimestamp(d["timestamp"])

        def alert_supplement(d):
            d["alert_msg"] = bitcoin_streamer.parse_as_dict(
                "version relayUntil expiration id cancel setCancel minVer maxVer"
                " setSubVer priority comment statusBar reserved".split(),
                "LQQLLSLLSLSSS",
                d["payload"])

        PARSE_PAIR = {
            'version': (
                "version node_type timestamp remote_address local_address"
                " nonce subversion last_block_index",
                "LQQAAQSL",
                version_supplement
            ),
            'verack': ("", ""),
            'inv': ("items", "[v]"),
            'getdata': ("items", "[v]"),
            'addr': ("date_address_tuples", "[LA]"),
            'alert': ("payload signature", "SS", alert_supplement),
            'tx': ("tx", "T"),
            'block': ("block", "B"),
            'ping': ("nonce", "Q"),
            'pong': ("nonce", "Q"),
        }

        the_tuple = PARSE_PAIR.get(message_name)
        if the_tuple is None:
            logging.error("unknown message: %s %s", message_name, binascii.hexlify(message_data))
        else:
            prop_names, prop_struct = the_tuple[:2]
            post_f = lambda d: 0
            if len(the_tuple) > 2:
                post_f = the_tuple[2]
            d = bitcoin_streamer.parse_as_dict(
                prop_names.split(), prop_struct, io.BytesIO(message_data))
            post_f(d)
            yield from self.messages.put((message_name, d))

    def next_message(self):
        return self.messages.get()

    def send_msg_version(
        self, version, subversion, node_type, current_time,
            remote_address, remote_listen_port, local_address, local_listen_port, nonce, last_block_index):
        remote = PeerAddress(1, remote_address, remote_listen_port)
        local = PeerAddress(1, local_address, local_listen_port)
        the_bytes = bitcoin_streamer.pack_struct(
            "LQQAAQSL", version, node_type, current_time,
            remote, local, nonce, subversion, last_block_index)
        self.send_message(b"version", the_bytes)

    def send_msg_verack(self):
        self.send_message(b"verack")

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
