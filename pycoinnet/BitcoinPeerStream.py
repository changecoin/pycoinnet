import binascii
import logging
import struct

import asyncio

from pycoin import encoding


class BitcoinProtocolError(Exception):
    pass


class BitcoinPeerStreamReader(asyncio.StreamReader):
    MAX_MESSAGE_SIZE = 20*1024*1024

    @asyncio.coroutine
    def read_message(self, expected_magic):
        blob = yield from self.readexactly(len(expected_magic))
        if blob != expected_magic:
            s = "bad magic: got %s" % binascii.hexlify(blob)
            logging.error(s)
            raise BitcoinProtocolError(s)
        message_name = (yield from self.readexactly(12)).replace(b"\0", b"")
        blob = yield from self.readexactly(4)
        size = int.from_bytes(blob, byteorder="little")
        if size > self.MAX_MESSAGE_SIZE:
            raise BitcoinProtocolError("absurdly large message size %d" % size)
        transmitted_hash = yield from self.readexactly(4)
        message_data = yield from self.readexactly(size)
        actual_hash = encoding.double_sha256(message_data)[:4]
        if actual_hash == transmitted_hash:
            logging.debug("checksum is CORRECT")
        else:
            s = "checksum is WRONG: %s instead of %s" % (
                binascii.hexlify(actual_hash), binascii.hexlify(transmitted_hash))
            logging.error(s)
            raise BitcoinProtocolError(s)
        return message_name.decode("utf8"), message_data
