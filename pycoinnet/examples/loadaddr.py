#!/usr/bin/env python

import asyncio
import binascii
import logging

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.helpers.standards import default_msg_version_parameters
from pycoinnet.helpers.standards import initial_handshake
from pycoinnet.helpers.standards import get_date_address_tuples


TESTNET_MAGIC_HEADER = binascii.unhexlify('0B110907')
MAINNET_MAGIC_HEADER = binascii.unhexlify('F9BEB4D9')

@asyncio.coroutine
def run():
    host = "127.0.0.1"
    port = 28333
    transport, peer = yield from asyncio.get_event_loop().create_connection(
        lambda: BitcoinPeerProtocol(MAINNET_MAGIC_HEADER), host=host, port=port)

    remote_version_data = yield from initial_handshake(peer, default_msg_version_parameters(peer))

    date_address_tuples = yield from get_date_address_tuples(peer)
    date_address_tuples = sorted(date_address_tuples, key=lambda dat: -dat[0])
    print(date_address_tuples)

def main():
    logging.basicConfig(level=logging.DEBUG,
        format=('%(asctime)s [%(process)d] [%(levelname)s] '
                '%(filename)s:%(lineno)d %(message)s'))
    asyncio.get_event_loop().run_until_complete(run())

main()
