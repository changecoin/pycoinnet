#!/usr/bin/env python

"""
Custom bitcoin client
"""

import argparse
import asyncio
import binascii
import logging


from pycoinnet.util.BlockChain import BlockChain
from pycoinnet.util.LocalDB_RAM import LocalDB
from pycoinnet.util.PetrifyDB_RAM import PetrifyDB_RAM as PetrifyDB

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol

from pycoinnet.peergroup.FastForward import fast_forwarder_add_peer_f
from pycoinnet.peergroup.ConnectionManager import ConnectionManager

from pycoinnet.helpers.standards import default_msg_version_parameters
from pycoinnet.helpers.standards import initial_handshake
from pycoinnet.helpers.standards import install_ping_manager
from pycoinnet.helpers.standards import install_pong_manager

from pycoinnet.util.Queue import Queue

MAINNET_MAGIC_HEADER = binascii.unhexlify('F9BEB4D9')
TESTNET_MAGIC_HEADER = binascii.unhexlify('0B110907')


@asyncio.coroutine
def run_peer(peer, fast_forward_add_peer):
    yield from asyncio.wait_for(peer.did_connection_made, timeout=None)
    version_parameters = default_msg_version_parameters(peer)
    version_data = yield from initial_handshake(peer, version_parameters)
    last_block_index = version_data["last_block_index"]
    fast_forward_add_peer(peer, last_block_index)

def run():
    ADDRESS_QUEUE = Queue(maxsize=20)
    ADDRESS_QUEUE.put_nowait(("127.0.0.1", 28333))

    local_db = LocalDB()
    petrify_db = PetrifyDB(b'\0'*32)
    block_chain = BlockChain(local_db, petrify_db)
    fast_forward_add_peer = fast_forwarder_add_peer_f(block_chain)

    def create_protocol_callback():
        peer = BitcoinPeerProtocol(MAINNET_MAGIC_HEADER)
        install_ping_manager(peer)
        install_pong_manager(peer)
        asyncio.Task(run_peer(peer, fast_forward_add_peer))
        return peer

    cm = ConnectionManager(ADDRESS_QUEUE, create_protocol_callback)
    cm.run()

def main():
    asyncio.tasks._DEBUG = True
    logging.basicConfig(
        level=logging.DEBUG,
        format=('%(asctime)s [%(process)d] [%(levelname)s] '
                '%(filename)s:%(lineno)d %(message)s'))
    run()
    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    main()
