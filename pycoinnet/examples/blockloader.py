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

from pycoinnet.peergroup.fast_forwarder import fast_forwarder_add_peer_f
from pycoinnet.peergroup.Blockfetcher import Blockfetcher

from pycoinnet.helpers.standards import default_msg_version_parameters
from pycoinnet.helpers.standards import initial_handshake
from pycoinnet.helpers.standards import install_ping_manager
from pycoinnet.helpers.standards import install_pong_manager
from pycoinnet.helpers.standards import manage_connection_count
from pycoinnet.helpers.dnsbootstrap import new_queue_of_timestamp_peeraddress_tuples

from pycoinnet.util.Queue import Queue

MAINNET_MAGIC_HEADER = binascii.unhexlify('F9BEB4D9')
TESTNET_MAGIC_HEADER = binascii.unhexlify('0B110907')

from pycoinnet.PeerAddress import PeerAddress

@asyncio.coroutine
def run_peer(peer, fast_forward_add_peer, blockfetcher):
    yield from asyncio.wait_for(peer.did_connection_made, timeout=None)
    version_parameters = default_msg_version_parameters(peer)
    version_data = yield from initial_handshake(peer, version_parameters)
    last_block_index = version_data["last_block_index"]
    fast_forward_add_peer(peer, last_block_index)
    blockfetcher.add_peer(peer, last_block_index)

@asyncio.coroutine
def download_blocks(blockfetcher, change_q):
    def download(block_hash, block_index):
        f = blockfetcher.get_block_future(block_hash, block_index)
        block = yield from asyncio.wait_for(f, timeout=None)
        logging.info("got block id %s", block.id())

    while True:
        add_or_remove, block_hash, block_index = yield from change_q.get()
        if add_or_remove == 'add':
            if block_index > 200000:
                asyncio.Task(download(block_hash, block_index))

@asyncio.coroutine
def show_connection_info(connection_info_q):
    while True:
        verb, noun = yield from connection_info_q.get()
        logging.info("connection manager: %s on %s", verb, noun)

def main():
    asyncio.tasks._DEBUG = True
    logging.basicConfig(
        level=logging.DEBUG,
        format=('%(asctime)s [%(process)d] [%(levelname)s] '
                '%(filename)s:%(lineno)d %(message)s'))
    logging.getLogger("asyncio").setLevel(logging.INFO)
    queue_of_timestamp_peeraddress_tuples = new_queue_of_timestamp_peeraddress_tuples()
    queue_of_timestamp_peeraddress_tuples.put_nowait((0, PeerAddress(1, "127.0.0.1", 28333)))

    local_db = LocalDB()
    petrify_db = PetrifyDB(b'\0'*32)
    block_chain = BlockChain(local_db, petrify_db)
    change_q = block_chain.new_change_q()
    blockfetcher = Blockfetcher()
    asyncio.Task(download_blocks(blockfetcher, change_q))
    fast_forward_add_peer = fast_forwarder_add_peer_f(block_chain)

    def create_protocol_callback():
        peer = BitcoinPeerProtocol(MAINNET_MAGIC_HEADER)
        install_ping_manager(peer)
        install_pong_manager(peer)
        asyncio.Task(run_peer(peer, fast_forward_add_peer, blockfetcher))
        return peer

    connection_info_q = manage_connection_count(queue_of_timestamp_peeraddress_tuples, create_protocol_callback, 20)

    asyncio.Task(show_connection_info(connection_info_q))

    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    main()
