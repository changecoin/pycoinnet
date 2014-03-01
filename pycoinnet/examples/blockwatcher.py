#!/usr/bin/env python

"""
Custom bitcoin client
"""

import argparse
import asyncio
import logging
import os

from pycoinnet.InvItem import ITEM_TYPE_BLOCK

from pycoinnet.util.BlockChain import BlockChain
from pycoinnet.util.BlockChainStore import BlockChainStore

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol

from pycoinnet.peergroup.fast_forwarder import fast_forwarder_add_peer_f
from pycoinnet.peergroup.Blockfetcher import Blockfetcher
from pycoinnet.peergroup.InvCollector import InvCollector
from pycoinnet.peergroup.TxMempool import TxMempool

from pycoinnet.helpers.networks import MAINNET
from pycoinnet.helpers.standards import default_msg_version_parameters
from pycoinnet.helpers.standards import initial_handshake
from pycoinnet.helpers.standards import install_ping_manager
from pycoinnet.helpers.standards import install_pong_manager
from pycoinnet.helpers.standards import manage_connection_count
from pycoinnet.helpers.dnsbootstrap import new_queue_of_timestamp_peeraddress_tuples

from pycoinnet.util.Queue import Queue

from pycoinnet.PeerAddress import PeerAddress


@asyncio.coroutine
def show_connection_info(connection_info_q):
    while True:
        verb, noun, peer = yield from connection_info_q.get()
        logging.info("connection manager: %s on %s", verb, noun)


def write_block_to_disk(blockdir, block, block_index):
    p = os.path.join(blockdir, "block-%06d-%s.bin" % (block_index, block.id()))
    tmp_path = p + ".tmp"
    f = open(tmp_path, "wb")
    block.stream(f)
    f.close()
    os.rename(tmp_path, p)


def block_processor(change_q, blockfetcher, inv_collector, config_dir, blockdir, depth, fast_forward):
    # load the last processed block index
    last_processed_block = 0
    # TODO: should load from disk
    # HACK. We should go to disk to cache this
    block_q = Queue()
    last_processed_block = max(last_processed_block, fast_forward)
    while True:
        add_remove, block_hash, block_index = yield from change_q.get()
        if add_remove == "remove":
            the_other = block_q.pop()
            if the_other[1:] != (block_hash, block_index):
                logging.fatal("problem merging! did the block chain fork? %s %s", the_other, block_hash)
                import sys
                sys.exit(-1)
            continue
        if add_remove != "add":
            logging.error("something weird from change_q")
            continue
        if block_index < fast_forward:
            continue
        item = (blockfetcher.get_block_future(block_hash, block_index), block_hash, block_index)
        block_q.put_nowait(item)
        if change_q.qsize() > 0:
            continue
        while block_q.qsize() >= depth:
            # we have blocks that are buries and ready to write
            future, block_hash, block_index = yield from block_q.get()
            block = yield from asyncio.wait_for(future, timeout=None)
            write_block_to_disk(blockdir, block, block_index)


@asyncio.coroutine
def run_peer(peer, fast_forward_add_peer, blockfetcher, tx_mempool, inv_collector):
    yield from asyncio.wait_for(peer.connection_made_future, timeout=None)
    version_parameters = default_msg_version_parameters(peer)
    version_data = yield from initial_handshake(peer, version_parameters)
    last_block_index = version_data["last_block_index"]
    fast_forward_add_peer(peer, last_block_index)
    blockfetcher.add_peer(peer, last_block_index)
    tx_mempool.add_peer(peer)
    inv_collector.add_peer(peer)

def block_chain_locker(block_chain):
    @asyncio.coroutine
    def _run(block_chain, change_q):
        LOCKED_MULTIPLE = 32
        while True:
            total_length = block_chain.length()
            locked_length = block_chain.locked_length()
            unlocked_length = total_length - locked_length
            if unlocked_length > LOCKED_MULTIPLE:
                new_locked_length = total_length - (total_length % LOCKED_MULTIPLE) - LOCKED_MULTIPLE
                block_chain.lock_to_index(new_locked_length)
            # wait for a change to blockchain
            op, block_header, block_index = yield from change_q()

    asyncio.Task(_run(block_chain, block_chain.new_change_q()))

def main():
    parser = argparse.ArgumentParser(description="Watch Bitcoin network for new blocks.")
    parser.add_argument('-c', "--config-dir", help='The directory where config files are stored.')
    parser.add_argument(
        '-f', "--fast-forward", type=int,
        help="block index to fast-forward to (ie. don't download full blocks prior to this one)", default=0
    )
    parser.add_argument(
        '-d', "--depth", type=int,
        help="Minimum depth blocks must be buried before being dropped in blockdir", default=2
    )
    parser.add_argument("blockdir", help='The directory where new blocks are dropped.')

    asyncio.tasks._DEBUG = True
    logging.basicConfig(
        level=logging.DEBUG,
        format=('%(asctime)s [%(process)d] [%(levelname)s] '
                '%(filename)s:%(lineno)d %(message)s'))
    logging.getLogger("asyncio").setLevel(logging.INFO)
    if 1:
        queue_of_timestamp_peeraddress_tuples = new_queue_of_timestamp_peeraddress_tuples(MAINNET)
    else:
        queue_of_timestamp_peeraddress_tuples = Queue()
        queue_of_timestamp_peeraddress_tuples.put_nowait((0, PeerAddress(1, "127.0.0.1", 28333)))

    args = parser.parse_args()

    block_chain = BlockChain()
    block_chain_store = BlockChainStore(args.config_dir)
    change_q = block_chain.new_change_q()

    block_chain_locker(block_chain)
    block_chain.add_nodes(block_chain_store.block_tuple_iterator())

    blockfetcher = Blockfetcher()

    inv_collector = InvCollector()
    tx_mempool = TxMempool(
        inv_collector,
        is_interested_f=lambda inv_item: inv_item.item_type == ITEM_TYPE_BLOCK
    )

    asyncio.Task(
        block_processor(
            change_q, blockfetcher, inv_collector, args.config_dir,
            args.blockdir, args.depth, args.fast_forward))
    fast_forward_add_peer = fast_forwarder_add_peer_f(block_chain)

    def create_protocol_callback():
        peer = BitcoinPeerProtocol(MAINNET["MAGIC_HEADER"])
        install_ping_manager(peer)
        install_pong_manager(peer)
        asyncio.Task(run_peer(peer, fast_forward_add_peer, blockfetcher, tx_mempool, inv_collector))
        return peer

    connection_info_q = manage_connection_count(
        queue_of_timestamp_peeraddress_tuples,
        create_protocol_callback, 8)
    asyncio.Task(show_connection_info(connection_info_q))
    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    main()
