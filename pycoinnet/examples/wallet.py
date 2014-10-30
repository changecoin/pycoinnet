#!/usr/bin/env python

import asyncio
import logging
import os.path
import sqlite3

from pycoin.blockchain.BlockChain import BlockChain, _update_q

from pycoinnet.helpers.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.helpers.networks import MAINNET
from pycoinnet.helpers.standards import initial_handshake
from pycoinnet.helpers.standards import install_pingpong_manager
from pycoinnet.helpers.standards import version_data_for_peer

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.peer.Fetcher import Fetcher

from pycoinnet.peergroup.fast_forwarder import fast_forwarder_add_peer_f
from pycoinnet.peergroup.Blockfetcher import Blockfetcher
from pycoinnet.peergroup.BlockHandler import BlockHandler
from pycoinnet.peergroup.InvCollector import InvCollector

from pycoinnet.util.BlockChainStore import BlockChainStore
from pycoinnet.util.BlockChainView import BlockChainView

from pycoinnet.examples.spvclient import SPVClient
from pycoinnet.bloom import BloomFilter, filter_size_required, hash_function_count_required

#def filter_size_required(element_count, false_positive_probability):
#def hash_function_count_required(filter_size, element_count):

from pycoin.wallet.SQLite3Persistence import SQLite3Persistence
from pycoin.wallet.SQLite3Wallet import SQLite3Wallet


def storage_base_path():
    p = os.path.expanduser("~/.wallet/default/")
    if not os.path.exists(p):
        os.makedirs(p)
    return p


class Keychain(object):
    def __init__(self, addresses):
        self.interested_addresses = set(addresses)

    def is_spendable_interesting(self, address):
        return address in self.interested_addresses


def do_fetch():
    network = MAINNET

    addresses = [a[:-1] for a in open(os.path.join(storage_base_path(), "watch_addresses")).readlines()]

    keychain = Keychain(addresses)

    sql_db = sqlite3.Connection(os.path.join(storage_base_path(), "wallet.db"))
    persistence = SQLite3Persistence(sql_db)
    wallet = SQLite3Wallet(keychain, persistence, desired_spendable_count=20)

    initial_blockchain_view = BlockChainView()

    element_count = len(addresses)
    false_positive_probability = 0.00001

    filter_size = filter_size_required(element_count, false_positive_probability)
    hash_function_count = hash_function_count_required(filter_size, element_count)
    bloom_filter = BloomFilter(filter_size, hash_function_count=hash_function_count, tweak=1)

    for a in addresses:
        bloom_filter.add_address(a)

    merkle_block_index_queue = asyncio.Queue()
    spv = SPVClient(
        network, initial_blockchain_view, bloom_filter, merkle_block_index_queue, host_port_q=None)

    @asyncio.coroutine
    def process_updates(merkle_block_index_queue):
        while True:
            merkle_block, index = yield from merkle_block_index_queue.get()
            if len(merkle_block.txs) > 0:
                print("got block %06d: %s... with %d transactions" % (index, merkle_block.id()[:32], len(merkle_block.txs)))
            elif index % 1000 == 0:
                print("at block %06d" % index)

    t = asyncio.Task(process_updates(merkle_block_index_queue))
    asyncio.get_event_loop().run_forever()


def main():
    print(storage_base_path())
    print("wallet. Fetching.")
    do_fetch()


if __name__ == '__main__':
    main()
