#!/usr/bin/env python

"""
Custom bitcoin client
"""

import argparse
import asyncio
import binascii
import logging

from pycoin.serialize import b2h_rev

from pycoinnet.InvItem import InvItem

from pycoinnet.util.BlockChain import BlockChain
from pycoinnet.util.LocalDB_RAM import LocalDB
from pycoinnet.util.PetrifyDB_RAM import PetrifyDB

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.peer.InvItemHandler import InvItemHandler
from pycoinnet.peer.PingPongHandler import PingPongHandler

from pycoinnet.peergroup.Blockfetcher import Blockfetcher
from pycoinnet.peergroup.BlockChainBuilder import BlockChainBuilder
from pycoinnet.peergroup.ConnectionManager import ConnectionManager
from pycoinnet.peergroup.InvCollector import InvCollector

from pycoinnet.util.Queue import Queue

MAINNET_MAGIC_HEADER = binascii.unhexlify('F9BEB4D9')
TESTNET_MAGIC_HEADER = binascii.unhexlify('0B110907')

TESTNET_DNS_BOOTSTRAP = [
    "bitcoin.petertodd.org", "testnet-seed.bitcoin.petertodd.org",
    "bluematt.me", "testnet-seed.bluematt.me"
]

MAINNET_DNS_BOOTSTRAP = [
    "bitseed.xf2.org", "dnsseed.bluematt.me",
    "seed.bitcoin.sipa.be", "dnsseed.bitcoin.dashjr.org"
]

from pycoin.convention import satoshi_to_btc

def show_tx(tx):
    logging.info("Tx ID %s", b2h_rev(tx.hash()))
    for idx, tx_out in enumerate(tx.txs_out):
        ba = tx_out.bitcoin_address()
        if ba:
            logging.info("%d: %s %s BTC", idx, ba, satoshi_to_btc(tx_out.coin_value))
        else:
            logging.info("can't figure out destination of tx_out id %d", idx)

def run():
    ADDRESS_QUEUE = Queue(maxsize=20)

    local_db = LocalDB()
    #petrify_db = PetrifyDB("blockstore", b'\0'*32)
    petrify_db = PetrifyDB(b'\0'*32)
    block_chain = BlockChain(local_db, petrify_db)
    inv_collector = InvCollector()
    block_chain_builder = BlockChainBuilder(block_chain, inv_collector)
    blockfetcher = Blockfetcher()

    def create_protocol_callback():
        peer = BitcoinPeerProtocol(TESTNET_MAGIC_HEADER)
        peer.register_delegate(cm)
        InvItemHandler(peer)
        PingPongHandler(peer)
        peer.register_delegate(inv_collector)
        peer.register_delegate(block_chain_builder)
        peer.register_delegate(blockfetcher)
        peer.run()
        return peer

    cm = ConnectionManager(ADDRESS_QUEUE, create_protocol_callback)

    @asyncio.coroutine
    def fetch_addresses(dns_bootstrap):
        yield from ADDRESS_QUEUE.put(("127.0.0.1", 38333))
        #yield from asyncio.sleep(1800)
        for h in dns_bootstrap:
            r = yield from asyncio.get_event_loop().getaddrinfo(h, 8333)
            results = set(t[-1][:2] for t in r)
            for t in results:
                yield from ADDRESS_QUEUE.put(t)
                logging.debug("got address %s", t)

    @asyncio.coroutine
    def tx_collector():

        @asyncio.coroutine
        def fetch_tx(item):
            tx = yield from inv_collector.download_inv_item(item)
            name = tx.id()
            #f = open("txs/%s" % name, "wb")
            #tx.stream(f)
            #f.close()
            show_tx(tx)

        while True:
            item = yield from inv_collector.next_new_tx_inv_item()
            asyncio.Task(fetch_tx(item))

    def block_chain_builder_updated(new_hashes, removed_hashes):
        chain_size = block_chain.block_chain_size()
        logging.info("block chain has %d new items; %d total items", len(new_hashes), chain_size)
        if len(removed_hashes) > 0:
            logging.info("block chain lost %d items!", len(removed_hashes))
        ## this should call the Blockfetcher
        futures = blockfetcher.get_blocks(reversed(new_hashes), chain_size-len(new_hashes))

    block_chain_builder.add_block_change_callback(block_chain_builder_updated)

    cm.run()
    asyncio.Task(fetch_addresses(TESTNET_DNS_BOOTSTRAP))
    asyncio.Task(tx_collector())


def main():
    asyncio.tasks._DEBUG = True
    logging.basicConfig(
        level=logging.DEBUG,
        format=('%(asctime)s [%(process)d] [%(levelname)s] '
                '%(filename)s:%(lineno)d %(message)s'))
    run()
    asyncio.get_event_loop().run_forever()


main()
