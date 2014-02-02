#!/usr/bin/env python

"""
Custom bitcoin client
"""

import argparse
import asyncio
import binascii
import logging

from pycoin.serialize import b2h_rev

from pycoinnet.examples.BlockStore import BlockStore

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.peer.InvItemHandler import InvItemHandler
from pycoinnet.peer.PingPongHandler import PingPongHandler

from pycoinnet.peergroup.ConnectionManager import ConnectionManager
from pycoinnet.peergroup.InvCollector import InvCollector

from pycoinnet.util.Queue import Queue

MAINNET_MAGIC_HEADER = binascii.unhexlify('F9BEB4D9')
TESTNET_MAGIC_HEADER = binascii.unhexlify('0B110907')

ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)

from pycoin.convention import satoshi_to_btc

def show_tx(tx):
    logging.info("Tx ID %s", b2h_rev(tx.hash()))
    for idx, tx_out in enumerate(tx.txs_out):
        ba = tx_out.bitcoin_address()
        if ba:
            logging.info("%d: %s %s BTC", idx, ba, satoshi_to_btc(tx_out.coin_value))
        else:
            logging.info("can't figure out destination of tx_out id %d", idx)

class CatchupHeaders:
    def __init__(self, blockstore):
        self.blockstore = blockstore

    def petrify_if_appropriate(self):
        longest = self.blockstore.longest_nonpetrified_chain()
        if len(longest) > 32:
            self.blockstore.petrify(len(longest)-32)

    def handle_msg_verack(self, peer, **kwargs):
        self.fetch_more_headers(peer)

    def handle_msg_headers(self, peer, headers, **kwargs):
        new_headers = self.blockstore.accept_blocks(header for header, tx_count in headers)
        if len(new_headers) > 0:
            self.fetch_more_headers(peer)
            self.petrify_if_appropriate()

    def fetch_more_headers(self, peer):
        # this really needs to use the last item in the known block chain
        last_header = self.blockstore.last_block_hash()
        block_number = self.blockstore.last_block_index()
        logging.debug("block number = %d", block_number)
        hashes = [last_header]
        peer.send_msg(message_name="getheaders", version=1, hashes=hashes, hash_stop=last_header)


def run():
    ADDRESS_QUEUE = Queue(maxsize=20)

    blockstore = BlockStore("blockstore")

    inv_collector = InvCollector()

    def create_protocol_callback():
        peer = BitcoinPeerProtocol(MAINNET_MAGIC_HEADER)
        peer.register_delegate(cm)
        InvItemHandler(peer)
        PingPongHandler(peer)
        peer.register_delegate(inv_collector)
        peer.run()
        catchup = CatchupHeaders(blockstore)
        import pdb; pdb.set_trace()
        catchup.petrify_if_appropriate()
        peer.register_delegate(catchup)
        return peer

    cm = ConnectionManager(ADDRESS_QUEUE, create_protocol_callback)

    @asyncio.coroutine
    def fetch_addresses():
        yield from ADDRESS_QUEUE.put(("127.0.0.1", 28333))
        yield from asyncio.sleep(1800)
        for h in [
            "bitseed.xf2.org", "dnsseed.bluematt.me",
            "seed.bitcoin.sipa.be", "dnsseed.bitcoin.dashjr.org"
        ]:
            r = yield from asyncio.get_event_loop().getaddrinfo(h, 8333)
            results = set(t[-1][:2] for t in r)
            for t in results:
                yield from ADDRESS_QUEUE.put(t)
                logging.debug("got address %s", t)

    @asyncio.coroutine
    def block_collector():

        @asyncio.coroutine
        def fetch_tx(item):
            tx = yield from inv_collector.download_inv_item(item)
            name = tx.id()
            #f = open("txs/%s" % name, "wb")
            #tx.stream(f)
            #f.close()
            show_tx(tx)

        @asyncio.coroutine
        def fetch_block(item):
            block = yield from inv_collector.download_inv_item(item)
            name = block.id()
            f = open("blocks/%s" % name, "wb")
            block.stream(f)
            f.close()
            logging.info("WE GOT A BLOCK!! %s", block)

        while True:
            item = yield from inv_collector.next_new_inv_item()
            if item.item_type == ITEM_TYPE_TX:
                asyncio.Task(fetch_tx(item))
            if item.item_type == ITEM_TYPE_BLOCK:
                asyncio.Task(fetch_block(item))

    cm.run()
    asyncio.Task(fetch_addresses())
    asyncio.Task(block_collector())


def main():
    asyncio.tasks._DEBUG = True
    logging.basicConfig(
        level=logging.DEBUG,
        format=('%(asctime)s [%(process)d] [%(levelname)s] '
                '%(filename)s:%(lineno)d %(message)s'))
    run()
    asyncio.get_event_loop().run_forever()


main()
