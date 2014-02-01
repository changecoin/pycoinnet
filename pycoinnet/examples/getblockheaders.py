#!/usr/bin/env python

"""
Custom bitcoin client
"""

import argparse
import asyncio
import binascii
import logging

from pycoin.block import BlockHeader
from pycoin.serialize import b2h_rev

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
from pycoinnet.util.BlockChain import BlockChain

LOCAL_GENESIS = (bytes(reversed(binascii.unhexlify('000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214'))), 113300798888791, 250000)

class CatchupHeaders:
    def __init__(self, path):
        self.header_lookup = {}
        self.path = path
        self.blockchain = BlockChain([LOCAL_GENESIS])
        pos = 0
        def header_iter():
            try:
                with open(self.path, "rb") as f:
                    while 1:
                        yield BlockHeader.parse(f)
            except Exception:
                pass
        self.add_to_header_db(header_iter())
        self.header_f = open(self.path, "ab")
        self.header_f.truncate(len(self.header_lookup) * 80)

    def handle_msg_verack(self, peer, **kwargs):
        self.fetch_more_headers(peer)

    def handle_msg_headers(self, peer, headers, **kwargs):
        new_headers = self.add_to_header_db(header for header, tx_count in headers)
        if len(new_headers) > 0:
            for header in new_headers:
                header.stream(self.header_f)
            self.header_f.flush()
            self.fetch_more_headers(peer)

    def add_to_header_db(self, headers):
        new_headers = []
        for header in headers:
            the_hash = header.hash()
            if the_hash not in self.header_lookup:
                self.header_lookup[the_hash] = header
                new_headers.append(header)
        logging.debug("now have %d headers", len(self.header_lookup))
        self.blockchain.load_blocks(new_headers)
        return new_headers

    def fetch_more_headers(self, peer):
        # this really needs to use the last item in the known block chain
        last_header = self.blockchain.longest_chain_endpoint()
        difficulty, block_number = self.blockchain.distance(last_header)
        logging.debug("block number = %d", block_number)
        hashes = [last_header]
        peer.send_msg(message_name="getheaders", version=1, hashes=hashes, hash_stop=last_header)

def run():
    ADDRESS_QUEUE = Queue(maxsize=20)

    inv_collector = InvCollector()
    get_headers = CatchupHeaders("headers.bin")

    def create_protocol_callback():
        peer = BitcoinPeerProtocol(MAINNET_MAGIC_HEADER)
        peer.register_delegate(cm)
        InvItemHandler(peer)
        PingPongHandler(peer)
        peer.register_delegate(get_headers)
        peer.register_delegate(inv_collector)
        peer.run()
        return peer

    cm = ConnectionManager(ADDRESS_QUEUE, create_protocol_callback)

    @asyncio.coroutine
    def fetch_addresses():
        yield from ADDRESS_QUEUE.put(("127.0.0.1", 28333))
        yield from asyncio.sleep(180)
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
            return
            tx = yield from inv_collector.download_inv_item(item)
            name = tx.id()
            #f = open("txs/%s" % name, "wb")
            #tx.stream(f)
            #f.close()
            #show_tx(tx)

        @asyncio.coroutine
        def fetch_block(item):
            #block = yield from inv_collector.download_inv_item(item)
            logging.info("WE GOT A block inv!! %s", item)

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
