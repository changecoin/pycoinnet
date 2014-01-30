
"""
Keep track of all connected peers.
"""

import asyncio.queues
import binascii
import logging

from pycoin.serialize import b2h_rev

from pycoinnet.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.peer.InvItemHandler import InvItemHandler
from pycoinnet.peer.PingPongHandler import PingPongHandler
from pycoinnet.peergroup.InvCollector import InvCollector
from pycoinnet.util.Queue import Queue

MAINNET_MAGIC_HEADER=binascii.unhexlify('F9BEB4D9')
TESTNET_MAGIC_HEADER=binascii.unhexlify('0B110907')

ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)

class ConnectionManager:

    def __init__(self, address_queue, inv_collector, magic_header=MAINNET_MAGIC_HEADER):
        self.address_queue = address_queue
        self.peers_connecting = set()
        self.peers_connected = set()
        self.magic_header = magic_header
        self.connections = set()
        self.inv_collector = inv_collector
        self.peer_inv_item_handler_lookup = {}

    def handle_connection_made(self, peer, transport):
        logging.debug("connection made %s", transport)
        self.connections.add(peer)

    def handle_connection_lost(self, peer, exc):
        self.connections.remove(peer)

    @asyncio.coroutine
    def request_inv_item(self, peer, inv_item):
        yield from self.peer_inv_item_handler_lookup[peer].request_inv_item(inv_item)

    def run(self, min_connection_count=4):
        self.min_connection_count = min_connection_count
        asyncio.Task(self.keep_minimum_connections())

    @asyncio.coroutine
    def keep_minimum_connections(self):
        while 1:
            logging.debug("checking connection count (currently %d)", len(self.connections))
            difference = self.min_connection_count - len(self.connections)
            for i in range(difference*3):
                asyncio.Task(self.connect_to_remote(asyncio.get_event_loop(), self.connections))
            yield from asyncio.sleep(10)

    @asyncio.coroutine
    def connect_to_remote(self, event_loop, connections):
        host, port = yield from self.address_queue.get()
        logging.info("connecting to %s port %d", host, port)
        def make_callback():
            peer = BitcoinPeerProtocol(self.magic_header)
            peer.register_delegate(self)
            iih = InvItemHandler(peer)
            self.peer_inv_item_handler_lookup[peer] = iih
            PingPongHandler(peer)
            peer.register_delegate(self.inv_collector)
            peer.run()
            return peer
        try:
            transport, protocol = yield from event_loop.create_connection(
                make_callback, host=host, port=port)
        except Exception:
            logging.exception("failed to connect to %s:%d", host, port)
            return

"""
    def handle_msg_addr(self, peer, message):
        self.address_db.add_addresses(
            (timestamp, address.ip_address.exploded, address.port)
            for timestamp, address in message.date_address_tuples)
        self.address_db.save()
"""

from pycoin.convention import satoshi_to_btc

def show_tx(tx):
    logging.info("Tx ID %s", b2h_rev(tx.hash()))
    for idx, tx_out in enumerate(tx.txs_out):
        ba = tx_out.bitcoin_address()
        if ba:
            logging.info("%d: %s %s BTC", idx, ba, satoshi_to_btc(tx_out.coin_value))
        else:
            logging.info("can't figure out destination of tx_out id %d", idx)
