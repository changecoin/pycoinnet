
"""
Keep track of all connected peers.
"""

import asyncio.queues
import binascii
import logging

from pycoin.serialize import b2h_rev

from pycoinnet.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.InvItem import InvItem
from pycoinnet.util.BitcoinPeer import BitcoinPeer
from pycoinnet.util.InvItemHandler import InvItemHandler
from pycoinnet.util.PingPongHandler import PingPongHandler
from pycoinnet.util.Queue import Queue

MAINNET_MAGIC_HEADER=binascii.unhexlify('F9BEB4D9')
TESTNET_MAGIC_HEADER=binascii.unhexlify('0B110907')

ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)

class ConnectionManager:

    def __init__(self, address_queue, magic_header=MAINNET_MAGIC_HEADER):
        self.address_queue = address_queue
        self.peers_connecting = set()
        self.peers_connected = set()
        self.magic_header = magic_header
        self.connections = set()

    def run(self, min_connection_count=4):
        self.min_connection_count = min_connection_count
        asyncio.Task(self.keep_minimum_connections())

    @asyncio.coroutine
    def keep_minimum_connections(self):
        connections = set()
        while 1:
            logging.debug("checking connection count (currently %d)", len(connections))
            difference = self.min_connection_count - len(connections)
            for i in range(difference*3):
                asyncio.Task(self.connect_to_remote(asyncio.get_event_loop(), connections))
            yield from asyncio.sleep(10)

    @asyncio.coroutine
    def connect_to_remote(self, event_loop, connections):
        host, port = yield from self.address_queue.get()
        logging.info("connecting to %s port %d", host, port)
        try:
            transport, protocol = yield from event_loop.create_connection(
                lambda: BitcoinPeerProtocol(self.magic_header),
                host=host, port=port)
            peer = BitcoinPeer()
            peer.register_delegate(self)
            connections.add(peer)

            iih = InvItemHandler(peer)
            PingPongHandler(peer)
            InvCollector(peer, iih.request_inv_item)

            yield from peer.run(self, protocol)
            connections.discard(peer)
        except Exception:
            logging.exception("failed to connect to %s:%d", host, port)
            #address_db.remove_address(host, port)
            #address_db.save()
            return


class InvCollector:

    def __init__(self, peer, request_inv_item):
        self.peer = peer
        self.request_inv_item = request_inv_item
        self.inv_item_queue = asyncio.queues.Queue()
        self.tx_queue = Queue()
        self.block_queue = Queue()
        peer.register_delegate(self)
        asyncio.Task(self.run())

    def handle_msg_inv(self, peer, items, **kwargs):
        logging.debug("inv from %s : %d items", peer, len(items))
        self.inv_item_queue.put_nowait((peer, items))

    @asyncio.coroutine
    def run(self):
        peers_with_inv_item = {}
        while True:
            peer, items = yield from self.inv_item_queue.get()
            for inv_item in items:
                # don't get Tx items for now
                #if item.type == ITEM_TYPE_TX:
                #    continue
                logging.debug("peer %s has inv item %s", peer, inv_item)
                if inv_item not in peers_with_inv_item:
                    peer_queue = Queue()
                    peers_with_inv_item[inv_item] = peer_queue
                    asyncio.Task(self.download_inv_item(inv_item, peer_queue))
                else:
                    logging.debug("tasks fetching for %s already in progress", inv_item)
                    peer_queue = peers_with_inv_item[inv_item]
                yield from peer_queue.put(peer)

    @asyncio.coroutine
    def download_inv_item(self, inv_item, peer_queue, peer_timeout=15):
        peers_tried = set()
        peers_we_can_try = set()
        while True:
            if peer_queue.qsize() > 0:
                more_peers = yield from peer_queue.get_all()
                peers_we_can_try.update(more_peers)

            peers_we_can_try.difference_update(peers_tried)
            if len(peers_we_can_try) == 0:
                peer = yield from peer_queue.get()
                peers_we_can_try.add(peer)
                continue

            # pick a peer. For now, we just do it randomly
            peer = peers_we_can_try.pop()
            peers_tried.add(peer)

            logging.debug("queuing for fetch %s from %s", inv_item, peer)
            item = yield from self.request_inv_item(inv_item)
            if item:
                break
            # otherwise, just try another
        if inv_item.item_type == ITEM_TYPE_TX:
            yield from self.tx_queue.put(item)
        if inv_item.item_type == ITEM_TYPE_BLOCK:
            yield from self.block_queue.put(item)


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
