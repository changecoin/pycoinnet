"""
Provide InvCollector which has a "download_inv_item" method, a tx_queue
and a block_queue.
"""

import asyncio
import logging

from pycoinnet.util.Queue import Queue


ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)


class InvCollector:

    def __init__(self):
        self.connections = {}
        self.inv_item_queue = Queue()
        self.tx_queue = Queue()
        self.block_queue = Queue()
        asyncio.Task(self.run())

    def handle_connection_made(self, peer, transport):
        self.connections.add(peer)

    def handle_connection_lost(self, peer, exc):
        self.connections.remove(peer)

    def handle_msg_inv(self, peer, items, **kwargs):
        logging.debug("inv from %s : %d items", peer, len(items))
        self.inv_item_queue.put_nowait((peer, items))

    @asyncio.coroutine
    def run(self):
        peers_with_inv_item = {}
        while True:
            peer, fetch_f, items = yield from self.inv_item_queue.get()
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
                yield from peer_queue.put((peer, fetch_f))

    @asyncio.coroutine
    def download_inv_item(self, inv_item, peer_queue, peer_timeout=15):
        lookup = {}
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
            item = yield from self.request_inv_item(peer, inv_item)
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
from pycoin.serialize import b2h_rev

def show_tx(tx):
    logging.info("Tx ID %s", b2h_rev(tx.hash()))
    for idx, tx_out in enumerate(tx.txs_out):
        ba = tx_out.bitcoin_address()
        if ba:
            logging.info("%d: %s %s BTC", idx, ba, satoshi_to_btc(tx_out.coin_value))
        else:
            logging.info("can't figure out destination of tx_out id %d", idx)
