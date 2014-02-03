"""
Provide InvCollector which has a "download_inv_item" method.
"""

import asyncio
import logging

from pycoinnet.util.Queue import Queue


ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)


class InvCollector:

    def __init__(self):
        self.new_inv_item_tx_queue = Queue()
        self.new_inv_item_block_queue = Queue()

        ## keys: InvItem objects; values: weakref.WeakSet of peers
        self.inv_item_db = {}

    def handle_msg_inv(self, peer, items, **kwargs):
        logging.debug("inv from %s : %d items", peer, len(items))
        for item in items:
            if item not in self.inv_item_db:
                # it's new!
                self.inv_item_db[item] = Queue()
                if item.item_type == ITEM_TYPE_TX:
                    self.new_inv_item_tx_queue.put_nowait(item)
                elif item.item_type == ITEM_TYPE_BLOCK:
                    self.new_inv_item_block_queue.put_nowait(item)
            self.inv_item_db[item].put_nowait(peer)

    @asyncio.coroutine
    def next_new_tx_inv_item(self):
        v = yield from self.new_inv_item_tx_queue.get()
        return v

    @asyncio.coroutine
    def next_new_block_inv_item(self):
        v = yield from self.new_inv_item_block_queue.get()
        return v

    @asyncio.coroutine
    def download_inv_item(self, inv_item, peer_timeout=15):
        futures = []
        q = self.inv_item_db.get(inv_item)
        if not q:
            logging.error("%s is unknown", inv_item)
            return
        peers_tried = set()
        peers_we_can_try = set()
        while True:
            if q.qsize() > 0:
                more_peers = yield from q.get_all()
                peers_we_can_try.update(more_peers)

            peers_we_can_try.difference_update(peers_tried)
            if len(peers_we_can_try) == 0:
                peer = yield from q.get()
                peers_we_can_try.add(peer)
                continue

            # pick a peer. Eventually, by reputation, or some other
            # metric. For now, we just do it randomly
            peer = peers_we_can_try.pop()
            peers_tried.add(peer)

            logging.debug("queuing for fetch %s from %s", inv_item, peer)

            future = yield from wait1(peer.request_inv_item(inv_item), timeout=peer_timeout)
            if future.done():
                return future.result()


def wait1(future, timeout=None):
    done, pending = yield from asyncio.wait([future], timeout=timeout)
    if len(done) > 0:
        future = done.pop()
        exc = future.exception()
        if exc:
            raise exc
    else:
        future = pending.pop()
    return future
