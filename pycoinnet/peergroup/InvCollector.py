"""
InvCollector.py

Listen to peers and queue when new InvItem objects are seen.

Allow them to be fetched.

Advertise objects that are fetched (to peers that haven't told us they have it).

- queue of InvItem objects
- fetch
- advertise
"""

import asyncio
import logging
import time
import weakref

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK
from pycoinnet.peer.Fetcher import Fetcher
from pycoinnet.util.Queue import Queue


class InvCollector:

    def __init__(self):
        self.inv_item_db = {}
        # key: InvItem; value: weakref.WeakSet of peers

        self.fetchers_by_peer = {}
        self.advertise_queues = weakref.WeakSet()
        self.inv_item_queues = weakref.WeakSet()

    def add_peer(self, peer):
        self.fetchers_by_peer[peer] = Fetcher(peer, ITEM_TYPE_TX)
        q = Queue()
        self.advertise_queues.add(q)

        @asyncio.coroutine
        def _advertise_to_peer(peer, q):
            while True:
                items = []
                while True:
                    inv_item = yield from q.get()
                    if peer not in self.inv_item_db.get(inv_item.data, []):
                        items.append(inv_item)
                    if q.qsize() == 0:
                        break
                # advertise the presence of the item!
                if len(items) > 0:
                    peer.send_msg("inv", items=items)

        @asyncio.coroutine
        def _watch_peer(peer, next_message, advertise_task):
            try:
                while True:
                    name, data = yield from next_message()
                    for inv_item in data["items"]:
                        logging.debug("noting %s available from %s", inv_item, peer)
                        self._register_inv_item(inv_item, peer)
            except EOFError:
                del self.fetchers_by_peer[peer]
                advertise_task.cancel()

        advertise_task = asyncio.Task(_advertise_to_peer(peer, q))

        next_message = peer.new_get_next_message_f(lambda name, data: name == "inv")
        asyncio.Task(_watch_peer(peer, next_message, advertise_task))

    def new_inv_item_queue(self):
        q = Queue()
        self.inv_item_queues.add(q)
        return q

    @asyncio.coroutine
    def fetch(self, inv_item, peer_timeout=10):
        logging.debug("launched task to fetch %s", inv_item)
        while True:
            the_dict = self.inv_item_db[inv_item.data]
            if len(the_dict) == 0:
                logging.error("couldn't find a place from which to fetch %s", inv_item)
                del self.inv_item_db[inv_item.data]
                return
            peer, when = the_dict.popitem()
            logging.debug("trying to fetch %s from %s, timeout %s", inv_item, peer, peer_timeout)
            fetcher = self.fetchers_by_peer.get(peer)
            if not fetcher:
                logging.error("no fetcher for %s", peer)
                continue
            item = yield from fetcher.fetch(inv_item.data, timeout=peer_timeout)
            if item:
                logging.debug("got %s", item)
                return item

    def advertise_item(self, inv_item):
        for q in self.advertise_queues:
            q.put_nowait(inv_item)

    def _register_inv_item(self, inv_item, peer):
        the_hash = inv_item.data
        if the_hash not in self.inv_item_db:
            # it's new!
            self.inv_item_db[the_hash] = {}
            for q in self.inv_item_queues:
                q.put_nowait(inv_item)
        self.inv_item_db[the_hash][peer] = time.time()
