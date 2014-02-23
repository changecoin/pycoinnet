"""
GoodNeighbor.py

This really has two parts:

1) listen to peers getting new messages and fetching them
2) accept new messages into mempool and advertising them to peers,
  and responding to subsequent getdata requests

1 is the TxCollector.py
    - yields Tx items in a queue where they can be validated and dumped into
      the GoodNeighbor

2 is the GoodNeighbor.py
    - doesn't really need to report anything. Maybe keep stats.

BlockCollector:
  - when a new block comes in, validate it, then feed it to GoodNeighbor
  - when a "remove block" comes in, validate it, then feed it to GoodNeighbor

"""

"""
This accepts peers, listens to their messages, and responds to them.

Upon receipt of "mempool", it sends inventory list to peer.
Upon receipt of "inv", it fetches the unseen Tx objects and puts them in the mempool.

When a new Tx arrives, it advertises the presence of that object to any peers.

TODO:
 - figure out what to do with Block objects
 - have a list of poison Tx objects that can be ignored (and have this list time out)
 - filter mempool according to some policy (like, "is this Tx valid based on our blockchain?")
"""

import asyncio
import logging
import weakref

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK
from pycoinnet.peer.Fetcher import Fetcher
from pycoinnet.util.Queue import Queue


class TxCollector:
    def __init__(self, peer_timeout=10):
        self.inv_item_db = {}
        # key: InvItem; value: weakref.WeakSet of peers

        self.disinterested_inv_item_set = set()

        self.fetchers_by_peer = {}
        self.tx_queue = Queue()
        self.peer_timeout = peer_timeout

    def add_peer(self, peer):
        self.fetchers_by_peer[peer] = Fetcher(peer, ITEM_TYPE_TX)

        next_message = peer.new_get_next_message_f(lambda name, data: name == "inv")
        asyncio.Task(self._watch_peer(peer, next_message))

    def add_disinterested_inv_item_set(self, inv_item):
        self.disinterested_inv_item_set.add(inv_item)

    @asyncio.coroutine
    def _watch_peer(self, peer, next_message):
        while True:
            name, data = yield from next_message()
            if name is None:
                break
            for inv_item in data["items"]:
                self._register_inv_item(inv_item, peer)

        del self.fetchers_by_peer[peer]

    def _register_inv_item(self, inv_item, peer):
        """
        When we get a new inv item, we do the following:
        - check to see if we have the item already. If so, stop.
        - note that this peer has the item (in a set)
        - if we have never seen the item, kick off task to fetch it
            - get set of peers who have the item. This is probably just one most of the time
            - choose one (how??). Arbitrary seems okay since it's usually just one.
            - try to fetch it from chosen peer. Use Tx Fetcher with X second timeout
            - on success:
                - advertise to all peers who have not advertised
                - exit
            - on failure (ie. timeout or notfound):
                - remove that peer from set of peers with item
        """
        if inv_item.item_type != ITEM_TYPE_TX:
            ## TODO: figure out what to do with InvItem objects that represent blocks
            return
        # don't act on disinterested hashes
        if inv_item in self.disinterested_inv_item_set:
            return
        if inv_item not in self.inv_item_db:
            # it's new!
            self.inv_item_db[inv_item] = weakref.WeakSet()
            self.inv_item_db[inv_item].add(peer)
            asyncio.Task(self._fetch_inv_item(inv_item))
        self.inv_item_db[inv_item].add(peer)

    @asyncio.coroutine
    def _fetch_inv_item(self, inv_item):
        while True:
            the_set = self.inv_item_db[inv_item]
            if len(the_set) == 0:
                logging.error("couldn't find a place to fetch %s from", inv_item)
                return
            peer = the_set.pop()
            the_set.add(peer)
            fetcher = self.fetchers_by_peer[peer]
            tx = yield from fetcher.fetch(inv_item.data, timeout=self.peer_timeout)
            if tx:
                break
            the_set.discard(peer)
        # we have the item
        del self.inv_item_db[inv_item]
        self.tx_queue.put_nowait(tx)
