
"""
  - add_tx
    - advertise
  - add_block
    - advertise
  - monitor InvCollector
    - fetch
    - policy (filter InvItem objects)

TODO:
  - deal with block additions and un-additions removing and adding Txs
  - eventually kill Tx
  - add disinterested Tx hashes

"""

import asyncio
import logging

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK

class TxMempool:
    def __init__(self, inv_collector):
        self.inv_collector = inv_collector
        self.q = inv_collector.new_inv_item_queue()
        self.pool = {}
        self.disinterested_items = set()
        asyncio.Task(self._run())

    def add_peer(self, peer):
        @asyncio.coroutine
        def _run_mempool(next_message):
            try:
                name, data = yield from next_message()
                inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in self.pool.values()]
                logging.debug("sending inv of %d item(s) in response to mempool", len(inv_items))
                if len(inv_items) > 0:
                    peer.send_msg("inv", items=inv_items)
                # then we exit. We don't need to handle this message more than once.
            except EOFError:
                pass

        @asyncio.coroutine
        def _run_getdata(next_message):
            while True:
                name, data = yield from next_message()
                inv_items = data["items"]
                not_found = []
                tx_found = []
                for inv_item in inv_items:
                    if inv_item in self.pool:
                        if inv_item.item_type == ITEM_TYPE_TX:
                            tx_found.append(self.pool[inv_item])
                        else:
                            not_found.append(inv_item)
                    else:
                        not_found.append(inv_item)
                if not_found:
                    peer.send_msg("notfound", items=not_found)
                for tx in tx_found:
                    peer.send_msg("tx", tx=tx)

        asyncio.Task(_run_mempool(peer.new_get_next_message_f(lambda name, data: name == 'mempool')))
        asyncio.Task(_run_getdata(peer.new_get_next_message_f(lambda name, data: name == 'getdata')))

    def add_tx(self, tx):
        inv_item = InvItem(ITEM_TYPE_TX, tx.hash())
        if inv_item not in self.pool and inv_item not in self.disinterested_items:
            self.pool[inv_item] = tx
            self.inv_collector.advertise_item(inv_item)

    def add_disinterested_item(self, inv_item):
        self.disinterested_items.add(inv_item)

    @asyncio.coroutine
    def _run(self):
        @asyncio.coroutine
        def fetch_item(inv_item):
            item = yield from self.inv_collector.fetch(inv_item)
            if inv_item.item_type == ITEM_TYPE_TX:
                self.add_tx(item)

        while True:
            inv_item = yield from self.q.get()
            if inv_item in self.disinterested_items:
                continue
            asyncio.Task(fetch_item(inv_item))
