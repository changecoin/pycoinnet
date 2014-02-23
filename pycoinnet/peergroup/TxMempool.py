import asyncio
import logging

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK

class TxMempool:
    def __init__(self, inv_collector, is_interested_f=lambda inv_item: inv_item.item_type == ITEM_TYPE_TX):
        self.inv_collector = inv_collector
        self.q = inv_collector.new_inv_item_queue()
        self.pool = {}
        self.is_interested_f = is_interested_f
        asyncio.Task(self._run())

    def add_peer(self, peer):
        """
        Call this method when a peer comes online and you want to keep its mempool
        in sync with this mempool.
        """
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
        """
        Add a transaction to the mempool and advertise it to peers so it can
        propogate throughout the network.
        """
        inv_item = InvItem(ITEM_TYPE_TX, tx.hash())
        if inv_item not in self.pool:
            self.pool[inv_item] = tx
            self.inv_collector.advertise_item(inv_item)

    @asyncio.coroutine
    def _run(self):
        @asyncio.coroutine
        def fetch_item(inv_item):
            item = yield from self.inv_collector.fetch(inv_item)
            if inv_item.item_type == ITEM_TYPE_TX:
                self.add_tx(item)

        while True:
            inv_item = yield from self.q.get()
            if self.is_interested_f(inv_item):
                asyncio.Task(fetch_item(inv_item))
