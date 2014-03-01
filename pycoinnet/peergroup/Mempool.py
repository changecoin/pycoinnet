import asyncio
import logging

from pycoinnet.InvItem import InvItem, ITEM_TYPE_BLOCK, ITEM_TYPE_TX


class Mempool:
    def __init__(self, inv_collector, is_interested_f=lambda inv_item: True):
        self.inv_collector = inv_collector
        self.q = inv_collector.new_inv_item_queue()
        self.tx_pool = {}
        self.block_pool = {}
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
                inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in self.tx_pool.values()]
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
                txs_found = []
                blocks_found = []
                for inv_item in inv_items:
                    pool, the_list = (self.tx_pool, txs_found) if inv_item.item_type == ITEM_TYPE_TX else (self.block_pool, blocks_found)
                    if inv_item.data in pool:
                        the_list.append(pool[inv_item.data])
                    else:
                        not_found.append(inv_item)
                if not_found:
                    peer.send_msg("notfound", items=not_found)
                for tx in txs_found:
                    peer.send_msg("tx", tx=tx)
                for block in blocks_found:
                    peer.send_msg("block", block=block)

        asyncio.Task(_run_mempool(peer.new_get_next_message_f(lambda name, data: name == 'mempool')))
        asyncio.Task(_run_getdata(peer.new_get_next_message_f(lambda name, data: name == 'getdata')))

    def add_tx(self, tx):
        """
        Add a transaction to the mempool and advertise it to peers so it can
        propogate throughout the network.
        """
        the_hash = tx.hash()
        if the_hash not in self.tx_pool:
            self.tx_pool[the_hash] = tx
            self.inv_collector.advertise_item(InvItem(ITEM_TYPE_TX, the_hash))

    def add_block(self, block):
        the_hash = block.hash()
        if the_hash not in self.block_pool:
            self.block_pool[the_hash] = block
            self.inv_collector.advertise_item(InvItem(ITEM_TYPE_BLOCK, the_hash))

    @asyncio.coroutine
    def _run(self):
        @asyncio.coroutine
        def fetch_item(inv_item):
            item = yield from self.inv_collector.fetch(inv_item)
            if item:
                if inv_item.item_type == ITEM_TYPE_TX:
                    self.add_tx(item)
                else:
                    self.add_block(item)

        while True:
            inv_item = yield from self.q.get()
            if self.is_interested_f(inv_item):
                asyncio.Task(fetch_item(inv_item))
