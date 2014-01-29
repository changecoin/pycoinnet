
import asyncio
import logging

from pycoinnet.InvItem import InvItem
from pycoinnet.util.Queue import Queue

ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)


class InvItemHandler:
    def __init__(self, peer):
        self.inv_items_requested = Queue()
        self.inv_item_futures = {}

        peer.register_delegate(self)
        self.peer = peer
        asyncio.Task(self.run())

    def handle_msg_tx(self, peer, tx, **kwargs):
        inv_item = InvItem(ITEM_TYPE_TX, tx.hash())
        future = self.inv_item_futures.get(inv_item)
        if future:
            if not future.done():
                future.set_result(tx)
            else:
                logging.info("got %s unsolicited", tx.id())

    def handle_msg_block(self, peer, block, **kwargs):
        inv_item = InvItem(ITEM_TYPE_BLOCK, block.hash())
        future = self.inv_item_futures.get(inv_item)
        if future:
            if not future.done():
                future.set_result(block)
            else:
                logging.info("got %s unsolicited", block.id())

    @asyncio.coroutine
    def request_inv_item(self, inv_item, timeout=15):
        """timeout is naive... we want to be smarter about it somehow"""
        future = asyncio.Future()
        yield from self.inv_items_requested.put((inv_item, future))
        done, pending = yield from asyncio.wait([future], timeout=timeout)
        if len(done) > 0:
            exc = future.exception()
            if exc:
                raise exc
            return future.result()
        for p in pending:
            p.cancel()
        return None

    @asyncio.coroutine
    def run(self):
        def make_del(inv_item):
            def f(future):
                if inv_item not in self.inv_item_futures:
                    logging.error("inv_item already handled!!")
                    import pdb; pdb.set_trace()
                del self.inv_item_futures[inv_item]
            return f
        while True:
            pairs = yield from self.inv_items_requested.get_all()
            while len(pairs) > 0:
                for inv_item, future in pairs[:50000]:
                    self.inv_item_futures[inv_item] = future
                    future.add_done_callback(make_del(inv_item))
                self.peer.protocol.send_msg_getdata([p[0] for p in pairs[:50000]])
                pairs = pairs[50000:]
