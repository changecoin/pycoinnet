
import asyncio
import logging
import weakref

from pycoinnet.InvItem import InvItem
from pycoinnet.util.Queue import Queue

ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)

logging = logging.getLogger("InvItemHandler")


class InvItemHandler:
    def __init__(self, peer):
        self.inv_items_requested = Queue()
        self.inv_item_futures = weakref.WeakValueDictionary()

        peer.register_delegate(self)
        self.peer = peer
        peer.request_inv_item = self.request_inv_item
        asyncio.Task(self.run())

    def handle_msg_tx(self, peer, tx, **kwargs):
        self.fulfill(ITEM_TYPE_TX, tx)

    def handle_msg_block(self, peer, block, **kwargs):
        self.fulfill(ITEM_TYPE_BLOCK, block)

    def handle_msg_notfound(self, peer, items, **kwargs):
        logging.info("notfound from %s for items %s", peer, items)
        for item in items:
            future = self.inv_item_futures.get(item)
            if future:
                future.cancel()

    def fulfill(self, inv_item_type, result):
        inv_item = InvItem(inv_item_type, result.hash())
        future = self.inv_item_futures.get(inv_item)
        if future:
            if not future.done():
                future.set_result(result)
            else:
                logging.info("got %s unsolicited", result.id())

    @asyncio.coroutine
    def request_inv_item(self, inv_item):
        future = asyncio.Future()
        yield from self.inv_items_requested.put((inv_item, future))
        done, pending = yield from asyncio.wait([future])
        return done.pop().result()

    @asyncio.coroutine
    def run(self):
        while True:
            pairs = yield from self.inv_items_requested.get_all()
            so_far = []
            for inv_item, future in pairs:
                if future.cancelled():
                    continue
                so_far.append(inv_item)
                self.inv_item_futures[inv_item] = future
                if len(so_far) >= 50000:
                    self.peer.send_msg("getdata", items=so_far)
                    so_far = []
            if len(so_far) > 0:
                self.peer.send_msg("getdata", items=so_far)
