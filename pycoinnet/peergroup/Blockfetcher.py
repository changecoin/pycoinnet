
# this provides the following API:

# get_blocks(hash_list, expected_index)

# it goes to the list of known peers who have the blocks in question

#manages block peers
# fetch block

import asyncio
import logging
import time

from pycoinnet.InvItem import InvItem

ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)


class Blockfetcher:
    def __init__(self):
        self.block_hash_priority_queue = asyncio.PriorityQueue()
        self.fetch_future_lookup = {}

    def get_blocks(self, hash_list, expected_index):
        r = []
        for idx, the_hash in enumerate(hash_list):
            future = asyncio.Future()
            item = (idx + expected_index, the_hash, future)
            self.block_hash_priority_queue.put_nowait(item)
            r.append(future)
        return r

    def handle_connection_made(self, peer, transport):
        self.fetch_future_lookup[peer] = asyncio.Task(self.fetch_from_peer(peer, peer.request_inv_item))

    def handle_connection_lost(self, peer, exc):
        self.fetch_future_lookup[peer].cancel()

    # for each peer, a loop:
    #   pull an item out of priority queue
    #   if we should have it, add it to temp list
    #   repeat until N blocks or queue empty
    #   attempt to fetch N blocks
    #     reset after no progress after X seconds
    #   on failure, replace back in queue (noting this peer doesn't have it)
    #   on success
    #     if less than X/3 seconds used, double N

    @asyncio.coroutine
    def fetch_from_peer(self, peer, request_inv_item):
        version_data = yield from asyncio.wait_for(peer.handshake_complete, timeout=None)
        last_block_index = version_data["last_block_index"]
        missing = set()
        per_loop = 1
        loop_timeout = 30
        while True:
            items_to_try = []
            items_to_not_try = []
            count = 0
            while True:
                item = yield from self.block_hash_priority_queue.get()
                if item in missing or item[0] > last_block_index:
                    items_to_not_try.append(item)
                else:
                    items_to_try.append(item)
                if self.block_hash_priority_queue.empty():
                    break
                if len(items_to_try) >= per_loop or item[0] > last_block_index:
                    break
            for item in items_to_not_try:
                self.block_hash_priority_queue.put_nowait(item)
            start_time = time.time()
            def futures_for_item(item):
                future = asyncio.Future()
                def f(item, future):
                    v = yield from request_inv_item(InvItem(ITEM_TYPE_BLOCK, item[1]))
                    future.set_result(v)
                asyncio.Task(f(item, future))
                return future
            futures = [futures_for_item(item) for item in items_to_try]
            done, pending = yield from asyncio.wait(futures, timeout=loop_timeout)
            finish_time = time.time() - start_time
            if len(pending) > 0:
                import pdb; pdb.set_trace()
                per_loop = int(per_loop * 0.5 + 1)
                logging.debug("we did not finish all of them (%f s), decreasing count per loop to %d", finish_time, per_loop)
                done, pending = yield from asyncio.wait(futures, timeout=loop_timeout)
            else:
                if finish_time * 3 < loop_timeout:
                    per_loop = int(0.8 + 0.4 * per_loop)
                    logging.debug("we finished quickly (%f s), increasing count per loop to %d", finish_time, per_loop)
            for future, item in zip(futures, items_to_try):
                if future.done():
                    item[-1].set_result(future.result())
                else:
                    self.block_hash_priority_queue.put(item)
                    missing.add(item)
