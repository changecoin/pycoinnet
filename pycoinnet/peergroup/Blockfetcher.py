
# this provides the following API:

# add_peer(peer, last_known_block)
# get_block(block_hash, expected_index)

# get_blocks(hash_list, expected_index)

# it goes to the list of known peers who have the blocks in question

#manages block peers
# fetch block

import asyncio
import logging
import time
import weakref

from pycoinnet.InvItem import InvItem, ITEM_TYPE_BLOCK

from pycoinnet.peer.Fetcher import Fetcher

class Blockfetcher:
    def __init__(self):
        self.block_hash_priority_queue = asyncio.PriorityQueue()
        self.fetch_future_lookup = weakref.WeakKeyDictionary()

    def add_peer(self, peer, last_block_index):
        self.fetch_future_lookup[peer] = asyncio.Task(self.fetch_from_peer(peer, last_block_index))

    def get_block_future(self, block_hash, block_index):
        future = asyncio.Future()
        item = (block_index, block_hash, future)
        self.block_hash_priority_queue.put_nowait((block_index, block_hash, future))
        return future

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
    def fetch_from_peer(self, peer, last_block_index):
        block_fetcher = Fetcher(peer, ITEM_TYPE_BLOCK)
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
            futures = []
            for item in items_to_try:
                if item[-1].done():
                    break
                future = block_fetcher.fetch_future(item[1])
                def make_cb(the_future):
                    def cb(f):
                        if not the_future.done():
                            the_future.set_result(f.result())
                    return cb
                future.add_done_callback(make_cb(item[-1]))
                futures.append(future)
            if len(futures) == 0:
                continue
            done, pending = yield from asyncio.wait(futures, timeout=loop_timeout)
            finish_time = time.time() - start_time
            if len(pending) > 0:
                per_loop = int(per_loop * 0.5 + 1)
                logging.debug("time elapsed %f s but unfinished, decreasing count per loop to %d for %s", finish_time, per_loop, peer)
                done, pending = yield from asyncio.wait(futures, timeout=loop_timeout)
            else:
                if finish_time * 3 < loop_timeout:
                    per_loop = min(1000, int(0.8 + 1.4 * per_loop))
                    logging.debug("time elapsed %f s, increasing count per loop to %d for %s", finish_time, per_loop, peer)
                else:
                    logging.debug("time elapsed %f s, keeping count at %d for %s", finish_time, per_loop, peer)
            for item in items_to_try:
                if not item[-1].done():
                    self.block_hash_priority_queue.put(item)
                    missing.add(item)
