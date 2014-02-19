"""
- write BlockChainBuilder
  - peer group
  - on top of InvCollector
  - 1. do "catch-up"
  - 2. find missing parents
  - 3. prune chains that point to parents that are "excluded", ie. petrified or poisoned/uninteresting
  - 4. create "block chain changed" events
"""

"""
- FastForwarder
  - takes specified "headers only" date and BlockChain object
  - run and watch queue for peers & last block #
  - whenever a peer connects and its block chain size is larger, kick off run
    - "fast forward to" that number
    - getheaders from base
    - gotheaders headers prior to specified date, use those
      - timeout:
        - make priority queue value worse
    - for others, fetch the full block
    - use a priority queue based on speed (- records/s)
"""

import asyncio
import logging
import time
import weakref

from asyncio.queues import PriorityQueue

from pycoinnet.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoinnet.helpers.standards import get_headers_hashes
from pycoinnet.util.Queue import Queue


class FastForward:
    def __init__(self, blockchain):
        self.peer_queue = Queue()
        self.block_change_queues = weakref.WeakSet()
        asyncio.Task(self._run_ff(blockchain))

    def new_block_change_queue(self):
        """
        Return a Queue that yields tuples of (op, block_hash, block_index) where
        op is one of "add" or "remove" and
        block_hash is a block hash
        block_index is the block index
        """
        q = Queue()
        self.block_change_queues.add(q)
        return q

    def add_peer(self, peer, last_block_index):
        self.peer_queue.put_nowait((0, (peer, last_block_index, dict(total_seconds=0, records=0))))

    def _update_q(self, q, ops):
        # first, we meld out complimentary adds and removes
        while len(ops) > 0:
            op = ops[0]
            if op[0] != 'remove':
                break
            last = q.pop()
            if op[1:] != last[1:]:
                q.put_nowait(last)
                break
            ops = ops[1:]
        for op in ops:
            q.put_nowait(op)

    def _update_qs(self, ops):
        for q in self.block_change_queues:
            self._update_q(q, ops)

    @asyncio.coroutine
    def _fetch_missing(self, peer, blockchain):
        next_message = peer.new_get_next_message_f(lambda msg, data: msg == 'block')
        for h in blockchain.missing_parents():
            peer.send_msg("getdata", items=[InvItem(ITEM_TYPE_BLOCK, h)])
            msg, data = yield from next_message()
            block = data["block"]
            ops = blockchain.add_items([block])
            if len(ops) > 0:
                break
        return ops

    @asyncio.coroutine
    def _run_ff(self, blockchain):
        # this kind of works, but it's lame because we put
        # peers into a queue, so they'll never be garbage collected
        # even if they vanish. I think.
        while 1:
            priority, (peer, lbi, rate_dict) = yield from self.peer_queue.get()
            if lbi - blockchain.block_chain_size() > 0:
                # let's get some headers from this guy!
                start_time = time.time()
                h = blockchain.last_blockchain_hash()
                try:
                    headers = yield from asyncio.wait_for(get_headers_hashes(peer, h), timeout=10)
                except EOFError:
                    # this peer is dead... so don't put it back in the queue
                    continue
                except asyncio.TimeoutError:
                    # this peer timed out. How useless
                    continue
                # TODO: what if the stupid client sends us bogus headers?
                # how will we ever figure this out?
                # answer: go through headers and remove fake ones or ones that we've seen
                # check hash, difficulty, difficulty against hash, and that they form
                # a chain. This make it expensive to produce bogus headers
                time_elapsed = time.time() - start_time
                rate_dict["total_seconds"] += time_elapsed
                rate_dict["records"] += len(headers)
                priority = - rate_dict["records"] / rate_dict["total_seconds"]
                # let's make sure we actually extend the chain
                ops = blockchain.add_items(headers)
                ## this hack is necessary because the stupid default client
                # does not send the genesis block!
                try:
                    while len(ops) == 0:
                        ops = yield from asyncio.wait_for(self._fetch_missing(peer, blockchain), timeout=30)
                except Exception:
                    logging.exception("problem fetching missing parents")
                if len(ops) > 0:
                    self._update_qs(ops)
                    self.peer_queue.put_nowait((priority, (peer, lbi, rate_dict)))
                # otherwise, this peer is stupid and should be ignored
