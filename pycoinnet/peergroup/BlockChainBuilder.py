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
import time

from asyncio.queues import PriorityQueue

from pycoinnet.InvItem import InvItem
from pycoinnet.util.Queue import Queue

ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)


class BlockChainBuilder:
    def __init__(self, blockchain, inv_collector, headers_only_prior_to=None):
        self.headers_only_prior_to = headers_only_prior_to
        self.peer_queue = Queue()
        self.block_change_callbacks = []
        self.headers_queue = Queue()
        asyncio.Task(self.run_ff(blockchain))
        #asyncio.Task(self.watch_inv_collector(blockchain, inv_collector))

    def add_block_change_callback(self, callback):
        """
        The callback is invoked (in a task) with (new_path, removed_path) where
        the paths are lists of hashes.
        """
        self.block_change_callbacks.append(callback)

    def handle_msg_version(self, peer, **kwargs):
        lbi = kwargs.get("last_block_index")
        services = kwargs.get("services")
        # TODO: check services to see if they have blocks or just headers
        self.peer_queue.put_nowait((0, (peer, lbi, dict(total_seconds=0, records=0))))

    def handle_msg_headers(self, peer, headers, **kwargs):
        headers = [header for header, tx_count in headers]
        self.headers_queue.put_nowait((peer, headers))

    """
    @asyncio.coroutine
    def watch_inv_collector(self, blockchain, inv_collector):
        @asyncio.coroutine
        def fetch_block(item):
            block = yield from inv_collector.download_inv_item(item)
            new_path, old_path = blockchain.add_items([block])
            if new_path:
                self.block_change_queue.put_nowait((new_path, old_path))

        while True:
            item = yield from inv_collector.next_new_block_inv_item()
            asyncio.Task(fetch_block(item))
    """

    @asyncio.coroutine
    def run_ff(self, blockchain):
        # this kind of works, but it's lame because we put
        # peers into a queue, so they'll never be garbage collected
        # even if they vanish. I think.
        while 1:
            priority, (peer, lbi, rate_dict) = yield from self.peer_queue.get()
            if lbi - blockchain.block_chain_size() > 10:
                # let's get some headers from this guy!
                start_time = time.time()
                h = blockchain.last_blockchain_hash()
                peer.send_msg(message_name="getheaders", version=1, hashes=[h], hash_stop=h)
                # TODO: put a timeout here
                peer1, headers = yield from self.headers_queue.get()
                # TODO: what if the stupid client sends us bogus headers?
                # how will we ever figure this out?
                # answer: go through headers and remove fake ones or ones that we've seen
                # check hash, difficulty, difficulty against hash, and that they form
                # a chain. This make it expensive to produce bogus headers
                time_elapsed = time.time() - start_time
                if peer == peer1:
                    rate_dict["total_seconds"] += time_elapsed
                    rate_dict["records"] += len(headers)
                    priority = - rate_dict["records"] / rate_dict["total_seconds"]
                # let's make sure we actually extend the chain
                new_path, old_path = blockchain.add_items(headers)
                ## this hack is necessary because the stupid default client
                # does not send the genesis block!
                while len(new_path) == 0:
                    for h in blockchain.missing_parents():
                        block = yield from peer.request_inv_item(InvItem(ITEM_TYPE_BLOCK, h))
                        if block:
                            new_path, old_path = blockchain.add_items([block])
                            if len(new_path) > 0:
                                break
                        else:
                            break
                if len(new_path) > 0:
                    for callback in self.block_change_callbacks:
                        callback(new_path, old_path)
                    self.peer_queue.put_nowait((priority, (peer, lbi, rate_dict)))
                # otherwise, this peer is stupid and should be ignored
