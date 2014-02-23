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

from pycoinnet.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoinnet.util.Queue import Queue


class BlockChainBuilder:
    def __init__(self, blockchain, inv_collector):
        self.peer_queue = Queue()
        self.headers_queue = Queue()
        self.block_change_queues = set()
        asyncio.Task(self.run_ff(blockchain))
        #asyncio.Task(self.watch_inv_collector(blockchain, inv_collector))

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

    def update_q(self, q, new_path, old_path, common_index):
        while len(old_path) > 0:
            last = q.pop()
            if old_path[-1] != last[1]:
                q.put_nowait(last)
                break
            old_path.pop()
        while len(old_path) > 0:
            h = old_path.pop()
            t = ("remove", h, common_index + len(old_path))
            q.put_nowait(t)
        idx = common_index
        while len(new_path) > 0:
            h = new_path.pop()
            t = ("add", h, idx)
            idx += 1
            q.put_nowait(t)

    def update_qs(self, new_path, old_path, common_index):
        for q in self.block_change_queues:
            self.update_q(q, list(new_path), list(old_path), common_index)

    def handle_msg_version(self, peer, **kwargs):
        lbi = kwargs.get("last_block_index")
        #services = kwargs.get("services")
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
                    self.update_qs(new_path, old_path, blockchain.block_chain_size() - len(new_path))
                    self.peer_queue.put_nowait((priority, (peer, lbi, rate_dict)))
                # otherwise, this peer is stupid and should be ignored
