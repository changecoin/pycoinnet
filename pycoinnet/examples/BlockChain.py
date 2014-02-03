import binascii
import logging
import os
import re

from pycoin.block import BlockHeader
from pycoin.serialize import b2h_rev
from pycoinnet.util.LocalBlockChain import LocalBlockChain


"""
- add_items
  - returns added_chain, deleted_chain (if any)
- last_item_index
- hash_is_known (can just use item_for_hash)
- index_for_hash
- hash_for_index
- item_for_hash
- petrify_blocks(subchain)
  - adds to petrified_db
  - rebuilds LocalBlockChain object
  - remove obsolete blocks from local db
    - items moved to petrified_db
    - items pointing to now petrified chain
    - items very old
"""

def bh_to_node(bh):
    return bh.hash(), bh.previous_block_hash

class BlockChain(object):
    """
    This is a compound object that accepts blocks (or block headers) via accept_blocks and
    returns a list of new block hashes now in the best chain. This is typically just one block.

    There are two chains: the local chain, which is still open to be altered if a better
    subtree is found, and the petrified chain, which will never change because it's simply
    buried too deep.
    """

    def __init__(self, local_db, petrify_db):
        self.local_db = local_db
        self.petrify_db = petrify_db
        self.local_block_chain = LocalBlockChain()
        self.local_block_chain.load_nodes(
            bh_to_node(self.local_db.item_for_hash(h)) for h in self.local_db.all_hashes())
        self._longest_chain_cache = None

    def longest_local_block_chain(self):
        def node_weight_f(h):
            item = self.local_db.item_for_hash(h)
            if item:
                return item.difficulty
            return 0
        if self._longest_chain_cache is None:
            chains = self.local_block_chain.longest_chains_by_difficulty(node_weight_f)
            chains.sort()
            if len(chains) > 0:
                self._longest_chain_cache = chains[0]
            else:
                self._longest_chain_cache = []
        return self._longest_chain_cache

    def last_item_index(self):
        return len(self.longest_local_block_chain()) + self.petrify_db.count_of_hashes()

    def hash_is_known(self, h):
        return self.petrify_db.hash_is_known(h) or self.local_db.hash_is_known(h)

    def hash_for_index(self, idx):
        h = self.petrify_db.hash_for_index(idx)
        if h:
            return h
        idx -= self.petrify_db.count_of_hashes()
        lc = self.longest_local_block_chain()
        if 0<= idx < len(lc):
            return lc[idx]

    def item_for_hash(self, h):
        v = self.local_db.item_for_hash(h)
        if v:
            return v
        return self.petrify_db.item_for_hash(h)

    def add_items(self, items):
        def items_to_add(items):
            for item in items:
                if self.hash_is_known(item.hash()):
                    continue
                self.local_db.add_items([item])
                yield bh_to_node(item)

        old_longest_chain = self.longest_local_block_chain()
        self.local_block_chain.load_nodes(items_to_add(items))
        self._longest_chain_cache = None
        new_longest_chain = self.longest_local_block_chain()

        if old_longest_chain and new_longest_chain:
            old_path, new_path = self.local_block_chain.find_ancestral_path(old_longest_chain[0], new_longest_chain[0])
        else:
            old_path = old_longest_chain
            new_path = new_longest_chain
        if old_path:
            logging.debug("old_path is %s", b2h_rev(old_path[0]))
        if new_path:
            logging.debug("new_path is %s", b2h_rev(new_path[0]))

        return new_path[1:], old_path[1:]

    def longest_local_block_chain_length(self):
        return len(self.longest_local_block_chain())

    def petrify_blocks(self, subchain_size):
        """
        """
        petrify_list = self.longest_local_block_chain()[-subchain_size:]
        petrify_list.reverse()
        if len(petrify_list) < subchain_size:
            raise PetrifyError("local_block_chain does not have enough records")

        self.petrify_db._log()
        #logging.debug("local_block_chain : %s", self.local_block_chain)

        items = [self.local_db.item_for_hash(h) for h in petrify_list]
        self.petrify_db.add_chain(items)
        self.petrify_db._log()

        self.local_db.remove_items_with_hash(petrify_list)

        new_blockchain = LocalBlockChain()
        new_blockchain.load_nodes(
            bh_to_node(self.local_db.item_for_hash(h)) for h in self.local_db.all_hashes())

        # TODO: deal with orphan blocks!!
        self.blockchain = new_blockchain
