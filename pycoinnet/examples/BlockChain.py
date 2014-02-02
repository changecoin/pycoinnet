import binascii
import logging
import os
import re

from pycoin.block import BlockHeader
from pycoin.serialize import b2h_rev
from pycoinnet.util.LocalBlockChain import LocalBlockChain, block_header_to_block_chain_record


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
        self.local_block_chain = LocalBlockChain(self.petrify_db)
        self.local_block_chain.load_records(
            LocalBlockChain.block_header_to_block_chain_record(
                self.local_db.item_for_hash(h)) for h in self.local_db.all_hashes())

    def last_item_index(self):
        return self.index_for_hash(self.local_block_chain.longest_chain_endpoint())

    def hash_is_known(self, h):
        return self.petrify_db.hash_is_known(h) or self.local_db.hash_is_known(h)

    def index_for_hash(self, h):
        bcr = self.local_block_chain.lookup.get(h)
        if bcr:
            return bcr.index_difficulty[0] + self.petrify_db.count_of_hashes()
        return self.petrify_db.index_for_hash(h)

    def hash_for_index(self, idx):
        h = self.petrify_db.hash_for_index(idx)
        if h:
            return h
        h = self.local_block_chain.hash_by_number(idx)
        if h:
            return h

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
                yield item

        old_longest_chain_endpoint = self.local_block_chain.longest_chain_endpoint()
        self.local_block_chain.load_records(LocalBlockChain.block_header_to_block_chain_record(item) for item in items_to_add(items))
        new_longest_chain_endpoint = self.local_block_chain.longest_chain_endpoint()
        logging.debug("old chain endpoint is %s", b2h_rev(old_longest_chain_endpoint))
        logging.debug("new chain endpoint is %s", b2h_rev(new_longest_chain_endpoint))
        common_ancestor = self.local_block_chain.common_ancestor(old_longest_chain_endpoint, new_longest_chain_endpoint)
        logging.debug("common_ancestor is %s", b2h_rev(common_ancestor))

        new_hashes = []
        k = new_longest_chain_endpoint
        while k and k != common_ancestor:
            new_hashes.append(k)
            k = self.local_block_chain.lookup.get(k).parent_hash

        removed_hashes = []
        k = old_longest_chain_endpoint
        while k and k != common_ancestor:
            removed_hashes.append(k)
            k = self.local_block_chain.lookup.get(k).parent_hash

        return new_hashes, removed_hashes

    def longest_local_block_chain_length(self):
        return len(self.local_block_chain.longest_path())

    def longest_local_block_chain(self):
        return self.local_block_chain.longest_path()

    def petrify_blocks(self, subchain_size):
        """
        """
        petrify_list = self.local_block_chain.longest_path()[:subchain_size]
        if len(petrify_list) < subchain_size:
            raise PetrifyError("local_block_chain does not have enough records")

        self.petrify_db._log()
        self.local_block_chain._log()

        items = [self.local_db.item_for_hash(h) for h in petrify_list]
        self.petrify_db.add_chain(items)
        self.petrify_db._log()

        self.local_db.remove_items_with_hash(petrify_list)

        new_blockchain = LocalBlockChain(self.petrify_db)
        new_blockchain.load_records(
            LocalBlockChain.block_header_to_block_chain_record(
                self.local_db.item_for_hash(h)) for h in self.local_db.all_hashes())

        new_blockchain.load_records(bcr for bcr in self.local_block_chain.lookup.values())
        # deal with orphan blocks!!
        self.blockchain = new_blockchain
