
import binascii
import logging
import struct

from pycoin.block import Block
from pycoin.tx import script
from pycoin.serialize import b2h_rev

#from collections import namedtuple

#BlockChainRecord = namedtuple("BlockChainRecord", "hash parent_hash difficulty".split())

class BlockHashOnly(object):
    __slots__ = "h previous_block_hash difficulty".split()

    def __init__(self, h, previous_block_hash, difficulty):
        self.h = h
        self.previous_block_hash = previous_block_hash
        self.difficulty = difficulty

    def hash(self):
        return self.h
    def __repr__(self):
        return "<BHO: id:%s parent:%s difficulty:%s>" % (self.h, self.previous_block_hash, self.difficulty)

GENESIS_TRIPLE = (-1, 0, None)

class LocalBlockChain(object):
    def __init__(self, genesis_hash, is_pregenesis_f=lambda h: False):
        self.item_lookup = {}
        self.index_difficulty_lookup = dict()

        self.descendents_by_top = {}
        self.trees_from_bottom = {}

        self.genesis_hash = genesis_hash
        self.is_pregenesis_f = is_pregenesis_f

        self._longest_chain_endpoint = None
        self._longest_path = []

    def __repr__(self):
        return "<LocalBlockChain: trees_fb:%s d_b_tops:%s>" % (self.trees_from_bottom, self.descendents_by_top)

    def load_items(self, items):
        # register everything
        new_hashes = set()
        for item in items:
            h = item.hash()
            if self.is_pregenesis_f(h):
                continue
            if h in self.item_lookup:
                continue
            #item = BlockHashOnly(h, item.previous_block_hash, item.difficulty)
            self.item_lookup[h] = item
            new_hashes.add(h)
        if new_hashes:
            self.meld_new_hashes(new_hashes)
            self._longest_chain_endpoint = None

    def find_changed_paths(self, old_chain_endpoint, new_chain_endpoint):

        common_ancestor = self.common_ancestor(old_chain_endpoint, new_chain_endpoint)

        new_hashes = []
        k = new_chain_endpoint
        while k and k != common_ancestor:
            new_hashes.append(k)
            k = self.item_lookup.get(k).previous_block_hash

        removed_hashes = []
        k = old_chain_endpoint
        while k and k != common_ancestor:
            removed_hashes.append(k)
            k = self.item_lookup.get(k).previous_block_hash

        return new_hashes, removed_hashes

    def meld_new_hashes(self, new_hashes):
        # make a list
        while len(new_hashes) > 0:
            h = new_hashes.pop()
            initial_bottom_h = h
            path = [h]
            item = self.item_lookup.get(h)
            while item:
                h = item.previous_block_hash
                new_hashes.discard(h)
                preceding_path = self.trees_from_bottom.get(h)
                if preceding_path:
                    del self.trees_from_bottom[h]
                    path.extend(preceding_path)
                    # we extended an existing path. Fix up descendents_by_top
                    self.descendents_by_top[preceding_path[-1]].remove(preceding_path[0])
                    break
                path.append(h)
                item = self.item_lookup.get(h)
            self.trees_from_bottom[path[0]] = path

            if len(path) <= 1:
                # this is a lone element... don't bother trying to extend
                return

            # now, perform extensions on any trees that start below here

            bottom_h, top_h = path[0], path[-1]

            top_descendents = self.descendents_by_top.setdefault(top_h, set())
            bottom_descendents = self.descendents_by_top.get(bottom_h)
            if bottom_descendents:
                for descendent in bottom_descendents:
                    prior_path = self.trees_from_bottom[descendent]
                    prior_path.extend(path[1:])
                    if path[0] in self.trees_from_bottom:
                        del self.trees_from_bottom[path[0]]
                    else:
                        pass#import pdb; pdb.set_trace()
                del self.descendents_by_top[bottom_h]
                top_descendents.update(bottom_descendents)
            else:
                top_descendents.add(bottom_h)

    def longest_chain_endpoint(self):
        if not self._longest_chain_endpoint:
            self._longest_chain_endpoint = max(self.trees_from_bottom.keys(), key=lambda h: self.total_difficulty_for_hash(h))
        return self._longest_chain_endpoint

    def distance_for_hash(self, h):
        return self.distance_total_difficulty_for_hash(h)[0]

    def total_difficulty_for_hash(self, h):
        return self.distance_total_difficulty_for_hash(h)[-1]

    def distance_total_difficulty_for_hash(self, h):
        distance, total_difficulty, basis_hash = self._distance_triple_for_hash(h)
        if basis_hash == self.genesis_hash:
            return distance, total_difficulty
        return 0, 0

    def _distance_triple_for_hash(self, h):
        v  = self.index_difficulty_lookup.get(h)
        if v:
            distance, total_difficulty, basis_hash = v
            if basis_hash == self.genesis_hash:
                return v
        item = self.item_lookup.get(h)
        if item:
            distance, total_difficulty, basis_hash = self._distance_triple_for_hash(item.previous_block_hash)
            v = distance+1, total_difficulty + item.difficulty, basis_hash
        else:
            v = (0, 0, h)
        self.index_difficulty_lookup[h] = v
        return v

    def _log(self):
        logging.debug("LBC: %s", self)

    def longest_path(self):
        return self.trees_from_bottom[self.longest_chain_endpoint()]

    def common_ancestor(self, block_hash_1, block_hash_2):
        while 1:
            if block_hash_1 == block_hash_2:
                return block_hash_1
            if self.distance_for_hash(block_hash_1) > self.distance_for_hash(block_hash_2):
                block_hash_1, block_hash_2 = block_hash_2, block_hash_1
            bcr2 = self.item_lookup.get(block_hash_2)
            if not bcr2:
                return None
            block_hash_2 = bcr2.previous_block_hash
