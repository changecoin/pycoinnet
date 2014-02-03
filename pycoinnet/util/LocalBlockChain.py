
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

class LocalBlockChain(object):
    def __init__(self, genesis_hash, is_pregenesis_f=lambda h: False):
        self.item_lookup = {}
        self.index_difficulty_lookup = dict(genesis_hash=(-1, 0))

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
        new_items = []
        for item in items:
            h = item.hash()
            if self.is_pregenesis_f(h):
                continue
            if h in self.item_lookup:
                continue
            #item = BlockHashOnly(h, item.previous_block_hash, item.difficulty)
            self.item_lookup[h] = item
            new_items.append(item)
        if new_items:
            for item in new_items:
                self.meld_item(item)
            self._longest_chain_endpoint = None

    def meld_item(self, item):
        # make a list
        h = item.hash()
        initial_bottom_h = h
        path = [h]
        while item:
            h = item.previous_block_hash
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
                del self.trees_from_bottom[path[0]]
            del self.descendents_by_top[bottom_h]
            top_descendents.update(bottom_descendents)
        else:
            top_descendents.add(bottom_h)

    def longest_chain_endpoint(self):
        if not self._longest_chain_endpoint:
            self._longest_chain_endpoint = max(self.trees_from_bottom.keys(), key=lambda h: self.distance_for_hash(h).index_difficulty[-1])
        return self._longest_chain_endpoint

    def distance_for_hash(self, h):
        pass
        self.index_difficulty_lookup

    def _log(self):
        logging.debug("LBC: %s", self)

    def distance(self, h):
        bcr = self.lookup.get(h)
        if bcr:
            return bcr.index_difficulty

    def longest_path(self):
        return self.trees_from_bottom[self.longest_chain_endpoint()]

    def hash_by_number(self, index):
        index -= self.lookup[self._longest_path[0]].index_difficulty[0]
        if 0 <= index < len(self._longest_path):
            return self._longest_path[index]
        return None

    def common_ancestor(self, block_hash_1, block_hash_2):
        while 1:
            if block_hash_1 == block_hash_2:
                return block_hash_1
            if self.distance(block_hash_1) > self.distance(block_hash_2):
                block_hash_1, block_hash_2 = block_hash_2, block_hash_1
            bcr2 = self.lookup.get(block_hash_2)
            if not bcr2:
                return None
            block_hash_2 = bcr2.parent_hash
